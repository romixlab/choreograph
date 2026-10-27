//! The running service: a [`Service`] behind a lock, the clock, the ticker that plans when nothing else
//! happens, and the waits that are woken when something changes.
//!
//! Time is tokio's: the wall clock at the start plus tokio's monotonic time since, so a test on tokio's paused
//! clock runs hours of leases and ticks in milliseconds, and a wall clock that jumps does not move leases.

use std::sync::{Arc, Mutex, MutexGuard};

use choreograph::{Dur, Time};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::api::{JobRef, JobView};
use crate::config::{ClientConfig, Config};
use crate::service::{Refusal, Service, StartError};

/// The scheduler's time: a base plus tokio's monotonic time since the start.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    base: Time,
    start: Instant,
}

impl Clock {
    /// A clock that reads `base` now.
    pub fn new(base: Time) -> Clock {
        Clock {
            base,
            start: Instant::now(),
        }
    }

    /// A clock on the wall time now (milliseconds since the Unix epoch).
    pub fn wall() -> Clock {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        Clock::new(Time(ms))
    }

    pub fn now(&self) -> Time {
        self.base + Dur(self.start.elapsed().as_millis() as i64)
    }

    /// The tokio instant of a scheduler time (now if it has passed).
    pub fn instant(&self, t: Time) -> Instant {
        let d = (t - self.base).as_ms().max(0) as u64;
        (self.start + std::time::Duration::from_millis(d)).max(Instant::now())
    }

    /// The same clock moved so that it reads at least `t` (after a restart on a journal from a later time).
    fn at_least(self, t: Time) -> Clock {
        let now = self.now();
        if now >= t {
            return self;
        }
        Clock {
            base: self.base + (t - now),
            start: self.start,
        }
    }
}

struct Inner {
    svc: Mutex<Service>,
    clock: Clock,
    /// Bumped after every change; waits and the ticker watch it.
    changed: watch::Sender<u64>,
}

/// The service as the HTTP layer uses it. Cheap to clone.
#[derive(Clone)]
pub struct Daemon {
    inner: Arc<Inner>,
}

impl Daemon {
    /// Opens the state directory and starts on it.
    pub fn open(config: Config, clock: Clock) -> Result<Daemon, StartError> {
        let svc = Service::open(config, clock.now())?;
        Ok(Daemon::from_service(svc, clock))
    }

    /// A daemon without a journal on disk.
    pub fn in_memory(config: Config, clock: Clock) -> Daemon {
        Daemon::from_service(Service::in_memory(config, clock.now()), clock)
    }

    fn from_service(svc: Service, clock: Clock) -> Daemon {
        let clock = clock.at_least(svc.now());
        Daemon {
            inner: Arc::new(Inner {
                svc: Mutex::new(svc),
                clock,
                changed: watch::channel(0).0,
            }),
        }
    }

    pub fn now(&self) -> Time {
        self.inner.clock.now()
    }

    pub fn config(&self) -> Config {
        self.lock().config().clone()
    }

    /// The service, locked. Calls on it are short: no IO but the journal append.
    pub fn lock(&self) -> MutexGuard<'_, Service> {
        self.inner.svc.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Runs `f` on the service at the current time without changing anything (nobody is woken).
    pub fn look<T>(&self, f: impl FnOnce(&mut Service, Time) -> T) -> T {
        let mut s = self.lock();
        f(&mut s, self.now())
    }

    /// Runs `f` on the service at the current time and wakes everyone who waits for a change.
    pub fn call<T>(
        &self,
        f: impl FnOnce(&mut Service, Time) -> Result<T, Refusal>,
    ) -> Result<T, Refusal> {
        let r = {
            let mut s = self.lock();
            f(&mut s, self.now())
        };
        self.inner.changed.send_modify(|n| *n += 1);
        r
    }

    /// Who a call comes from (see [`Config::caller`]).
    pub fn caller(
        &self,
        bearer: Option<&str>,
        named: Option<&str>,
    ) -> Result<ClientConfig, Refusal> {
        self.lock().caller(bearer, named)
    }

    /// Waits until the job runs or ends (a resumed job: until it has back what it gave back), or `timeout` passes (at most `max_wait`), and answers with the job
    /// as it is then. Every call tells the service the client still wants the job.
    pub async fn wait(
        &self,
        client: &ClientConfig,
        id: JobRef,
        timeout: Option<Dur>,
    ) -> Result<JobView, Refusal> {
        let max = self.lock().config().settings.max_wait;
        let timeout = timeout.unwrap_or(max).clamp(Dur::ZERO, max);
        let deadline = self.inner.clock.instant(self.now() + timeout);
        let mut rx = self.inner.changed.subscribe();
        loop {
            rx.borrow_and_update();
            let v = {
                let mut s = self.lock();
                s.touch(client, id, self.now())?
            };
            if v.wait.is_none() || Instant::now() >= deadline {
                return Ok(v);
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => continue,
                // Timed out, or the service is going away: the job as it is.
                _ => return self.lock().touch(client, id, self.now()),
            }
        }
    }

    /// Plans whenever a lease is due to expire, the plan asked to be woken, or a tick has passed, until the
    /// service breaks (a failed journal write). Returns why it stopped.
    pub async fn run_ticker(&self) -> String {
        let mut rx = self.inner.changed.subscribe();
        loop {
            rx.borrow_and_update();
            let wake = {
                let s = self.lock();
                if let Some(e) = s.broken() {
                    return e.to_string();
                }
                s.next_wake()
            };
            tokio::select! {
                _ = tokio::time::sleep_until(self.inner.clock.instant(wake)) => {
                    if let Err(e) = self.call(|s, now| s.tick(now)) {
                        return e.to_string();
                    }
                }
                r = rx.changed() => {
                    if r.is_err() {
                        return "the service ended".into();
                    }
                }
            }
        }
    }

    /// Resolves once the service is broken (a failed journal write), with the reason.
    pub async fn broken(&self) -> String {
        let mut rx = self.inner.changed.subscribe();
        loop {
            if let Some(e) = self.lock().broken() {
                return e.to_string();
            }
            if rx.changed().await.is_err() {
                return "the service ended".into();
            }
        }
    }
}
