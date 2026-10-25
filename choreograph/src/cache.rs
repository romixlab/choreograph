//! The result cache: a job whose inputs are unchanged and whose last result is still valid is not run again.
//!
//! A job names everything that can change its result in [`Job::inputs`] (firmware hash, test version, board
//! serial, fixture revision, instrument calibration id, a merged tree and its toolchain). The [`CacheKey`] is a
//! SHA-256 hash of a canonical encoding of the job's name and those inputs, so the same job with the same
//! inputs always has the same key, on every machine and in every version of this crate that keeps
//! [`KEY_VERSION`].
//!
//! A stored [`Entry`] is reused only while its validity holds: every [`Expiry`] recorded with it (a calibration
//! that runs out) is in the future, and it is not older than [`CachePolicy::max_age`]. Only a [`Verdict::Pass`]
//! or a [`Verdict::Fail`] is a result; a flaky run or an infrastructure error says nothing reliable about the
//! inputs and is never reused.
//!
//! [`ResultCache::lookup`] sorts every job into one of the states a test matrix shows: never run
//! ([`Lookup::Untested`]), a result that no longer holds with the reason, such as the inputs that changed
//! ([`Lookup::Stale`]), or a valid result ([`Lookup::Valid`]). [`ResultCache::settle`] marks queued jobs with a
//! valid result as done without running them.
//!
//! ```
//! use choreograph::cache::{Lookup, ResultCache, Verdict};
//! use choreograph::model::{Job, ModelBuilder};
//! use choreograph::Time;
//!
//! let mut job = Job::new("adc-test");
//! job.inputs.insert("firmware".into(), "f00d".into());
//! let mut cache = ResultCache::default();
//! assert!(matches!(cache.lookup(&job, Time::ZERO), Lookup::Untested));
//!
//! cache.record(&job, Verdict::Pass, Time::ZERO, vec![]);
//! assert!(matches!(cache.lookup(&job, Time::ZERO), Lookup::Valid(_)));
//!
//! job.inputs.insert("firmware".into(), "beef".into());
//! let Lookup::Stale { why, .. } = cache.lookup(&job, Time::ZERO) else { panic!() };
//! assert_eq!(why.to_string(), "firmware changed: f00d -> beef");
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::model::{Job, JobId, Model};
use crate::state::{JobStatus, Outcome, State, StateError};
use crate::time::{Dur, Time};

/// The version of the key encoding. A change to the encoding bumps it, so old keys never match new ones.
pub const KEY_VERSION: u32 = 1;

/// The hash of a job's name and inputs. Printed and serialised as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CacheKey(pub [u8; 32]);

impl CacheKey {
    /// The key of a job: its name and its inputs.
    pub fn of(job: &Job) -> CacheKey {
        CacheKey::from_parts(&job.name, &job.inputs)
    }

    /// The key of a job name and a set of inputs.
    ///
    /// The encoding is canonical: a domain tag with [`KEY_VERSION`], then the name and every input name and value
    /// in name order, each as its length (u64, little endian) and its UTF-8 bytes. Length prefixes keep
    /// `("ab", "c")` and `("a", "bc")` apart; the map's order makes the insertion order irrelevant.
    pub fn from_parts(name: &str, inputs: &BTreeMap<String, String>) -> CacheKey {
        let mut h = Sha256::new();
        h.update(b"choreograph-cache\0");
        h.update(KEY_VERSION.to_le_bytes());
        let mut field = |s: &str| {
            h.update((s.len() as u64).to_le_bytes());
            h.update(s.as_bytes());
        };
        field(name);
        for (k, v) in inputs {
            field(k);
            field(v);
        }
        CacheKey(h.finalize().into())
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CacheKey({self})")
    }
}

/// A string that is not 64 hex digits.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a cache key is 64 hex digits, got {0:?}")]
pub struct BadKey(pub String);

impl FromStr for CacheKey {
    type Err = BadKey;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || BadKey(s.to_string());
        if s.len() != 64 || !s.is_ascii() {
            return Err(bad());
        }
        let mut out = [0u8; 32];
        for (i, b) in out.iter_mut().enumerate() {
            *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| bad())?;
        }
        Ok(CacheKey(out))
    }
}

impl Serialize for CacheKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CacheKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// What a run says about its inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    /// Passed. A result: reused while valid.
    Pass,
    /// Failed, deterministically. A result too: a known-bad combination is not run again unless asked
    /// ([`CachePolicy::rerun_failures`]).
    Fail,
    /// Failed `failed` of `runs` attempts and passed the rest. Never a valid result: it replaces any earlier
    /// result for the same inputs, so the next run is a real one.
    Flaky { failed: u32, runs: u32 },
    /// The run could not say anything (a lost connection, a crashed runner). Never stored: an earlier result
    /// for the same inputs stays as it was.
    Error { reason: String },
}

impl Verdict {
    /// Whether the verdict is a result that may be reused.
    pub fn is_result(&self) -> bool {
        matches!(self, Verdict::Pass | Verdict::Fail)
    }
}

/// A point after which a result no longer holds, and what runs out then.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expiry {
    pub at: Time,
    /// What runs out (`calibration of scope-1`), shown when the result goes stale.
    pub what: String,
}

/// One stored run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub key: CacheKey,
    /// The job's name.
    pub job: String,
    /// The inputs it ran with.
    pub inputs: BTreeMap<String, String>,
    pub verdict: Verdict,
    /// When the run finished.
    pub recorded: Time,
    /// When the result runs out, beyond the policy's max age.
    pub expires: Vec<Expiry>,
}

impl Entry {
    /// The first point at which the result stops holding under `policy`, and why; `None` if it never does.
    pub fn valid_until(&self, policy: &CachePolicy) -> Option<(Time, Expired)> {
        let age = policy
            .max_age
            .map(|a| (self.recorded + a, Expired::MaxAge { max_age: a }));
        let windows = self.expires.iter().map(|e| {
            (
                e.at,
                Expired::Window {
                    what: e.what.clone(),
                },
            )
        });
        // earliest first; on a tie the max age, then the windows in recorded order
        age.into_iter().chain(windows).min_by_key(|(t, _)| *t)
    }
}

/// Why a result with the same inputs no longer holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "expired", rename_all = "snake_case")]
pub enum Expired {
    /// Older than the policy's max age.
    MaxAge { max_age: Dur },
    /// A recorded validity window ran out.
    Window { what: String },
}

/// One input that differs between a stored run and the job now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputChange {
    pub name: String,
    /// The value the stored run had (`None`: it had no such input).
    pub was: Option<String>,
    /// The value now (`None`: the job no longer has it).
    pub now: Option<String>,
}

impl fmt::Display for InputChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.was, &self.now) {
            (Some(a), Some(b)) => write!(f, "{} changed: {a} -> {b}", self.name),
            (None, Some(b)) => write!(f, "{} added: {b}", self.name),
            (Some(a), None) => write!(f, "{} removed (was {a})", self.name),
            (None, None) => write!(f, "{} unchanged", self.name),
        }
    }
}

/// Why a job that ran before must run again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "stale", rename_all = "snake_case")]
pub enum Staleness {
    /// The inputs differ from the newest run of the job; every differing input, in name order.
    Changed { inputs: Vec<InputChange> },
    /// The same inputs, but the result ran out at `at`.
    Expired { at: Time, why: Expired },
    /// The same inputs last ran flaky.
    Flaky { failed: u32, runs: u32 },
}

impl fmt::Display for Staleness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Staleness::Changed { inputs } => {
                for (i, c) in inputs.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{c}")?;
                }
                Ok(())
            }
            Staleness::Expired {
                at,
                why: Expired::MaxAge { max_age },
            } => write!(f, "older than {max_age} since {at}"),
            Staleness::Expired {
                at,
                why: Expired::Window { what },
            } => write!(f, "{what} ran out at {at}"),
            Staleness::Flaky { failed, runs } => write!(f, "flaky: failed {failed} of {runs} runs"),
        }
    }
}

/// Where a job stands against the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup<'a> {
    /// The job declares no inputs, so nothing tells whether a result still holds: it always runs.
    NoInputs,
    /// No run of this job is stored.
    Untested,
    /// A run is stored but does not hold for the job now; `entry` is the run compared against.
    Stale { entry: &'a Entry, why: Staleness },
    /// A valid result for exactly these inputs.
    Valid(&'a Entry),
}

/// Settings of the cache.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CachePolicy {
    /// A result older than this is stale even if nothing else ran out (`None`: no age limit).
    pub max_age: Option<Dur>,
    /// Run jobs whose valid cached result is a failure instead of settling them as failed.
    pub rerun_failures: bool,
}

/// What [`ResultCache::record`] did with a verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recorded {
    /// Stored as a result.
    Stored,
    /// Stored as flaky: any earlier result for these inputs is gone.
    Invalidated,
    /// Not stored (an infrastructure error, or a job without inputs).
    Ignored,
}

/// A job settled from the cache by [`ResultCache::settle`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settled {
    pub job: JobId,
    /// [`Outcome::Cached`] for a pass, [`Outcome::CachedFail`] for a failure.
    pub outcome: Outcome,
    pub key: CacheKey,
}

/// Stored runs by key, and the newest run of each job by name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultCache {
    pub policy: CachePolicy,
    entries: BTreeMap<CacheKey, Entry>,
    /// The key of the newest stored run of each job name.
    newest: BTreeMap<String, CacheKey>,
}

impl ResultCache {
    pub fn new(policy: CachePolicy) -> ResultCache {
        ResultCache {
            policy,
            ..ResultCache::default()
        }
    }

    /// Every stored run, in key order.
    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.values()
    }

    pub fn get(&self, key: &CacheKey) -> Option<&Entry> {
        self.entries.get(key)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Stores the verdict of a run of `job` that finished at `at`, with the validity windows it came with.
    ///
    /// A pass or a failure replaces the stored run for the same inputs. A flaky run replaces it too, so a
    /// result that was trusted before is not reused. An infrastructure error changes nothing. Runs of a job
    /// without inputs are not stored.
    pub fn record(
        &mut self,
        job: &Job,
        verdict: Verdict,
        at: Time,
        expires: Vec<Expiry>,
    ) -> Recorded {
        if job.inputs.is_empty() || matches!(verdict, Verdict::Error { .. }) {
            return Recorded::Ignored;
        }
        let key = CacheKey::of(job);
        let done = if verdict.is_result() {
            Recorded::Stored
        } else {
            Recorded::Invalidated
        };
        self.entries.insert(
            key,
            Entry {
                key,
                job: job.name.clone(),
                inputs: job.inputs.clone(),
                verdict,
                recorded: at,
                expires,
            },
        );
        // a run recorded out of order does not hide a newer one
        let newer = self
            .newest
            .get(&job.name)
            .and_then(|k| self.entries.get(k))
            .is_some_and(|e| e.key != key && e.recorded > at);
        if !newer {
            self.newest.insert(job.name.clone(), key);
        }
        done
    }

    /// Where `job` stands at `now`.
    ///
    /// The run stored for exactly these inputs decides: valid, or stale because it expired or was flaky. With no
    /// run for these inputs, the newest run of the job (by name) names the inputs that changed; with none at
    /// all the job is untested.
    pub fn lookup(&self, job: &Job, now: Time) -> Lookup<'_> {
        if job.inputs.is_empty() {
            return Lookup::NoInputs;
        }
        if let Some(entry) = self.entries.get(&CacheKey::of(job)) {
            return match entry.verdict {
                Verdict::Flaky { failed, runs } => Lookup::Stale {
                    entry,
                    why: Staleness::Flaky { failed, runs },
                },
                // never stored, but a hand-built cache may hold one
                Verdict::Error { .. } => Lookup::Untested,
                Verdict::Pass | Verdict::Fail => match entry.valid_until(&self.policy) {
                    Some((at, why)) if now >= at => Lookup::Stale {
                        entry,
                        why: Staleness::Expired { at, why },
                    },
                    _ => Lookup::Valid(entry),
                },
            };
        }
        match self.newest.get(&job.name).and_then(|k| self.entries.get(k)) {
            Some(entry) => Lookup::Stale {
                entry,
                why: Staleness::Changed {
                    inputs: diff(&entry.inputs, &job.inputs),
                },
            },
            None => Lookup::Untested,
        }
    }

    /// Marks every queued job of `state` whose result is valid now as done without running it:
    /// [`Outcome::Cached`] for a pass, [`Outcome::CachedFail`] for a failure (unless
    /// [`CachePolicy::rerun_failures`]). Returns them in job id order.
    ///
    /// A job is settled only once its dependencies are done and let it go on, like a start, so a cached job
    /// never finishes before what it comes after. A job in a group is settled only together with every other
    /// member: when one member must run, the whole group runs.
    pub fn settle(&self, model: &Model, state: &mut State) -> Result<Vec<Settled>, StateError> {
        let now = state.now();
        let mut ready: BTreeMap<JobId, Settled> = BTreeMap::new();
        for j in state.queued() {
            let job = model.job(j);
            let deps_ok = job
                .after
                .iter()
                .all(|d| state.outcome(*d).is_some_and(Outcome::satisfies_deps));
            if !deps_ok {
                continue;
            }
            let Lookup::Valid(entry) = self.lookup(job, now) else {
                continue;
            };
            let outcome = match entry.verdict {
                Verdict::Pass => Outcome::Cached,
                Verdict::Fail if !self.policy.rerun_failures => Outcome::CachedFail,
                _ => continue,
            };
            ready.insert(
                j,
                Settled {
                    job: j,
                    outcome,
                    key: entry.key,
                },
            );
        }
        let held_back: BTreeSet<JobId> = ready
            .keys()
            .filter(|j| {
                model.job(**j).group.is_some_and(|g| {
                    model.group(g).members.iter().any(|m| {
                        !ready.contains_key(m)
                            && !matches!(state.status(*m), JobStatus::Done { .. })
                    })
                })
            })
            .copied()
            .collect();
        let mut out = Vec::new();
        for (j, s) in ready {
            if held_back.contains(&j) {
                continue;
            }
            match s.outcome {
                Outcome::CachedFail => state.mark_cached_fail(j)?,
                _ => state.mark_cached(j)?,
            }
            out.push(s);
        }
        Ok(out)
    }

    /// Drops every stored run that can no longer be reused at `now` (expired or flaky), keeping the newest run
    /// of each job so a later lookup can still name what changed. Returns how many were dropped.
    pub fn prune(&mut self, now: Time) -> usize {
        let keep: BTreeSet<CacheKey> = self.newest.values().copied().collect();
        let before = self.entries.len();
        let policy = &self.policy;
        self.entries.retain(|k, e| {
            keep.contains(k)
                || (e.verdict.is_result() && e.valid_until(policy).is_none_or(|(at, _)| now < at))
        });
        before - self.entries.len()
    }
}

/// The inputs that differ between `was` and `now`, in name order.
pub fn diff(was: &BTreeMap<String, String>, now: &BTreeMap<String, String>) -> Vec<InputChange> {
    let names: BTreeSet<&String> = was.keys().chain(now.keys()).collect();
    names
        .into_iter()
        .filter(|n| was.get(*n) != now.get(*n))
        .map(|n| InputChange {
            name: n.clone(),
            was: was.get(n).cloned(),
            now: now.get(n).cloned(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Labels, ModelBuilder, Unit};

    fn job(name: &str, inputs: &[(&str, &str)]) -> Job {
        let mut j = Job::new(name);
        for (k, v) in inputs {
            j.inputs.insert(k.to_string(), v.to_string());
        }
        j
    }

    const T0: Time = Time(1_000);

    /// The encoding is the documented one: changing it must bump KEY_VERSION, which changes this value.
    #[test]
    fn key_pinned() {
        let k = CacheKey::of(&job("t", &[("firmware", "f00d"), ("board", "b1")]));
        assert_eq!(
            k.to_string(),
            "186d54712b76dec0260a581a72f7aea672722b4869ac6cd0bfdfbec5224c5df9"
        );
    }

    #[test]
    fn key_ignores_insertion_order_and_other_fields() {
        let a = job("t", &[("a", "1"), ("b", "2")]);
        let mut b = job("t", &[("b", "2"), ("a", "1")]);
        b.class = 7;
        b.weight = 3;
        b.arrival = Time(99);
        assert_eq!(CacheKey::of(&a), CacheKey::of(&b));
    }

    #[test]
    fn key_separates_fields() {
        let keys = [
            CacheKey::of(&job("t", &[("ab", "c")])),
            CacheKey::of(&job("t", &[("a", "bc")])),
            CacheKey::of(&job("ta", &[("b", "c")])),
            CacheKey::of(&job("t", &[("a", "bc"), ("", "")])),
            CacheKey::of(&job("u", &[("a", "bc")])),
            CacheKey::of(&job("t", &[("a", "bd")])),
        ];
        let set: BTreeSet<_> = keys.iter().collect();
        assert_eq!(set.len(), keys.len());
    }

    #[test]
    fn key_text_round_trip() {
        let k = CacheKey::of(&job("t", &[("a", "1")]));
        assert_eq!(k.to_string().parse::<CacheKey>(), Ok(k));
        assert!("abc".parse::<CacheKey>().is_err());
        assert!("zz".repeat(32).parse::<CacheKey>().is_err());
    }

    #[cfg(feature = "toml")]
    #[test]
    fn key_serialises_as_hex() {
        let k = CacheKey::of(&job("t", &[("a", "1")]));
        assert_eq!(key_through_toml(&k), k.to_string());
    }

    #[cfg(feature = "toml")]
    fn key_through_toml(k: &CacheKey) -> String {
        // a TOML document holds a key as a plain string
        #[derive(Serialize, Deserialize)]
        struct W {
            k: CacheKey,
        }
        let text = toml::to_string(&W { k: *k }).unwrap();
        let back: W = toml::from_str(&text).unwrap();
        assert_eq!(back.k, *k);
        text.trim()
            .strip_prefix("k = \"")
            .unwrap()
            .trim_end_matches('"')
            .to_string()
    }

    #[test]
    fn untested_valid_and_no_inputs() {
        let mut c = ResultCache::default();
        let j = job("t", &[("fw", "1")]);
        assert_eq!(c.lookup(&j, T0), Lookup::Untested);
        assert_eq!(c.record(&j, Verdict::Pass, T0, vec![]), Recorded::Stored);
        let Lookup::Valid(e) = c.lookup(&j, T0) else {
            panic!()
        };
        assert_eq!(e.verdict, Verdict::Pass);
        assert_eq!(e.key, CacheKey::of(&j));

        let bare = job("t", &[]);
        assert_eq!(c.lookup(&bare, T0), Lookup::NoInputs);
        assert_eq!(
            c.record(&bare, Verdict::Pass, T0, vec![]),
            Recorded::Ignored
        );
    }

    #[test]
    fn stale_names_changed_added_and_removed_inputs() {
        let mut c = ResultCache::default();
        c.record(
            &job("t", &[("fw", "1"), ("board", "b1"), ("cal", "c1")]),
            Verdict::Pass,
            T0,
            vec![],
        );
        let now = job("t", &[("fw", "2"), ("board", "b1"), ("fixture", "r3")]);
        let Lookup::Stale { entry, why } = c.lookup(&now, T0) else {
            panic!()
        };
        assert_eq!(entry.inputs["fw"], "1");
        let Staleness::Changed { inputs } = &why else {
            panic!("{why:?}")
        };
        let names: Vec<_> = inputs.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["cal", "fixture", "fw"]);
        assert_eq!(
            why.to_string(),
            "cal removed (was c1), fixture added: r3, fw changed: 1 -> 2"
        );
        // another job's runs say nothing about this one
        assert_eq!(c.lookup(&job("u", &[("fw", "1")]), T0), Lookup::Untested);
    }

    #[test]
    fn going_back_to_old_inputs_reuses_their_result() {
        let mut c = ResultCache::default();
        let a = job("t", &[("fw", "1")]);
        let b = job("t", &[("fw", "2")]);
        c.record(&a, Verdict::Pass, T0, vec![]);
        c.record(&b, Verdict::Fail, T0 + Dur(10), vec![]);
        assert!(
            matches!(c.lookup(&a, T0 + Dur(20)), Lookup::Valid(e) if e.verdict == Verdict::Pass)
        );
        assert!(
            matches!(c.lookup(&b, T0 + Dur(20)), Lookup::Valid(e) if e.verdict == Verdict::Fail)
        );
        // a third version is compared with the newest run
        let Lookup::Stale { entry, .. } = c.lookup(&job("t", &[("fw", "3")]), T0) else {
            panic!()
        };
        assert_eq!(entry.inputs["fw"], "2");
    }

    #[test]
    fn a_run_recorded_late_does_not_hide_a_newer_one() {
        let mut c = ResultCache::default();
        c.record(
            &job("t", &[("fw", "2")]),
            Verdict::Pass,
            T0 + Dur(10),
            vec![],
        );
        c.record(&job("t", &[("fw", "1")]), Verdict::Pass, T0, vec![]);
        let Lookup::Stale { entry, .. } = c.lookup(&job("t", &[("fw", "3")]), T0) else {
            panic!()
        };
        assert_eq!(entry.inputs["fw"], "2");
    }

    #[test]
    fn expiry_window_runs_out() {
        let mut c = ResultCache::default();
        let j = job("t", &[("cal", "c1")]);
        let exp = vec![
            Expiry {
                at: T0 + Dur(500),
                what: "calibration of scope-1".into(),
            },
            Expiry {
                at: T0 + Dur(100),
                what: "calibration of psu-1".into(),
            },
        ];
        c.record(&j, Verdict::Pass, T0, exp);
        assert!(matches!(c.lookup(&j, T0 + Dur(99)), Lookup::Valid(_)));
        let Lookup::Stale { why, .. } = c.lookup(&j, T0 + Dur(100)) else {
            panic!()
        };
        assert_eq!(
            why,
            Staleness::Expired {
                at: T0 + Dur(100),
                why: Expired::Window {
                    what: "calibration of psu-1".into()
                }
            }
        );
        assert_eq!(why.to_string(), "calibration of psu-1 ran out at t+1s100ms");
    }

    #[test]
    fn max_age_runs_out() {
        let mut c = ResultCache::new(CachePolicy {
            max_age: Some(Dur(1_000)),
            ..CachePolicy::default()
        });
        let j = job("t", &[("fw", "1")]);
        c.record(
            &j,
            Verdict::Fail,
            T0,
            vec![Expiry {
                at: T0 + Dur(5_000),
                what: "calibration".into(),
            }],
        );
        assert!(matches!(c.lookup(&j, T0 + Dur(999)), Lookup::Valid(_)));
        let Lookup::Stale { why, .. } = c.lookup(&j, T0 + Dur(1_000)) else {
            panic!()
        };
        assert_eq!(
            why,
            Staleness::Expired {
                at: T0 + Dur(1_000),
                why: Expired::MaxAge {
                    max_age: Dur(1_000)
                }
            }
        );
        // a re-run with the same inputs makes it valid again
        c.record(&j, Verdict::Pass, T0 + Dur(1_500), vec![]);
        assert!(matches!(c.lookup(&j, T0 + Dur(2_000)), Lookup::Valid(_)));
    }

    #[test]
    fn flaky_invalidates_and_error_changes_nothing() {
        let mut c = ResultCache::default();
        let j = job("t", &[("fw", "1")]);
        c.record(&j, Verdict::Pass, T0, vec![]);
        let err = Verdict::Error {
            reason: "runner lost".into(),
        };
        assert_eq!(c.record(&j, err, T0 + Dur(1), vec![]), Recorded::Ignored);
        assert!(matches!(c.lookup(&j, T0), Lookup::Valid(e) if e.recorded == T0));

        let flaky = Verdict::Flaky { failed: 1, runs: 3 };
        assert_eq!(
            c.record(&j, flaky, T0 + Dur(2), vec![]),
            Recorded::Invalidated
        );
        let Lookup::Stale { why, .. } = c.lookup(&j, T0 + Dur(3)) else {
            panic!()
        };
        assert_eq!(why, Staleness::Flaky { failed: 1, runs: 3 });
        // an error on a never-run job stores nothing
        let k = job("k", &[("fw", "1")]);
        c.record(&k, Verdict::Error { reason: "x".into() }, T0, vec![]);
        assert_eq!(c.lookup(&k, T0), Lookup::Untested);
    }

    #[test]
    fn prune_drops_unusable_runs_but_keeps_the_newest() {
        let mut c = ResultCache::new(CachePolicy {
            max_age: Some(Dur(100)),
            ..CachePolicy::default()
        });
        c.record(&job("t", &[("fw", "1")]), Verdict::Pass, T0, vec![]);
        c.record(
            &job("t", &[("fw", "2")]),
            Verdict::Pass,
            T0 + Dur(50),
            vec![],
        );
        c.record(&job("u", &[("fw", "1")]), Verdict::Pass, T0, vec![]);
        assert_eq!(c.prune(T0 + Dur(60)), 0);
        // t@1 is old and not the newest of t; u@1 is old but the newest of u
        assert_eq!(c.prune(T0 + Dur(120)), 1);
        assert_eq!(c.len(), 2);
        assert!(matches!(
            c.lookup(&job("t", &[("fw", "3")]), T0),
            Lookup::Stale { .. }
        ));
    }

    #[cfg(feature = "toml")]
    #[test]
    fn serde_round_trip() {
        let mut c = ResultCache::default();
        c.record(
            &job("t", &[("fw", "1")]),
            Verdict::Pass,
            T0,
            vec![Expiry {
                at: T0 + Dur(10),
                what: "cal".into(),
            }],
        );
        c.record(
            &job("u", &[("fw", "1")]),
            Verdict::Flaky { failed: 2, runs: 5 },
            T0,
            vec![],
        );
        let text = toml::to_string(&c).unwrap();
        let back: ResultCache = toml::from_str(&text).unwrap();
        assert_eq!(back, c);
    }

    /// a, b (after a), c and d in a group, e without inputs; one exclusive resource.
    fn model() -> (Model, [JobId; 5]) {
        let mut b = ModelBuilder::new();
        let k = b.kind("probe", Unit::Count);
        let n = b.node("bench", Labels::new());
        b.pool("probe-1", k, n, 1);
        let a = b.job(job("a", &[("fw", "1")]).needs(k, 1));
        let bb = b.job(job("b", &[("fw", "1")]).needs(k, 1).after(a));
        let c = b.job(job("c", &[("fw", "1")]).needs(k, 1));
        let d = b.job(job("d", &[("fw", "1")]).needs(k, 1));
        let e = b.job(job("e", &[]).needs(k, 1));
        b.group("cd", vec![c, d]);
        (b.build().unwrap(), [a, bb, c, d, e])
    }

    fn arrive_all(m: &Model) -> State {
        let mut s = State::new(m, T0);
        for j in m.job_ids() {
            s.arrive(j).unwrap();
        }
        s
    }

    #[test]
    fn settle_marks_valid_passes_and_failures() {
        let (m, [a, b, c, d, e]) = model();
        let mut cache = ResultCache::default();
        cache.record(m.job(a), Verdict::Pass, T0, vec![]);
        cache.record(m.job(b), Verdict::Fail, T0, vec![]);
        cache.record(m.job(e), Verdict::Pass, T0, vec![]);
        let mut s = arrive_all(&m);
        let got = cache.settle(&m, &mut s).unwrap();
        // b waits for a within the same call: settled on the next one
        assert_eq!(
            got,
            [Settled {
                job: a,
                outcome: Outcome::Cached,
                key: CacheKey::of(m.job(a))
            }]
        );
        let got = cache.settle(&m, &mut s).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].job, b);
        assert_eq!(s.outcome(b), Some(Outcome::CachedFail));
        assert!(!Outcome::CachedFail.satisfies_deps());
        for j in [c, d, e] {
            assert_eq!(s.status(j), &JobStatus::Queued);
        }
        assert!(cache.settle(&m, &mut s).unwrap().is_empty());
    }

    #[test]
    fn settle_reruns_failures_when_asked() {
        let (m, [a, ..]) = model();
        let mut cache = ResultCache::new(CachePolicy {
            rerun_failures: true,
            ..CachePolicy::default()
        });
        cache.record(m.job(a), Verdict::Fail, T0, vec![]);
        let mut s = arrive_all(&m);
        assert!(cache.settle(&m, &mut s).unwrap().is_empty());
        assert_eq!(s.status(a), &JobStatus::Queued);
    }

    #[test]
    fn settle_waits_for_a_dep_that_has_not_run() {
        let (m, [_, b, ..]) = model();
        let mut cache = ResultCache::default();
        cache.record(m.job(b), Verdict::Pass, T0, vec![]);
        let mut s = arrive_all(&m);
        assert!(cache.settle(&m, &mut s).unwrap().is_empty());
        assert_eq!(s.status(b), &JobStatus::Queued);
    }

    #[test]
    fn settle_takes_a_group_whole_or_not_at_all() {
        let (m, [_, _, c, d, _]) = model();
        let mut cache = ResultCache::default();
        cache.record(m.job(c), Verdict::Pass, T0, vec![]);
        let mut s = arrive_all(&m);
        assert!(cache.settle(&m, &mut s).unwrap().is_empty());
        cache.record(m.job(d), Verdict::Pass, T0, vec![]);
        let got: Vec<JobId> = cache
            .settle(&m, &mut s)
            .unwrap()
            .into_iter()
            .map(|s| s.job)
            .collect();
        assert_eq!(got, [c, d]);
    }
}
