//! Leases for long sessions: budgets on the lease that the holder hears about when it crosses a level,
//! restarts in place that keep the lease and its resources, and tells between holders with a guard against
//! loops between two of them. All of it journaled and replayed.

use std::collections::BTreeMap;

use choreograph::budget::{Budget, Level, Meter, OnHard, Scope};
use choreograph::lease::{Authority, LeaseError, LeaseSettings};
use choreograph::live::Live;
use choreograph::model::{Estimate, Job, JobId, KindId, Labels, ModelBuilder, Unit};
use choreograph::{Dur, JobStatus, Lease, ListScheduler, Outcome, Time};

fn at(d: Dur) -> Time {
    Time::ZERO + d
}

/// An authority over a node with four session slots, leases of two hours.
fn authority(settings: LeaseSettings) -> (Authority, KindId) {
    let mut b = ModelBuilder::new();
    let k = b.kind("slot", Unit::Count);
    let n = b.node("pc", Labels::new());
    b.pool("pc/slot", k, n, 4);
    let settings = LeaseSettings {
        ttl: Dur::hours(2),
        ..settings
    };
    (
        Authority::new(Live::new(b.build().unwrap(), Time::ZERO), settings),
        k,
    )
}

fn budget(name: &str, meter: Meter, scope: Scope, soft: u64, hard: u64, on: OnHard) -> Budget {
    Budget {
        name: name.into(),
        meter,
        scope,
        soft: Some(soft),
        hard: Some(hard),
        on_hard: on,
    }
}

fn start(auth: &mut Authority, job: Job) -> Lease {
    let j = auth.submit(job).unwrap();
    let plan = auth.plan(&mut ListScheduler::default());
    let l = auth.grant(&plan.batch()).unwrap();
    assert_eq!(l[0].job, j);
    l[0].clone()
}

fn session(k: KindId, name: &str) -> Job {
    Job::new(name)
        .needs(k, 1)
        .kind("session")
        .open()
        .run(Estimate::fixed(Dur::hours(1)))
}

fn levels(over: &[choreograph::Crossing]) -> Vec<(&str, Level)> {
    over.iter().map(|c| (c.budget.as_str(), c.level)).collect()
}

#[test]
fn budgets_are_heard_restarts_keep_the_lease_and_a_hard_stop_cancels() {
    let (mut auth, k) = authority(LeaseSettings::default());
    let job = session(k, "s")
        .budget(budget(
            "tokens",
            Meter::Reported,
            Scope::Lease,
            1000,
            2000,
            OnHard::Tell,
        ))
        .budget(budget(
            "context",
            Meter::Reported,
            Scope::Step,
            150,
            190,
            OnHard::Tell,
        ))
        .budget(budget(
            "wall",
            Meter::Wall,
            Scope::Lease,
            Dur::hours(4).as_ms() as u64,
            Dur::hours(6).as_ms() as u64,
            OnHard::Stop,
        ));
    let l = start(&mut auth, job);

    // Under every level: nothing to hear.
    let use_ = |t: u64, c: u64| BTreeMap::from([("tokens".into(), t), ("context".into(), c)]);
    assert!(auth.report_use(l.token, use_(400, 100)).unwrap().is_empty());
    // The context passes its soft level, then its hard one: told, not stopped.
    auth.set_now(at(Dur::mins(30))).unwrap();
    let over = auth.report_use(l.token, use_(800, 160)).unwrap();
    assert_eq!(levels(&over), [("context", Level::Soft)]);
    let over = auth.report_use(l.token, use_(900, 195)).unwrap();
    assert_eq!(levels(&over), [("context", Level::Hard)]);
    let beat = auth.heartbeat(l.token).unwrap();
    assert_eq!(levels(&beat.over), [("context", Level::Hard)]);
    assert!(!beat.stop);

    // A restart in place: same token and resources, the step's context begins again, tokens add up.
    let held = auth.live().state().status(l.job).clone();
    assert_eq!(auth.restart(l.token).unwrap(), 1);
    let after = auth.lease(l.token).unwrap();
    assert_eq!((after.job, after.usage.restarts), (l.job, 1));
    assert_eq!(auth.live().state().status(l.job), &held, "nothing moved");
    let over = auth.report_use(l.token, use_(300, 20)).unwrap();
    assert_eq!(
        levels(&over),
        [("tokens", Level::Soft)],
        "900 + 300 tokens over the lease, 20 context in the new step"
    );
    assert_eq!(over[0].used, 1200);

    // Six hours of wall time cross the hard level of a stopping budget: the heartbeat says stop.
    for h in 1..=6 {
        auth.set_now(at(Dur::hours(h))).unwrap();
        let beat = auth.heartbeat(l.token).unwrap();
        assert_eq!(beat.stop, h >= 6, "hour {h}");
    }
    let lease = auth.lease(l.token).unwrap().clone();
    auth.release(lease.token, Outcome::Cancelled).unwrap();
    assert!(matches!(
        auth.live().state().status(l.job),
        JobStatus::Done {
            outcome: Outcome::Cancelled,
            ..
        }
    ));
    let journal = auth.take_journal();
    assert_eq!(Authority::replay(journal).unwrap(), auth);
}

#[test]
fn tells_are_delivered_once_and_a_loop_between_two_is_refused() {
    let settings = LeaseSettings {
        pair_bounces: Some(3),
        pair_window: Dur::mins(10),
        ..LeaseSettings::default()
    };
    let (mut auth, k) = authority(settings);
    let a = start(&mut auth, session(k, "a"));
    let b = start(&mut auth, session(k, "b"));
    let c = start(&mut auth, session(k, "c"));

    // From outside a lease: delivered with the next heartbeat, once.
    auth.tell(None, a.job, "rebase onto main").unwrap();
    let beat = auth.heartbeat(a.token).unwrap();
    assert_eq!(beat.wakes.len(), 1);
    assert_eq!(
        (beat.wakes[0].from, beat.wakes[0].text.as_str()),
        (None, "rebase onto main")
    );
    assert!(auth.heartbeat(a.token).unwrap().wakes.is_empty());

    // a and b answer each other: three bounces pass, the fourth is a loop. Telling the same way again is no
    // bounce, and c is not held up by a and b.
    let mut t = Dur::ZERO;
    let mut tell = |auth: &mut Authority, from: &Lease, to: &Lease| {
        t += Dur::secs(30);
        auth.set_now(at(t)).unwrap();
        auth.tell(Some(from.token), to.job, "your turn")
    };
    tell(&mut auth, &a, &b).unwrap();
    tell(&mut auth, &a, &b).unwrap();
    tell(&mut auth, &b, &a).unwrap(); // 1
    tell(&mut auth, &a, &b).unwrap(); // 2
    tell(&mut auth, &b, &a).unwrap(); // 3
    let e = tell(&mut auth, &a, &b).unwrap_err();
    assert!(
        matches!(e, LeaseError::PairLoop { from, to, bounces: 3 } if from == a.job && to == b.job),
        "{e}"
    );
    tell(&mut auth, &c, &a).unwrap();
    tell(&mut auth, &a, &c).unwrap();
    let beat = auth.heartbeat(b.token).unwrap();
    let from: Vec<Option<JobId>> = beat.wakes.iter().map(|w| w.from).collect();
    assert_eq!(from, [Some(a.job); 3]);

    // Once the pair has been quiet for the window, it starts counting again.
    auth.set_now(at(t + Dur::mins(10))).unwrap();
    auth.tell(Some(a.token), b.job, "fresh start").unwrap();

    // A job without a lease cannot be told.
    let queued = auth.submit(Job::new("later").needs(k, 4)).unwrap();
    assert!(auth.tell(None, queued, "hi").is_err());

    let journal = auth.take_journal();
    let replayed = Authority::replay(journal).unwrap();
    assert_eq!(replayed, auth);
    assert_eq!(replayed.lease(b.token).unwrap().mail.len(), 1);
}

#[test]
fn pair_guard_settings_load_and_check() {
    let s = LeaseSettings::from_toml("pair_bounces = \"off\"\npair_window = \"5m\"").unwrap();
    assert_eq!((s.pair_bounces, s.pair_window), (None, Dur::mins(5)));
    let d = LeaseSettings::default();
    assert_eq!((d.pair_bounces, d.pair_window), (Some(6), Dur::mins(10)));
    assert!(LeaseSettings::from_toml("pair_window = \"0s\"").is_err());
    assert!(LeaseSettings::from_toml("pair_bounces = \"many\"").is_err());
}

#[test]
fn bad_budgets_are_refused_at_submit() {
    let (mut auth, k) = authority(LeaseSettings::default());
    let mut b = budget("tokens", Meter::Reported, Scope::Lease, 3, 2, OnHard::Tell);
    let e = auth.submit(session(k, "s").budget(b.clone())).unwrap_err();
    assert!(e.to_string().contains("above"), "{e}");
    b.soft = Some(1);
    auth.submit(session(k, "s").budget(b)).unwrap();
}

#[test]
fn compaction_keeps_pairs_and_senders_on_their_new_ids() {
    let settings = LeaseSettings {
        pair_bounces: Some(2),
        ..LeaseSettings::default()
    };
    let (mut auth, k) = authority(settings);
    let x = start(&mut auth, session(k, "x"));
    auth.release(x.token, Outcome::Ok).unwrap();
    let a = start(&mut auth, session(k, "a"));
    let b = start(&mut auth, session(k, "b"));
    auth.tell(Some(a.token), b.job, "1").unwrap();
    auth.tell(Some(b.token), a.job, "2").unwrap(); // bounce 1

    auth.set_now(at(Dur::secs(10))).unwrap();
    let remap = auth.compact(at(Dur::secs(5)));
    let (na, nb) = (remap.job(a.job).unwrap(), remap.job(b.job).unwrap());
    assert_eq!((na, nb), (JobId(0), JobId(1)), "x is gone, ids moved down");
    let beat = auth.heartbeat(a.token).unwrap();
    assert_eq!(beat.wakes[0].from, Some(nb));
    auth.tell(Some(a.token), nb, "3").unwrap(); // bounce 2
    let e = auth.tell(Some(b.token), na, "4").unwrap_err();
    assert!(
        matches!(e, LeaseError::PairLoop { from, to, .. } if from == nb && to == na),
        "{e}"
    );
    let journal = auth.take_journal();
    assert_eq!(Authority::replay(journal).unwrap(), auth);
}
