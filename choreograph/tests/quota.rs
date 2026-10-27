//! Resources with a refilling quota (logins with a weekly cap and a short window, an API key's rate limit): a
//! job waits for the refill with a reason that names the window and its reset, the planner books the refill
//! (an ETA and a reservation from the reset) and starts it then, and the holders hear when a window crosses
//! its soft level or its limit. All of it journaled and replayed.

use choreograph::lease::{Authority, LeaseError, LeaseSettings};
use choreograph::live::Live;
use choreograph::model::{Estimate, Job, KindId, Labels, ModelBuilder, ResourceId, Unit};
use choreograph::{Dur, FirstFit, Level, ListScheduler, Planner, Quota, Time, WaitReason, Window};

fn at(d: Dur) -> Time {
    Time::ZERO + d
}

/// Two logins on the shared node, eight sessions at once each.
fn authority() -> (Authority, KindId, ResourceId, ResourceId) {
    let mut b = ModelBuilder::new();
    let k = b.kind("login", Unit::Count);
    let n = b.node("shared", Labels::new());
    let a = b.pool("login-a", k, n, 8);
    let c = b.pool("login-b", k, n, 8);
    let settings = LeaseSettings {
        ttl: Dur::hours(2),
        ..LeaseSettings::default()
    };
    (
        Authority::new(Live::new(b.build().unwrap(), Time::ZERO), settings),
        k,
        a,
        c,
    )
}

fn window(name: &str, used: u64, soft: Option<u64>, resets_at: Option<Time>) -> Window {
    Window {
        name: name.into(),
        used,
        limit: 100,
        soft,
        resets_at,
        every: None,
    }
}

/// A weekly cap and a five-hour window, in percent.
fn quota(week: u64, short: u64, week_reset: Time, short_reset: Time) -> Quota {
    Quota {
        windows: vec![
            window("week", week, Some(80), Some(week_reset)),
            window("5h", short, None, Some(short_reset)),
        ],
    }
}

fn session(k: KindId, name: &str) -> Job {
    Job::new(name)
        .needs(k, 1)
        .kind("session")
        .open()
        .run(Estimate::fixed(Dur::hours(1)))
}

fn pinned(k: KindId, name: &str, r: ResourceId) -> Job {
    let mut j = session(k, name);
    j.needs[0].select.resource = Some(r);
    j
}

#[test]
fn a_job_waits_for_the_refill_and_starts_at_the_reset() {
    let (mut auth, k, a, b) = authority();
    // login-a is past its weekly soft level until day 2; login-b has used its short window until 3h.
    auth.report_quota(
        a,
        Some(quota(84, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)))),
    )
    .unwrap();
    auth.report_quota(
        b,
        Some(quota(40, 100, at(Dur::hours(24 * 5)), at(Dur::hours(3)))),
    )
    .unwrap();
    let j = auth.submit(session(k, "s")).unwrap();
    let mut planner = ListScheduler::default();
    let plan = auth.plan(&mut planner);
    assert!(plan.starts.is_empty());
    assert_eq!(
        plan.waits[&j],
        WaitReason::Quota {
            need: 0,
            resource: b,
            window: "5h".into(),
            used: 100,
            limit: 100,
            reopens: Some(at(Dur::hours(3))),
        },
        "the login that refills first is named"
    );
    assert_eq!(plan.wake_at, Some(at(Dur::hours(3))));
    assert_eq!(plan.reservations.len(), 1);
    assert_eq!(plan.reservations[0].at, at(Dur::hours(3)));
    assert_eq!(plan.reservations[0].assignment.resources, [b]);
    // The ETA comes from the reset.
    let f = planner.forecast(auth.live().model(), auth.live().state());
    assert_eq!(f.etas[&j].p50.unwrap().start, at(Dur::hours(3)));
    assert_eq!(f.etas[&j].p90.unwrap().start, at(Dur::hours(3)));
    // A grant before the reset is refused: the state counts a spent quota as no room.
    assert!(
        auth.grant(&[(j, plan.reservations[0].assignment.clone())])
            .is_err()
    );
    // Just before the reset it still waits; at the reset it starts on login-b.
    auth.set_now(at(Dur::hours(3)) - Dur(1)).unwrap();
    assert!(auth.plan(&mut planner).starts.is_empty());
    auth.set_now(at(Dur::hours(3))).unwrap();
    let plan = auth.plan(&mut planner);
    assert_eq!(plan.starts.len(), 1);
    assert_eq!(plan.starts[0].assignment.resources, [b]);
    auth.grant(&plan.batch()).unwrap();
}

#[test]
fn a_pinned_job_waits_for_its_own_login_and_first_fit_names_it_too() {
    let (mut auth, k, a, _) = authority();
    auth.report_quota(
        a,
        Some(quota(84, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)))),
    )
    .unwrap();
    let free = auth.submit(session(k, "any")).unwrap();
    let pin = auth.submit(pinned(k, "pinned", a)).unwrap();
    let mut planner = ListScheduler::default();
    let plan = auth.plan(&mut planner);
    assert_eq!(plan.starts.len(), 1);
    assert_eq!(plan.starts[0].job, free, "the other login takes it");
    let why = WaitReason::Quota {
        need: 0,
        resource: a,
        window: "week".into(),
        used: 84,
        limit: 100,
        reopens: Some(at(Dur::hours(24 * 2))),
    };
    assert_eq!(plan.waits[&pin], why);
    let f = planner.forecast(auth.live().model(), auth.live().state());
    assert_eq!(f.etas[&pin].p50.unwrap().start, at(Dur::hours(24 * 2)));
    let ff = FirstFit.plan(auth.live().model(), auth.live().state());
    assert_eq!(ff.waits[&pin], why);
    assert_eq!(ff.wake_at, Some(at(Dur::hours(24 * 2))));
}

#[test]
fn an_unknown_reset_waits_for_the_next_report() {
    let (mut auth, k, a, _) = authority();
    let q = Quota {
        windows: vec![window("day", 100, None, None)],
    };
    auth.report_quota(a, Some(q)).unwrap();
    let j = auth.submit(pinned(k, "pinned", a)).unwrap();
    let mut planner = ListScheduler::default();
    let plan = auth.plan(&mut planner);
    assert!(matches!(
        plan.waits[&j],
        WaitReason::Quota { reopens: None, .. }
    ));
    assert_eq!(plan.wake_at, None);
    assert!(plan.reservations.is_empty());
    // A new report with room lets it start.
    let q = Quota {
        windows: vec![window("day", 20, None, None)],
    };
    auth.report_quota(a, Some(q)).unwrap();
    assert_eq!(auth.plan(&mut planner).starts.len(), 1);
}

#[test]
fn holders_hear_the_soft_level_and_the_limit_and_keep_running() {
    let (mut auth, k, a, _) = authority();
    auth.report_quota(
        a,
        Some(quota(50, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)))),
    )
    .unwrap();
    let mut planner = ListScheduler::default();
    let j = auth.submit(pinned(k, "s", a)).unwrap();
    let plan = auth.plan(&mut planner);
    let lease = auth.grant(&plan.batch()).unwrap().remove(0);
    assert!(auth.heartbeat(lease.token).unwrap().quota.is_empty());

    auth.set_now(at(Dur::mins(30))).unwrap();
    auth.report_quota(
        a,
        Some(quota(84, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)))),
    )
    .unwrap();
    let beat = auth.heartbeat(lease.token).unwrap();
    let got: Vec<(ResourceId, &str, Level, Option<Time>)> = beat
        .quota
        .iter()
        .map(|c| (c.resource, c.window.as_str(), c.level, c.resets_at))
        .collect();
    assert_eq!(
        got,
        [(a, "week", Level::Soft, Some(at(Dur::hours(24 * 2))))]
    );
    assert!(!beat.stop, "a quota tells, it does not stop");
    // A second session on the same login now waits; the first keeps its place.
    let w = auth.submit(pinned(k, "w", a)).unwrap();
    let plan = auth.plan(&mut planner);
    assert!(matches!(plan.waits[&w], WaitReason::Quota { .. }));
    assert!(auth.live().state().holders(a).any(|(h, _)| h == j));

    auth.report_quota(
        a,
        Some(quota(100, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)))),
    )
    .unwrap();
    let beat = auth.heartbeat(lease.token).unwrap();
    assert_eq!(beat.quota[0].level, Level::Hard);

    // A report that is not well formed is refused and changes nothing.
    let mut bad = quota(10, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)));
    bad.windows[0].soft = Some(120);
    assert!(matches!(
        auth.report_quota(a, Some(bad)),
        Err(LeaseError::Quota(_))
    ));
    assert_eq!(auth.live().state().quota(a).unwrap().windows[0].used, 100);
}

#[test]
fn the_journal_replays_quota_reports() {
    let (mut auth, k, a, _) = authority();
    let first = auth.snapshot();
    auth.take_journal();
    auth.report_quota(
        a,
        Some(quota(84, 10, at(Dur::hours(24 * 2)), at(Dur::hours(5)))),
    )
    .unwrap();
    let j = auth.submit(pinned(k, "pinned", a)).unwrap();
    let mut entries = vec![first];
    entries.extend(auth.take_journal());
    // Through JSON, as a store keeps it.
    let text = serde_json::to_string(&entries).unwrap();
    let back: Vec<choreograph::Entry> = serde_json::from_str(&text).unwrap();
    let replayed = Authority::replay(back).unwrap();
    assert_eq!(replayed.live().state(), auth.live().state());
    let plan = replayed.plan(&mut ListScheduler::default());
    assert!(matches!(plan.waits[&j], WaitReason::Quota { .. }));
}
