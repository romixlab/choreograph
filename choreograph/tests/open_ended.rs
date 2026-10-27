//! Open-ended jobs: they hold their resources until released, plans treat their end honestly (their p90 while
//! young, predicted again from their age once older), a pause gives up their claim, and a more important job
//! may ask them to give way.

use choreograph::lease::{Authority, LeaseSettings};
use choreograph::live::Live;
use choreograph::model::{Estimate, Job, JobId, KindId, Labels, ModelBuilder, Preempt, Unit};
use choreograph::theory::lognormal_conditional_quantile;
use choreograph::{
    Dur, JobStatus, ListScheduler, Model, Outcome, Planner, Policy, State, Time, WaitReason,
};

fn est(p50: Dur, p90: Dur) -> Estimate {
    Estimate { p50, p90 }
}

/// A bench with a rig of two slots.
fn rig() -> (ModelBuilder, KindId) {
    let mut b = ModelBuilder::new();
    let k = b.kind("rig", Unit::Count);
    let n = b.node("bench", Labels::new());
    b.pool("bench/rig", k, n, 2);
    (b, k)
}

fn at(d: Dur) -> Time {
    Time::ZERO + d
}

/// The session (one rig slot, p50 1h, p90 3h), the head job that needs both slots and the two smaller jobs.
struct Bench {
    m: Model,
    session: JobId,
    head: JobId,
    short: JobId,
    long: JobId,
}

fn bench(open: bool) -> Bench {
    let (mut b, k) = rig();
    let mut s = Job::new("session")
        .needs(k, 1)
        .run(est(Dur::hours(1), Dur::hours(3)))
        .kind("session");
    s.open = open;
    let session = b.job(s);
    let head = b.job(
        Job::new("head")
            .needs(k, 2)
            .class(1)
            .run(Estimate::fixed(Dur::mins(30))),
    );
    let short = b.job(
        Job::new("short")
            .needs(k, 1)
            .run(Estimate::fixed(Dur::hours(2))),
    );
    let long = b.job(
        Job::new("long")
            .needs(k, 1)
            .run(Estimate::fixed(Dur::hours(4))),
    );
    Bench {
        m: b.build().unwrap(),
        session,
        head,
        short,
        long,
    }
}

/// The session runs since 0, the others are queued at `now`.
fn running_since_zero(x: &Bench, now: Time) -> State {
    let mut s = State::new(&x.m, Time::ZERO);
    s.arrive(x.session).unwrap();
    let p = ListScheduler::default().plan(&x.m, &s);
    s.start(&x.m, &p.batch()).unwrap();
    s.advance(&x.m, x.session).unwrap(); // setup (zero) done: it runs
    s.set_now(now).unwrap();
    for j in [x.head, x.short, x.long] {
        s.arrive(j).unwrap();
    }
    s
}

fn conditional_p90_end(age: Dur) -> Time {
    let t = lognormal_conditional_quantile(
        Dur::hours(1).as_ms() as f64,
        Dur::hours(3).as_ms() as f64,
        age.as_ms() as f64,
        0.9,
    )
    .unwrap();
    Time(t.round() as i64)
}

#[test]
fn a_young_open_job_shapes_backfill_with_its_p90() {
    let x = bench(true);
    let s = running_since_zero(&x, at(Dur::mins(10)));
    let plan = ListScheduler::default().plan(&x.m, &s);
    // The head job is promised both slots at the session's p90 (3 h; at 10 min the conditional p90 is a
    // hair later), the 2 h job backfills the free slot; the 4 h one would delay the head and does not.
    let r = &plan.reservations[0];
    assert_eq!(r.job, x.head);
    assert_eq!(r.at, conditional_p90_end(Dur::mins(10)));
    assert!(
        r.at >= at(Dur::hours(3)) && r.at < at(Dur::hours(3) + Dur::mins(5)),
        "{}",
        r.at
    );
    let started: Vec<JobId> = plan.starts.iter().map(|s| s.job).collect();
    assert_eq!(started, vec![x.short]);
    // Without the 2 h job in the queue, the 4 h one is held back by the reservation.
    let mut s2 = State::new(&x.m, Time::ZERO);
    s2.arrive(x.session).unwrap();
    let p = ListScheduler::default().plan(&x.m, &s2);
    s2.start(&x.m, &p.batch()).unwrap();
    s2.advance(&x.m, x.session).unwrap();
    s2.set_now(at(Dur::mins(10))).unwrap();
    s2.arrive(x.head).unwrap();
    s2.arrive(x.long).unwrap();
    let plan = ListScheduler::default().plan(&x.m, &s2);
    assert!(plan.starts.is_empty());
    assert_eq!(plan.waits[&x.long], WaitReason::ReservedFor { job: x.head });
}

#[test]
fn an_old_open_job_is_predicted_again_from_its_age() {
    // At 4 h the session is past its p90. A bounded job there would seem to end now; an open-ended one is
    // predicted again: the p90 given that it has lasted 4 h.
    let now = at(Dur::hours(4));
    let open = bench(true);
    let s = running_since_zero(&open, now);
    let plan = ListScheduler::default().plan(&open.m, &s);
    let r = plan
        .reservations
        .iter()
        .find(|r| r.job == open.head)
        .unwrap();
    let want = conditional_p90_end(Dur::hours(4));
    assert_eq!(r.at, want);
    assert!(
        want > now + Dur::hours(4),
        "Lindy: an old session is predicted to run a lot longer"
    );
    // So the 4 h job fits beside it before the head's reservation, which it would not if the session seemed
    // about to end.
    let started: Vec<JobId> = plan.starts.iter().map(|s| s.job).collect();
    assert_eq!(started, vec![open.short]);

    let bounded = bench(false);
    let s = running_since_zero(&bounded, now);
    let plan = ListScheduler::default().plan(&bounded.m, &s);
    let r = plan
        .reservations
        .iter()
        .find(|r| r.job == bounded.head)
        .unwrap();
    assert_eq!(
        r.at,
        now + Dur(1),
        "a bounded job past its estimate is planned to end at once"
    );

    // The forecast says the same: the session's p90 finish is the conditional one.
    let f = ListScheduler::default().forecast(&open.m, &running_since_zero(&open, now));
    assert_eq!(f.etas[&open.session].p90.unwrap().finish, want);
}

#[test]
fn the_floor_holds_an_open_job_past_every_guess() {
    // With p50 = p90 there is no spread to predict from: the floor keeps it held for that much longer.
    let (mut b, k) = rig();
    let session = b.job(
        Job::new("s")
            .needs(k, 2)
            .run(Estimate::fixed(Dur::hours(1)))
            .open(),
    );
    let head = b.job(Job::new("h").needs(k, 1).class(1));
    let m = b.build().unwrap();
    let mut s = State::new(&m, Time::ZERO);
    s.arrive(session).unwrap();
    let p = ListScheduler::default().plan(&m, &s);
    s.start(&m, &p.batch()).unwrap();
    s.set_now(at(Dur::hours(2))).unwrap();
    s.arrive(head).unwrap();
    let policy = Policy {
        open_floor: Dur::mins(20),
        ..Policy::default()
    };
    let plan = ListScheduler::new(policy).plan(&m, &s);
    assert_eq!(plan.reservations[0].at, at(Dur::hours(2) + Dur::mins(20)));
}

#[test]
fn a_pause_drops_the_claim_and_a_resume_comes_back_to_the_same_place() {
    let x = bench(true);
    let mut s = running_since_zero(&x, at(Dur::mins(10)));
    let ls = ListScheduler::default();
    s.pause(&x.m, x.session).unwrap();
    assert!(s.is_paused(x.session));
    // Nothing held, no future claimed: the head starts on both slots at once.
    let plan = ls.clone().plan(&x.m, &s);
    let started: Vec<JobId> = plan.starts.iter().map(|s| s.job).collect();
    assert_eq!(started, vec![x.head]);
    assert!(plan.reservations.iter().all(|r| r.job != x.session));
    assert!(
        ls.forecast(&x.m, &s).etas[&x.session].p90.is_none(),
        "no ETA while paused"
    );
    s.start(&x.m, &plan.batch()).unwrap();
    s.advance(&x.m, x.head).unwrap(); // its (zero) setup is done: it runs until 40 min

    // The holder comes back: it waits for its slot behind the head, with a reservation at the head's end.
    s.set_now(at(Dur::mins(15))).unwrap();
    s.resume(x.session).unwrap();
    let plan = ls.clone().plan(&x.m, &s);
    assert!(plan.starts.is_empty());
    assert!(
        matches!(
            plan.waits[&x.session],
            WaitReason::Resource { free_at: Some(t), .. } if t == at(Dur::mins(40))
        ),
        "{:?}",
        plan.waits[&x.session]
    );
    assert_eq!(plan.reservations[0].job, x.session);
    assert_eq!(plan.reservations[0].at, at(Dur::mins(40)));
    // The 2 h job queued behind is planned after the session's comeback, not before it.
    let f = ls.forecast(&x.m, &s);
    assert!(f.etas[&x.short].p90.unwrap().start >= at(Dur::mins(40)));

    // The head ends: the session takes its slot back, in the phase it paused in, before the queue.
    s.set_now(at(Dur::mins(40))).unwrap();
    s.finish(&x.m, x.head, Outcome::Ok).unwrap();
    let plan = ls.clone().plan(&x.m, &s);
    assert_eq!(plan.starts[0].job, x.session);
    s.start(&x.m, &plan.batch()).unwrap();
    assert!(!s.is_paused(x.session));
    assert!(matches!(
        s.status(x.session),
        JobStatus::Running { started, .. } if *started == Time::ZERO
    ));
}

#[test]
fn a_pause_keeps_the_kinds_it_names() {
    let mut b = ModelBuilder::new();
    let probe = b.kind("probe", Unit::Count);
    let logger = b.kind("logger", Unit::Count);
    let n = b.node("bench", Labels::new());
    let rp = b.pool("probe", probe, n, 1);
    let rl = b.pool("logger", logger, n, 1);
    let mut j = Job::new("debug").needs(probe, 1).needs(logger, 1).open();
    j.pause_keeps = vec![logger];
    let debug = b.job(j);
    let test = b.job(Job::new("test").needs(probe, 1).class(1));
    let rec = b.job(Job::new("rec").needs(logger, 1).class(1));
    let m = b.build().unwrap();
    let mut s = State::new(&m, Time::ZERO);
    s.arrive(debug).unwrap();
    let p = ListScheduler::default().plan(&m, &s);
    s.start(&m, &p.batch()).unwrap();
    s.arrive(test).unwrap();
    s.arrive(rec).unwrap();
    s.pause(&m, debug).unwrap();
    assert_eq!((s.used(rp), s.used(rl)), (0, 1));
    let plan = ListScheduler::default().plan(&m, &s);
    let started: Vec<JobId> = plan.starts.iter().map(|s| s.job).collect();
    assert_eq!(started, vec![test]);
    // The logger stays held with no end: nothing is promised it.
    assert!(matches!(
        plan.waits[&rec],
        WaitReason::Resource { free_at: None, .. }
    ));
}

/// The session may be asked to give way (class 0, `Preempt::Ask`); `urgent` needs both slots.
fn give_way_model(preempt: Preempt, urgent_class: i32) -> (Model, JobId, JobId) {
    let (mut b, k) = rig();
    let session = b.job(
        Job::new("session")
            .needs(k, 1)
            .run(est(Dur::hours(1), Dur::hours(3)))
            .open()
            .preempt(preempt),
    );
    let urgent = b.job(Job::new("urgent").needs(k, 2).class(urgent_class));
    (b.build().unwrap(), session, urgent)
}

#[test]
fn a_more_important_job_asks_an_askable_one_to_give_way() {
    let ask = |preempt, class, policy: Policy| {
        let (m, session, urgent) = give_way_model(preempt, class);
        let mut s = State::new(&m, Time::ZERO);
        s.arrive(session).unwrap();
        let p = ListScheduler::default().plan(&m, &s);
        s.start(&m, &p.batch()).unwrap();
        s.arrive(urgent).unwrap();
        (
            ListScheduler::new(policy).plan(&m, &s).give_way,
            session,
            urgent,
        )
    };
    let (gw, session, urgent) = ask(Preempt::Ask, 1, Policy::default());
    assert_eq!(gw.into_iter().collect::<Vec<_>>(), vec![(session, urgent)]);
    assert!(
        ask(Preempt::Never, 5, Policy::default()).0.is_empty(),
        "only if its policy says so"
    );
    let gap2 = Policy {
        preempt_gap: Some(2),
        ..Policy::default()
    };
    assert!(
        ask(Preempt::Ask, 1, gap2.clone()).0.is_empty(),
        "one class is not enough at gap 2"
    );
    assert_eq!(ask(Preempt::Ask, 2, gap2).0.len(), 1);
    let off = Policy {
        preempt_gap: None,
        ..Policy::default()
    };
    assert!(ask(Preempt::Ask, 9, off).0.is_empty());
}

#[test]
fn leases_pause_resume_give_way_learn_and_replay() {
    let (m, session, urgent) = give_way_model(Preempt::Ask, 1);
    let settings = LeaseSettings {
        ttl: Dur::mins(1),
        ..LeaseSettings::default()
    };
    // Jobs are submitted at runtime: start from the model's nodes and kinds only.
    let mut b = ModelBuilder::new();
    let k = b.kind("rig", Unit::Count);
    let n = b.node("bench", Labels::new());
    b.pool("bench/rig", k, n, 2);
    let mut auth = Authority::new(Live::new(b.build().unwrap(), Time::ZERO), settings);
    let mut ls = ListScheduler::default();
    let job = |j: JobId| {
        let mut x = m.job(j).clone();
        x.kind = Some(x.name.clone());
        x
    };
    let s_id = auth.submit(job(session)).unwrap();
    let plan = auth.plan(&mut ls);
    let lease = auth.grant(&plan.batch()).unwrap()[0].clone();
    assert_eq!(lease.job, s_id);
    let u_id = auth.submit(job(urgent)).unwrap();

    // The plan asks the session to give way; its heartbeat tells it.
    let plan = auth.plan(&mut ls);
    assert_eq!(plan.give_way.get(&s_id), Some(&u_id));
    assert_eq!(auth.ask_give_way(plan.give_way.keys().copied()), vec![s_id]);
    assert!(auth.ask_give_way([s_id]).is_empty(), "asked once");
    auth.set_now(at(Dur::secs(20))).unwrap();
    assert!(auth.heartbeat(lease.token).unwrap().give_way);

    // It pauses: the urgent job starts; the pause clears the ask.
    auth.pause(lease.token).unwrap();
    assert!(!auth.heartbeat(lease.token).unwrap().give_way);
    let plan = auth.plan(&mut ls);
    let ul = auth.grant(&plan.batch()).unwrap()[0].clone();
    assert_eq!(ul.job, u_id);
    auth.resume(lease.token).unwrap();
    auth.set_now(at(Dur::secs(40))).unwrap();
    auth.heartbeat(lease.token).unwrap();
    auth.release(ul.token, Outcome::Ok).unwrap();

    // It comes back under the same lease and token.
    let plan = auth.plan(&mut ls);
    let back = auth.grant(&plan.batch()).unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].token, lease.token);
    assert_eq!(back[0].expires, at(Dur::secs(40)) + Dur::mins(1));

    // Ended well after 50 s: the kind learns it.
    assert_eq!(auth.learned("session"), None);
    auth.set_now(at(Dur::secs(50))).unwrap();
    auth.release(lease.token, Outcome::Ok).unwrap();
    assert_eq!(
        auth.learned("session"),
        Some(Estimate::fixed(Dur::secs(50)))
    );
    assert_eq!(auth.learned("urgent"), Some(Estimate::fixed(Dur::secs(20))));

    let journal = auth.take_journal();
    assert_eq!(Authority::replay(journal).unwrap(), auth);
}

#[test]
fn the_bench_example_lets_the_test_in_when_the_session_pauses() {
    let text = include_str!("../../examples/hil_open.toml");
    let m = choreograph::jobset::from_toml(text).unwrap();
    let id = |n| m.job_by_name(n).unwrap();
    let ls = ListScheduler::default();
    let mut s = State::new(&m, Time::ZERO);
    s.arrive(id("debug")).unwrap();
    s.arrive(id("soak")).unwrap();
    let p = ls.clone().plan(&m, &s);
    s.start(&m, &p.batch()).unwrap();
    s.set_now(at(Dur::mins(5))).unwrap();
    s.arrive(id("regression")).unwrap();
    s.arrive(id("logger-smoke")).unwrap();
    let plan = ls.clone().plan(&m, &s);
    // The regression waits for the probe at the session's p90 (2 h from its start) and asks it to give way.
    assert!(plan.starts.is_empty());
    let r = plan
        .reservations
        .iter()
        .find(|r| r.job == id("regression"))
        .unwrap();
    assert!(r.at >= at(Dur::hours(2)), "{}", r.at);
    assert_eq!(plan.give_way.get(&id("debug")), Some(&id("regression")));
    // The smoke test needs two logger channels: the soak holds two for hours, so it waits for the debug
    // session's channel at that session's p90, not for a time that has passed.
    let eta = ls.forecast(&m, &s).etas[&id("logger-smoke")];
    assert!(eta.p90.unwrap().start >= at(Dur::hours(2)), "{eta:?}");
    // The person pauses: the regression goes, the logger channel stays with the session.
    s.pause(&m, id("debug")).unwrap();
    let plan = ls.clone().plan(&m, &s);
    let started: Vec<JobId> = plan.starts.iter().map(|s| s.job).collect();
    assert_eq!(started, vec![id("regression")]);
    assert_eq!(s.used(m.resource_by_name("logger").unwrap()), 3);
}
