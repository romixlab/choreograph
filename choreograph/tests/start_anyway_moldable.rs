//! Start-anyway and moldable jobs: a job pinned to a node starts past its wait, counted over capacity; a
//! moldable job gets the largest of its shapes that fits now, and plans count the shape it got.

use choreograph::lease::{Authority, LeaseSettings};
use choreograph::live::Live;
use choreograph::model::{Estimate, Job, JobId, KindId, Labels, ModelBuilder, NodeId, Unit};
use choreograph::{
    Assignment, Dur, FirstFit, JobStatus, ListScheduler, Model, ModelError, Planner, State, Time,
    WaitReason,
};

fn at(d: Dur) -> Time {
    Time::ZERO + d
}

fn fixed(d: Dur) -> Estimate {
    Estimate::fixed(d)
}

/// A soak rig with four channels on one bench.
fn rig() -> (ModelBuilder, KindId, NodeId) {
    let mut b = ModelBuilder::new();
    let k = b.kind("channel", Unit::Count);
    let n = b.node("bench", Labels::new());
    b.pool("bench/channels", k, n, 4);
    (b, k, n)
}

/// A soak test that runs on 4 channels or on 2.
fn soak(k: KindId) -> Job {
    let mut j = Job::new("soak").needs(k, 4).run(fixed(Dur::hours(10)));
    j.shapes = vec![Job::new("").needs(k, 2).needs];
    j
}

fn queued(m: &Model, now: Time, jobs: &[JobId]) -> State {
    let mut s = State::new(m, now);
    for j in jobs {
        s.arrive(*j).unwrap();
    }
    s
}

fn started(s: &State, j: JobId) -> Option<(Assignment, bool)> {
    match s.status(j) {
        JobStatus::Running {
            assignment, over, ..
        } => Some((assignment.clone(), *over)),
        _ => None,
    }
}

#[test]
fn the_largest_shape_that_fits_is_granted() {
    let (mut b, k, _) = rig();
    let soak = b.job(soak(k));
    let m = b.build().unwrap();
    for planner in [
        &mut ListScheduler::default() as &mut dyn Planner,
        &mut FirstFit,
    ] {
        let mut s = queued(&m, Time::ZERO, &[soak]);
        let p = planner.plan(&m, &s);
        assert_eq!(p.starts.len(), 1);
        assert_eq!(
            p.starts[0].assignment.shape, 0,
            "room for all four channels"
        );
        s.start(&m, &p.batch()).unwrap();
        assert_eq!(s.used(m.resource_ids().next().unwrap()), 4);
    }
}

#[test]
fn a_smaller_shape_starts_now_and_backfill_counts_it() {
    let (mut b, k, _) = rig();
    let holder = b.job(Job::new("holder").needs(k, 2).run(fixed(Dur::hours(1))));
    let soak = b.job(soak(k).class(1));
    let small = b.job(Job::new("small").needs(k, 1).run(fixed(Dur::mins(30))));
    let m = b.build().unwrap();
    let r = m.resource_ids().next().unwrap();
    for planner in [
        &mut ListScheduler::default() as &mut dyn Planner,
        &mut FirstFit,
    ] {
        let mut s = queued(&m, Time::ZERO, &[holder]);
        s.start(&m, &[(holder, Assignment::new(vec![r]))]).unwrap();
        s.arrive(soak).unwrap();
        s.arrive(small).unwrap();
        let p = planner.plan(&m, &s);
        let got: Vec<_> = p
            .starts
            .iter()
            .map(|st| (st.job, st.assignment.shape))
            .collect();
        assert_eq!(
            got,
            vec![(soak, 1)],
            "the soak takes two channels, all that is free"
        );
        s.start(&m, &p.batch()).unwrap();
        assert_eq!(s.used(r), 4);
        assert!(
            matches!(s.status(soak), JobStatus::Running { assignment, .. } if assignment.shape == 1)
        );
    }
    // The list scheduler's profile holds the two channels the soak got, not four: the small job is promised
    // the holder's channels when it ends, not the soak's in ten hours.
    let mut s = queued(&m, Time::ZERO, &[holder]);
    s.start(&m, &[(holder, Assignment::new(vec![r]))]).unwrap();
    s.arrive(soak).unwrap();
    s.start(&m, &[(soak, Assignment::shaped(1, vec![r]))])
        .unwrap();
    s.arrive(small).unwrap();
    let p = ListScheduler::default().plan(&m, &s);
    assert!(p.starts.is_empty());
    assert_eq!(p.reservations.len(), 1);
    assert_eq!(p.reservations[0].job, small);
    assert_eq!(p.reservations[0].at, at(Dur::hours(1)));
    match &p.waits[&small] {
        WaitReason::Resource { free_at, .. } => assert_eq!(*free_at, Some(at(Dur::hours(1)))),
        w => panic!("unexpected wait {w:?}"),
    }
    // Its forecast finishes at its own estimate on the shape it got.
    let eta = ListScheduler::default().forecast(&m, &s).etas[&soak];
    assert_eq!(eta.p50.unwrap().finish, at(Dur::hours(10)));
}

#[test]
fn a_shape_is_checked_and_journaled() {
    let (mut b, k, _) = rig();
    let soak = b.job(soak(k));
    let m = b.build().unwrap();
    let r = m.resource_ids().next().unwrap();
    let mut s = queued(&m, Time::ZERO, &[soak]);
    assert!(
        s.clone()
            .start(&m, &[(soak, Assignment::shaped(2, vec![r]))])
            .is_err()
    );
    s.start(&m, &[(soak, Assignment::shaped(1, vec![r]))])
        .unwrap();
    assert_eq!(s.used(r), 2);

    // Shape 0 keeps the old encoding (a bare list); others name their shape.
    assert_eq!(
        serde_json::to_string(&Assignment::new(vec![r])).unwrap(),
        "[0]"
    );
    let shaped = Assignment::shaped(1, vec![r]);
    let text = serde_json::to_string(&shaped).unwrap();
    assert_eq!(text, r#"{"shape":1,"resources":[0]}"#);
    assert_eq!(serde_json::from_str::<Assignment>(&text).unwrap(), shaped);

    // The authority's journal replays a shaped grant to the same state.
    let (b, k, _) = rig();
    let mut auth = Authority::new(
        Live::new(b.build().unwrap(), Time::ZERO),
        LeaseSettings::default(),
    );
    auth.submit(Job::new("holder").needs(k, 2)).unwrap();
    let p = auth.plan(&mut FirstFit);
    auth.grant(&p.batch()).unwrap();
    let soak = auth.submit(soak_job(k)).unwrap();
    let p = auth.plan(&mut ListScheduler::default());
    assert_eq!(p.starts[0].assignment.shape, 1);
    auth.grant(&p.batch()).unwrap();
    let again = Authority::replay(auth.take_journal()).unwrap();
    assert_eq!(again.live().state(), auth.live().state());
    assert!(
        matches!(again.live().state().status(soak), JobStatus::Running { assignment, .. } if assignment.shape == 1)
    );
}

fn soak_job(k: KindId) -> Job {
    soak(k)
}

#[test]
fn a_shape_that_can_never_be_met_is_refused() {
    let (mut b, k, _) = rig();
    let mut j = Job::new("soak").needs(k, 4);
    j.shapes = vec![Job::new("").needs(k, 9).needs];
    b.job(j);
    assert!(matches!(
        b.build(),
        Err(ModelError::NoMatchInShape {
            shape: 1,
            need: 0,
            ..
        })
    ));
    let (mut b, k, _) = rig();
    let mut j = Job::new("soak").needs(k, 4);
    j.shapes = vec![Vec::new()];
    b.job(j);
    assert!(matches!(
        b.build(),
        Err(ModelError::EmptyShape { shape: 1, .. })
    ));
}

/// A PC with 10 GB of RAM, 8 held by a long run, and a build pinned to it that needs 6.
fn crowded(anyway: Option<Dur>) -> (Model, JobId, JobId, State) {
    let mut b = ModelBuilder::new();
    let ram = b.kind("ram", Unit::Bytes);
    let pc = b.node("pc", Labels::new());
    b.pool("pc/ram", ram, pc, 10);
    let long = b.job(Job::new("long").needs(ram, 8).run(fixed(Dur::hours(2))));
    let mut build = Job::new("build").needs(ram, 6).run(fixed(Dur::mins(20)));
    build.needs[0].select.node = Some(pc);
    build.start_anyway_after = anyway;
    let build = b.job(build);
    let m = b.build().unwrap();
    let r = m.resource_ids().next().unwrap();
    let mut s = queued(&m, Time::ZERO, &[long]);
    s.start(&m, &[(long, Assignment::new(vec![r]))]).unwrap();
    s.arrive(build).unwrap();
    (m, long, build, s)
}

#[test]
fn a_pinned_job_starts_past_its_wait_over_capacity() {
    for planner in [
        &mut ListScheduler::default() as &mut dyn Planner,
        &mut FirstFit,
    ] {
        let (m, _, build, mut s) = crowded(Some(Dur::mins(30)));
        let r = m.resource_ids().next().unwrap();
        let p = planner.plan(&m, &s);
        assert!(p.starts.is_empty(), "it waits for its time");
        assert!(matches!(p.waits[&build], WaitReason::Resource { .. }));
        assert_eq!(
            p.wake_at,
            Some(at(Dur::mins(30))),
            "the planner asks to be woken then"
        );
        assert_eq!(s.start_anyway_at(&m, build), Some(at(Dur::mins(30))));
        assert!(!s.may_start_anyway(&m, build));

        s.set_now(at(Dur::mins(30))).unwrap();
        assert!(s.may_start_anyway(&m, build));
        let p = planner.plan(&m, &s);
        assert_eq!(p.starts.len(), 1);
        assert_eq!(p.starts[0].job, build);
        s.start(&m, &p.batch()).unwrap();
        assert_eq!(started(&s, build).map(|(_, over)| over), Some(true));
        assert_eq!(s.used(r), 14, "counted over the capacity of 10");
        assert_eq!(s.headroom(&m, r), -4);
    }
}

#[test]
fn without_a_wait_it_never_starts_over_capacity() {
    let (m, _, build, mut s) = crowded(None);
    s.set_now(at(Dur::hours(1))).unwrap();
    let p = ListScheduler::default().plan(&m, &s);
    assert!(p.starts.is_empty());
    assert!(p.waits.contains_key(&build));
    let r = m.resource_ids().next().unwrap();
    assert!(s.start(&m, &[(build, Assignment::new(vec![r]))]).is_err());
}

#[test]
fn a_job_that_fits_past_its_wait_is_not_over() {
    let (m, long, build, mut s) = crowded(Some(Dur::mins(30)));
    s.set_now(at(Dur::mins(40))).unwrap();
    s.advance(&m, long).unwrap();
    s.advance(&m, long).unwrap();
    s.advance(&m, long).unwrap(); // done: the RAM is free
    let p = ListScheduler::default().plan(&m, &s);
    s.start(&m, &p.batch()).unwrap();
    assert_eq!(started(&s, build).map(|(_, over)| over), Some(false));
}

#[test]
fn the_wait_counts_from_when_it_could_start() {
    let mut b = ModelBuilder::new();
    let ram = b.kind("ram", Unit::Bytes);
    let pc = b.node("pc", Labels::new());
    b.pool("pc/ram", ram, pc, 10);
    let first = b.job(Job::new("first").run(fixed(Dur::hours(1))));
    let mut j = Job::new("j").needs(ram, 4).after(first);
    j.needs[0].select.node = Some(pc);
    j.start_anyway_after = Some(Dur::mins(30));
    let j = b.job(j);
    let m = b.build().unwrap();
    let mut s = queued(&m, Time::ZERO, &[first, j]);
    let p = FirstFit.plan(&m, &s);
    s.start(&m, &p.batch()).unwrap();
    s.set_now(at(Dur::hours(1))).unwrap();
    for _ in 0..3 {
        s.advance(&m, first).unwrap();
    }
    assert_eq!(s.ready_since(&m, j), at(Dur::hours(1)));
    assert_eq!(s.start_anyway_at(&m, j), Some(at(Dur::mins(90))));
}

#[test]
fn a_moldable_job_starts_anyway_on_its_smallest_shape() {
    let (mut b, k, n) = rig();
    let holder = b.job(Job::new("holder").needs(k, 4).run(fixed(Dur::hours(8))));
    let mut j = soak(k);
    j.needs[0].select.node = Some(n);
    j.shapes[0][0].select.node = Some(n);
    j.start_anyway_after = Some(Dur::mins(30));
    let soak = b.job(j);
    let m = b.build().unwrap();
    let r = m.resource_ids().next().unwrap();
    let mut s = queued(&m, Time::ZERO, &[holder]);
    s.start(&m, &[(holder, Assignment::new(vec![r]))]).unwrap();
    s.arrive(soak).unwrap();
    s.set_now(at(Dur::mins(30))).unwrap();
    let p = ListScheduler::default().plan(&m, &s);
    assert_eq!(p.starts[0].assignment.shape, 1);
    s.start(&m, &p.batch()).unwrap();
    assert_eq!(s.used(r), 6);
}

#[test]
fn start_anyway_needs_a_pinned_job_outside_a_group() {
    let mut b = ModelBuilder::new();
    let ram = b.kind("ram", Unit::Bytes);
    let pc = b.node("pc", Labels::new());
    b.pool("pc/ram", ram, pc, 10);
    let mut j = Job::new("loose").needs(ram, 4);
    j.start_anyway_after = Some(Dur::mins(30));
    b.job(j);
    assert!(matches!(b.build(), Err(ModelError::StartAnyway { .. })));

    let mut b = ModelBuilder::new();
    let ram = b.kind("ram", Unit::Bytes);
    let pc = b.node("pc", Labels::new());
    b.pool("pc/ram", ram, pc, 10);
    let mut x = Job::new("x").needs(ram, 4);
    x.needs[0].select.node = Some(pc);
    x.start_anyway_after = Some(Dur::mins(30));
    let x = b.job(x);
    let y = b.job(Job::new("y").needs(ram, 4));
    b.group("g", vec![x, y]);
    assert!(matches!(b.build(), Err(ModelError::StartAnyway { .. })));
}

#[test]
fn job_sets_take_shapes_and_start_anyway_after() {
    let m = choreograph::jobset::from_toml(
        r#"
        [[node]]
        name = "bench"
        [[kind]]
        name = "channel"
        [[resource]]
        name = "bench/channels"
        kind = "channel"
        node = "bench"
        capacity = 4
        [[job]]
        name = "soak"
        needs = [{ kind = "channel", amount = 4, node = "bench" }]
        shapes = [[{ kind = "channel", amount = 2, node = "bench" }]]
        start_anyway_after = "1h"
        run = "10h"
        "#,
    )
    .unwrap();
    let j = m.job(JobId(0));
    assert_eq!(j.shape_count(), 2);
    assert_eq!(j.shape(1).unwrap()[0].amount, 2);
    assert_eq!(j.start_anyway_after, Some(Dur::hours(1)));
}

#[test]
fn the_moldable_bench_example() {
    let m =
        choreograph::jobset::from_toml(include_str!("../../examples/hil_moldable.toml")).unwrap();
    let by = |n: &str| m.job_by_name(n).unwrap();
    let (smoke, soak, cal) = (by("smoke"), by("soak"), by("calibration"));
    let mut planner = ListScheduler::default();
    let mut s = queued(&m, Time::ZERO, &[smoke]);
    s.start(&m, &planner.plan(&m, &s).batch()).unwrap();
    s.set_now(at(Dur::mins(1))).unwrap();
    s.arrive(soak).unwrap();
    s.start(&m, &planner.plan(&m, &s).batch()).unwrap();
    assert_eq!(
        started(&s, soak).map(|(a, _)| a.shape),
        Some(1),
        "the soak goes on two channels"
    );
    s.set_now(at(Dur::mins(2))).unwrap();
    s.arrive(cal).unwrap();
    let p = planner.plan(&m, &s);
    assert!(p.starts.is_empty());
    // The smoke test ends at 20 min, before the calibration's wait is over: it gets a free channel.
    s.set_now(at(Dur::mins(20))).unwrap();
    for _ in 0..3 {
        s.advance(&m, smoke).unwrap();
    }
    s.start(&m, &planner.plan(&m, &s).batch()).unwrap();
    assert_eq!(started(&s, cal).map(|(_, over)| over), Some(false));
}

#[test]
fn the_moldable_bench_example_goes_anyway_when_the_bench_stays_full() {
    let m =
        choreograph::jobset::from_toml(include_str!("../../examples/hil_moldable.toml")).unwrap();
    let by = |n: &str| m.job_by_name(n).unwrap();
    let (soak, cal) = (by("soak"), by("calibration"));
    let mut planner = ListScheduler::default();
    // The soak alone: it gets all four channels.
    let mut s = State::new(&m, Time::ZERO);
    s.set_now(at(Dur::mins(1))).unwrap();
    s.arrive(soak).unwrap();
    s.start(&m, &planner.plan(&m, &s).batch()).unwrap();
    assert_eq!(started(&s, soak).map(|(a, _)| a.shape), Some(0));
    s.set_now(at(Dur::mins(2))).unwrap();
    s.arrive(cal).unwrap();
    let p = planner.plan(&m, &s);
    assert!(p.starts.is_empty());
    assert_eq!(p.wake_at, Some(at(Dur::mins(32))));
    s.set_now(at(Dur::mins(32))).unwrap();
    s.start(&m, &planner.plan(&m, &s).batch()).unwrap();
    assert_eq!(started(&s, cal).map(|(_, over)| over), Some(true));
    assert_eq!(s.used(m.resource_ids().next().unwrap()), 5);
}
