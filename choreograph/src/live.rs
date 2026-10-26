//! A model that changes while it runs: jobs arrive and leave, nodes join, go down and come back, capacities are
//! measured, labels change.
//!
//! [`Live`] owns a [`Model`] and its [`State`] and keeps them in step. A long-running scheduler builds the static
//! part once (nodes, kinds, resources) and then feeds it what happens:
//!
//! - [`Live::submit`] and [`Live::submit_group`] add jobs, checked like a job set and queued at once (a job
//!   whose arrival or release time is later waits for it);
//! - [`Live::set_capacity`] sets a resource's capacity as measured; when it drops below what running jobs hold,
//!   nothing new starts there and the running jobs keep what they hold;
//! - [`Live::set_node_up`] takes a node out of placement (or back), [`Live::node_lost`] drops the jobs that ran
//!   on a node that is gone;
//! - [`Live::add_node`] and [`Live::add_resource`] grow the fleet, [`Live::set_node_labels`] and
//!   [`Live::set_node_policy`] change what a node offers and which work it keeps away (see [`Node::policy`]);
//! - [`Live::compact`] removes finished jobs, so memory stays bounded however long it runs.
//!
//! Planners read [`Live::model`] and [`Live::state`] as before; the planner and the state work from the indexes
//! of queued and running jobs, so a round costs what is waiting and running, not what ever ran.
//!
//! ```
//! use choreograph::live::Live;
//! use choreograph::model::{Job, Labels, ModelBuilder, Unit};
//! use choreograph::{ListScheduler, Policy, Time};
//!
//! let mut b = ModelBuilder::new();
//! let ram = b.kind("ram", Unit::Count);
//! let n = b.node("n", Labels::new());
//! let r = b.pool("n/ram", ram, n, 16);
//! let mut live = Live::new(b.build().unwrap(), Time::ZERO);
//! let mut planner = ListScheduler::new(Policy::default());
//!
//! let a = live.submit(Job::new("a").needs(ram, 10)).unwrap();
//! let plan = live.plan(&mut planner);
//! live.start(&plan.batch()).unwrap();
//!
//! // The node is measured with less room than declared: b waits, a keeps running.
//! live.set_capacity(r, Some(8));
//! let b = live.submit(Job::new("b").needs(ram, 4)).unwrap();
//! assert!(live.plan(&mut planner).starts.is_empty());
//! assert!(live.state().running().any(|j| j == a));
//! # let _ = b;
//! ```

use serde::{Deserialize, Serialize};

use crate::model::{
    GroupId, Job, JobId, Labels, Model, ModelError, Node, NodeId, Remap, Resource, ResourceId,
};
use crate::plan::{Plan, Planner};
use crate::state::{Assignment, JobStatus, LostJobs, State, StateError};
use crate::time::Time;

/// A model and its state that change together at runtime.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Live {
    model: Model,
    state: State,
}

impl Live {
    /// Starts from a validated model at `now`. Jobs already in it are queued, like submitted ones.
    pub fn new(model: Model, now: Time) -> Live {
        let mut state = State::new(&model, now);
        for j in model.job_ids() {
            state.arrive(j).expect("a new state has every job pending");
        }
        Live { model, state }
    }

    pub fn model(&self) -> &Model {
        &self.model
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    /// The model and a mutable state, for the state's own checked changes ([`State::advance`], ...).
    pub fn parts_mut(&mut self) -> (&Model, &mut State) {
        (&self.model, &mut self.state)
    }

    pub fn now(&self) -> Time {
        self.state.now()
    }

    /// Moves time forward (never back).
    pub fn set_now(&mut self, t: Time) -> Result<(), StateError> {
        self.state.set_now(t)
    }

    /// Adds a job and queues it. It may come after jobs still in the model (not after compacted ones).
    pub fn submit(&mut self, job: Job) -> Result<JobId, ModelError> {
        let ids = self.add(vec![job], None)?;
        Ok(ids[0])
    }

    /// Adds jobs that start together or not at all, as a group, and queues them. They may come after each
    /// other and after jobs still in the model.
    pub fn submit_group(
        &mut self,
        name: impl Into<String>,
        jobs: Vec<Job>,
    ) -> Result<(GroupId, Vec<JobId>), ModelError> {
        let ids = self.add(jobs, Some(name.into()))?;
        let g = GroupId(self.model.groups().len() as u32 - 1);
        Ok((g, ids))
    }

    fn add(&mut self, jobs: Vec<Job>, group: Option<String>) -> Result<Vec<JobId>, ModelError> {
        let ids = self.model.add_jobs(jobs, group)?;
        self.state.grow(&self.model);
        for j in &ids {
            self.state.arrive(*j).expect("a new job is pending");
        }
        Ok(ids)
    }

    /// Adds a node, up. Its policy labels (see [`Node::policy`]) apply from the start.
    pub fn add_node(
        &mut self,
        name: impl Into<String>,
        labels: Labels,
        policy: Labels,
    ) -> Result<NodeId, ModelError> {
        let n = self.model.add_node(Node {
            name: name.into(),
            labels,
            policy,
        })?;
        self.state.grow(&self.model);
        Ok(n)
    }

    /// Adds a resource with its declared capacity; it is free and in its initial state.
    pub fn add_resource(&mut self, r: Resource) -> Result<ResourceId, ModelError> {
        let id = self.model.add_resource(r)?;
        self.state.grow(&self.model);
        Ok(id)
    }

    /// Replaces a node's labels. Jobs running there keep running even if they would no longer match.
    pub fn set_node_labels(&mut self, n: NodeId, labels: Labels) {
        self.model.set_node_labels(n, labels);
    }

    /// Replaces a node's policy labels (see [`Node::policy`]). Jobs running there keep running.
    pub fn set_node_policy(&mut self, n: NodeId, policy: Labels) {
        self.model.set_node_policy(n, policy);
    }

    /// See [`State::set_capacity`].
    pub fn set_capacity(&mut self, r: ResourceId, capacity: Option<u64>) {
        self.state.set_capacity(r, capacity);
    }

    /// See [`State::set_node_up`].
    pub fn set_node_up(&mut self, n: NodeId, up: bool) {
        self.state.set_node_up(n, up);
    }

    /// See [`State::node_lost`].
    pub fn node_lost(&mut self, n: NodeId, then: LostJobs) -> Vec<JobId> {
        self.state.node_lost(&self.model, n, then)
    }

    /// Asks a planner what starts now.
    pub fn plan(&self, planner: &mut impl Planner) -> Plan {
        planner.plan(&self.model, &self.state)
    }

    /// See [`State::start`].
    pub fn start(&mut self, batch: &[(JobId, Assignment)]) -> Result<(), StateError> {
        self.state.start(&self.model, batch)
    }

    /// See [`State::advance`].
    pub fn advance(&mut self, j: JobId) -> Result<&JobStatus, StateError> {
        self.state.advance(&self.model, j)
    }

    /// See [`State::fail`].
    pub fn fail(&mut self, j: JobId) -> Result<(), StateError> {
        self.state.fail(j)
    }

    /// See [`State::cancel`].
    pub fn cancel(&mut self, j: JobId) -> Result<(), StateError> {
        self.state.cancel(j)
    }

    /// Removes the jobs done at or before `done_by`, except those a remaining job comes after, and the groups
    /// left empty; the remaining jobs and groups are renumbered in order. Ids held from before (a plan, a
    /// caller's map) are turned into new ones with the returned [`Remap`]; jobs are best named by their name
    /// across a compaction.
    pub fn compact(&mut self, done_by: Time) -> Remap {
        let keep = self.state.keep_after_compaction(&self.model, done_by);
        let remap = self.model.compact(&keep);
        self.state.remap(&remap);
        remap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Estimate, KindId, ModelBuilder, Unit};
    use crate::plan::{FirstFit, WaitReason};
    use crate::state::Outcome;
    use crate::time::Dur;
    use crate::{ListScheduler, Policy};

    fn labels(kv: &[(&str, &str)]) -> Labels {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Two nodes with RAM: `a` with 16, `b` with 32; no jobs.
    fn fleet() -> (Live, KindId, NodeId, NodeId, ResourceId, ResourceId) {
        let mut b = ModelBuilder::new();
        let ram = b.kind("ram", Unit::Count);
        let na = b.node("a", Labels::new());
        let nb = b.node("b", Labels::new());
        let ra = b.pool("a/ram", ram, na, 16);
        let rb = b.pool("b/ram", ram, nb, 32);
        (
            Live::new(b.build().unwrap(), Time::ZERO),
            ram,
            na,
            nb,
            ra,
            rb,
        )
    }

    fn mins(m: i64) -> Dur {
        Dur::secs(60 * m)
    }

    fn list() -> ListScheduler {
        ListScheduler::new(Policy::default())
    }

    /// Plans with the list scheduler and applies the starts; returns the plan.
    fn step(live: &mut Live) -> Plan {
        let plan = live.plan(&mut list());
        live.start(&plan.batch()).unwrap();
        plan
    }

    fn finish(live: &mut Live, j: JobId) {
        for _ in 0..3 {
            live.advance(j).unwrap();
        }
    }

    fn on(live: &Live, j: JobId) -> Vec<ResourceId> {
        match live.state().status(j) {
            JobStatus::Running { assignment, .. } => assignment.0.clone(),
            s => panic!("not running: {s:?}"),
        }
    }

    #[test]
    fn jobs_arrive_at_runtime() {
        let (mut live, ram, ..) = fleet();
        assert!(step(&mut live).starts.is_empty());
        let a = live.submit(Job::new("a").needs(ram, 10)).unwrap();
        let b = live
            .submit(Job::new("b").needs(ram, 10).after(a).arrival(Time(5)))
            .unwrap();
        assert_eq!(live.state().queued().collect::<Vec<_>>(), vec![a, b]);
        let p = step(&mut live);
        assert_eq!(p.starts.len(), 1);
        assert_eq!(p.waits[&b], WaitReason::Dep { job: a });
        live.set_now(Time(10)).unwrap();
        finish(&mut live, a);
        assert_eq!(step(&mut live).starts[0].job, b);
        // A job arriving later than now waits for its arrival.
        let c = live
            .submit(Job::new("c").needs(ram, 1).arrival(Time(20)))
            .unwrap();
        assert_eq!(
            step(&mut live).waits[&c],
            WaitReason::Release { at: Time(20) }
        );
    }

    #[test]
    fn a_refused_submit_changes_nothing() {
        let (mut live, ram, ..) = fleet();
        live.submit(Job::new("a").needs(ram, 1)).unwrap();
        let before = live.clone();
        assert!(matches!(
            live.submit(Job::new("a").needs(ram, 1)),
            Err(ModelError::Duplicate { .. })
        ));
        assert!(matches!(
            live.submit(Job::new("x").needs(ram, 64)),
            Err(ModelError::NoMatch { .. })
        ));
        assert!(matches!(
            live.submit(Job::new("x").after(JobId(7))),
            Err(ModelError::BadId { .. })
        ));
        assert!(matches!(
            live.submit(Job::new("x").after(JobId(1))),
            Err(ModelError::Cycle { .. })
        ));
        // A group is all or nothing: its second job cannot fit anywhere.
        assert!(
            live.submit_group(
                "g",
                vec![Job::new("g1").needs(ram, 1), Job::new("g2").needs(ram, 99)]
            )
            .is_err()
        );
        assert_eq!(live, before);
    }

    #[test]
    fn a_group_submitted_at_runtime_starts_together() {
        let (mut live, ram, ..) = fleet();
        let first = live.submit(Job::new("first").needs(ram, 30)).unwrap();
        let (g, ids) = live
            .submit_group(
                "change",
                vec![
                    Job::new("x").needs(ram, 16).after(first),
                    Job::new("y").needs(ram, 16).after(first),
                ],
            )
            .unwrap();
        assert_eq!(live.model().group(g).members, ids);
        assert_eq!(live.model().job(ids[1]).group, Some(g));
        let p = step(&mut live);
        assert_eq!(p.starts.len(), 1);
        assert_eq!(p.waits[&ids[0]], WaitReason::Dep { job: first });
        finish(&mut live, first);
        let p = step(&mut live);
        assert_eq!(
            p.starts.iter().map(|s| s.job).collect::<Vec<_>>(),
            ids,
            "both at once"
        );
    }

    #[test]
    fn capacity_drop_below_use_stops_new_starts_and_keeps_running_jobs() {
        let (mut live, ram, _, nb, ra, rb) = fleet();
        live.set_node_up(nb, false);
        let a = live
            .submit(Job::new("a").needs(ram, 10).run(Estimate::fixed(mins(10))))
            .unwrap();
        step(&mut live);
        assert_eq!(on(&live, a), vec![ra]);

        live.set_capacity(ra, Some(8));
        assert_eq!(live.state().capacity(live.model(), ra), 8);
        assert_eq!(live.state().free(live.model(), ra), 0);
        assert_eq!(live.state().headroom(live.model(), ra), -2);
        let b = live
            .submit(Job::new("b").needs(ram, 2).run(Estimate::fixed(mins(1))))
            .unwrap();
        let p = step(&mut live);
        assert!(p.starts.is_empty(), "nothing new starts while short");
        assert!(matches!(
            &p.waits[&b],
            WaitReason::Resource { resource, held_by, free_at: Some(t), .. }
                if *resource == ra && *held_by == vec![a] && *t == Time::ZERO + mins(10)
        ));
        assert_eq!(on(&live, a), vec![ra], "the running job keeps its place");
        assert_eq!(live.state().used(ra), 10);
        // The state refuses a start a planner should not have made.
        assert!(matches!(
            live.start(&[(b, Assignment(vec![ra]))]),
            Err(StateError::OverCapacity { free: 0, .. })
        ));
        // FirstFit sees the same free amounts.
        assert!(live.plan(&mut FirstFit).starts.is_empty());

        live.set_now(Time::ZERO + mins(10)).unwrap();
        finish(&mut live, a);
        assert_eq!(live.state().headroom(live.model(), ra), 8);
        assert_eq!(step(&mut live).starts[0].job, b);

        // Back to the declared capacity.
        live.set_capacity(ra, None);
        assert_eq!(live.state().capacity(live.model(), ra), 16);
        assert_eq!(live.state().measured(rb), None);
    }

    #[test]
    fn a_reservation_waits_for_the_shortfall_to_clear() {
        // Capacity 16 measured down to 8 while x (6) and y (6) run: b (4) fits only once both are gone,
        // not after the first one ends (8 - 6 = 2 < 4).
        let mut bld = ModelBuilder::new();
        let ram = bld.kind("ram", Unit::Count);
        let n = bld.node("n", Labels::new());
        let r = bld.pool("n/ram", ram, n, 16);
        let mut live = Live::new(bld.build().unwrap(), Time::ZERO);
        for (name, m) in [("x", 5), ("y", 9)] {
            live.submit(Job::new(name).needs(ram, 6).run(Estimate::fixed(mins(m))))
                .unwrap();
        }
        step(&mut live);
        live.set_capacity(r, Some(8));
        let b = live
            .submit(Job::new("b").needs(ram, 4).run(Estimate::fixed(mins(1))))
            .unwrap();
        let p = step(&mut live);
        assert!(p.starts.is_empty());
        assert_eq!(p.reservations.len(), 1);
        assert_eq!(p.reservations[0].job, b);
        assert_eq!(p.reservations[0].at, Time::ZERO + mins(9));
        let eta = list().forecast(live.model(), live.state()).etas[&b];
        assert_eq!(eta.p50.unwrap().start, Time::ZERO + mins(9));
    }

    #[test]
    fn a_node_that_goes_down_drains() {
        let (mut live, ram, na, nb, ra, rb) = fleet();
        let big = live.submit(Job::new("big").needs(ram, 20)).unwrap();
        step(&mut live);
        assert_eq!(on(&live, big), vec![rb]);
        live.set_node_up(nb, false);
        assert!(!live.state().node_up(nb));
        assert_eq!(live.state().free(live.model(), rb), 0);
        // Only b could take it, and b is down.
        let big2 = live.submit(Job::new("big2").needs(ram, 20)).unwrap();
        let small = live.submit(Job::new("small").needs(ram, 4)).unwrap();
        let p = step(&mut live);
        assert_eq!(p.waits[&big2], WaitReason::NodesDown { need: 0 });
        assert_eq!(
            on(&live, small),
            vec![ra],
            "work goes to the node that is up"
        );
        assert_eq!(on(&live, big), vec![rb], "the running job keeps running");
        finish(&mut live, big);
        assert_eq!(live.state().used(rb), 0, "it releases as usual");
        assert!(step(&mut live).starts.is_empty());
        live.set_node_up(nb, true);
        assert_eq!(step(&mut live).starts[0].job, big2);
        let _ = na;
    }

    #[test]
    fn a_lost_node_requeues_or_fails_its_jobs() {
        let (mut live, ram, _, nb, ra, rb) = fleet();
        let x = live.submit(Job::new("x").needs(ram, 20)).unwrap();
        let y = live.submit(Job::new("y").needs(ram, 10)).unwrap();
        let z = live.submit(Job::new("z").needs(ram, 10)).unwrap();
        step(&mut live);
        assert_eq!(on(&live, x), vec![rb]);
        let on_b: Vec<JobId> = [y, z]
            .into_iter()
            .filter(|j| on(&live, *j) == vec![rb])
            .collect();
        // One of y and z shares b with x; cancel it, so it is already ending when b is lost.
        let w = on_b[0];
        live.cancel(w).unwrap();
        let hit = live.node_lost(nb, LostJobs::Requeue);
        assert_eq!(hit, vec![x.min(w), x.max(w)], "in id order");
        assert!(!live.state().node_up(nb));
        assert_eq!(live.state().used(rb), 0, "a lost node holds nothing");
        assert_eq!(*live.state().status(x), JobStatus::Queued);
        assert_eq!(live.state().outcome(w), Some(Outcome::Cancelled));
        // x needs 20: a has 16, b is down.
        let p = step(&mut live);
        assert_eq!(p.waits[&x], WaitReason::NodesDown { need: 0 });
        live.set_node_up(nb, true);
        step(&mut live);
        assert_eq!(on(&live, x), vec![rb]);

        let lost = live.node_lost(nb, LostJobs::Fail);
        assert_eq!(lost, vec![x]);
        assert_eq!(live.state().outcome(x), Some(Outcome::Failed));
        assert!(live.state().used(ra) > 0, "jobs on other nodes go on");
    }

    #[test]
    fn compaction_drops_finished_jobs_and_renumbers() {
        let (mut live, ram, ..) = fleet();
        let a = live.submit(Job::new("a").needs(ram, 4)).unwrap();
        let c = live.submit(Job::new("c").needs(ram, 4)).unwrap();
        let d = live.submit(Job::new("d").needs(ram, 4)).unwrap();
        step(&mut live);
        let b = live.submit(Job::new("b").needs(ram, 4).after(a)).unwrap();
        live.set_now(Time(10)).unwrap();
        finish(&mut live, a);
        finish(&mut live, c);
        // d runs on, b is queued and comes after a, c is done and nobody needs it.
        live.set_now(Time(20)).unwrap();
        let remap = live.compact(Time(10));
        assert_eq!(remap.removed().collect::<Vec<_>>(), vec![c]);
        assert_eq!(remap.job(a), Some(JobId(0)), "kept: b comes after it");
        assert_eq!(remap.job(c), None);
        let (d2, b2) = (remap.job(d).unwrap(), remap.job(b).unwrap());
        assert_eq!((d2, b2), (JobId(1), JobId(2)));
        let m = live.model();
        assert_eq!(m.jobs().len(), 3);
        assert_eq!(m.job_by_name("b"), Some(b2));
        assert_eq!(m.job(b2).after, vec![JobId(0)]);
        assert_eq!(live.state().queued().collect::<Vec<_>>(), vec![b2]);
        assert_eq!(live.state().running().collect::<Vec<_>>(), vec![d2]);
        let rd = on(&live, d2)[0];
        assert_eq!(live.state().holders(rd).collect::<Vec<_>>(), vec![(d2, 4)]);
        // The renumbered state goes on as before.
        assert_eq!(step(&mut live).starts[0].job, b2);
        finish(&mut live, d2);
        assert!(live.state().holders(rd).all(|(j, _)| j != d2));
        finish(&mut live, b2);
        // The name c is free again.
        live.submit(Job::new("c").needs(ram, 1)).unwrap();
        live.set_now(Time(30)).unwrap();
        let r = live.compact(Time(30));
        assert_eq!(r.removed().count(), 3);
        assert_eq!(live.model().jobs().len(), 1);
        assert_eq!(live.model().job(JobId(0)).name, "c");
        assert_eq!(live.state().queued().collect::<Vec<_>>(), vec![JobId(0)]);
    }

    #[test]
    fn compaction_drops_empty_groups() {
        let (mut live, ram, ..) = fleet();
        live.submit_group(
            "g",
            vec![Job::new("x").needs(ram, 1), Job::new("y").needs(ram, 1)],
        )
        .unwrap();
        let (h, _) = live
            .submit_group("h", vec![Job::new("z").needs(ram, 1)])
            .unwrap();
        step(&mut live);
        for j in [JobId(0), JobId(1)] {
            finish(&mut live, j);
        }
        let remap = live.compact(Time::ZERO);
        assert_eq!(remap.group(GroupId(0)), None);
        assert_eq!(remap.group(h), Some(GroupId(0)));
        assert_eq!(live.model().groups().len(), 1);
        assert_eq!(live.model().group(GroupId(0)).members, vec![JobId(0)]);
        assert_eq!(live.model().job(JobId(0)).group, Some(GroupId(0)));
        assert!(live.compact(Time::ZERO).is_identity());
    }

    #[test]
    fn policy_labels_keep_work_away_unless_asked() {
        let (mut live, ram, _, _, ra, _) = fleet();
        // A node that hosts services: the most room, but builds stay off it.
        let svc = live
            .add_node("svc", Labels::new(), labels(&[("builds", "false")]))
            .unwrap();
        let rs = live
            .add_resource(Resource {
                name: "svc/ram".into(),
                kind: ram,
                node: svc,
                capacity: 64,
                labels: Labels::new(),
                states: None,
            })
            .unwrap();
        assert!(matches!(
            live.submit(Job::new("huge").needs(ram, 40)),
            Err(ModelError::NoMatch { .. })
        ));
        let build = live.submit(Job::new("build").needs(ram, 4)).unwrap();
        let mut light = Job::new("light").needs(ram, 4);
        light.needs[0].select.node_labels = labels(&[("builds", "false")]);
        let light = live.submit(light).unwrap();
        step(&mut live);
        assert_ne!(on(&live, build), vec![rs]);
        assert_eq!(
            on(&live, light),
            vec![rs],
            "only svc has the policy it asked for"
        );
        // A need that tolerates the policy may land on svc or elsewhere. With b down, only svc fits 20.
        let mut lint = Job::new("lint").needs(ram, 20);
        lint.needs[0].select.tolerate = labels(&[("builds", "false")]);
        let lint = live.submit(lint).unwrap();
        live.set_node_up(NodeId(1), false);
        let mut small = Job::new("small").needs(ram, 2);
        small.needs[0].select.tolerate = labels(&[("builds", "false")]);
        small.needs[0].select.node = Some(NodeId(0));
        let small = live.submit(small).unwrap();
        step(&mut live);
        assert_eq!(on(&live, lint), vec![rs]);
        assert_eq!(on(&live, small), vec![ra]);
        // The policy changes at runtime: svc takes builds now.
        live.set_node_policy(svc, Labels::new());
        live.set_capacity(ra, Some(0));
        let mut b2 = Job::new("b2").needs(ram, 40);
        b2.needs[0].select.node = Some(svc);
        let b2 = live.submit(b2).unwrap();
        step(&mut live);
        assert_eq!(on(&live, b2), vec![rs]);
    }

    #[test]
    fn a_new_node_gets_work_because_it_has_room() {
        let (mut live, ram, ..) = fleet();
        let j = live.submit(Job::new("j").needs(ram, 30)).unwrap();
        let k = live.submit(Job::new("k").needs(ram, 30)).unwrap();
        step(&mut live);
        assert!(matches!(
            live.plan(&mut list()).waits[&k],
            WaitReason::Resource { .. }
        ));
        let big = live.add_node("big", Labels::new(), Labels::new()).unwrap();
        let rbig = live
            .add_resource(Resource {
                name: "big/ram".into(),
                kind: ram,
                node: big,
                capacity: 128,
                labels: Labels::new(),
                states: None,
            })
            .unwrap();
        assert_eq!(live.state().free(live.model(), rbig), 128);
        assert_eq!(step(&mut live).starts[0].job, k);
        assert_eq!(on(&live, k), vec![rbig]);
        let _ = j;
        assert!(matches!(
            live.add_node("big", Labels::new(), Labels::new()),
            Err(ModelError::Duplicate { .. })
        ));
    }

    #[test]
    fn node_labels_change_at_runtime() {
        let (mut live, ram, na, nb, ra, _) = fleet();
        live.set_node_labels(na, labels(&[("toolchain", "nightly")]));
        let mut j = Job::new("j").needs(ram, 4);
        j.needs[0].select.node_labels = labels(&[("toolchain", "nightly")]);
        let j = live.submit(j.clone()).unwrap();
        live.set_node_labels(na, Labels::new());
        let p = live.plan(&mut list());
        assert_eq!(p.waits[&j], WaitReason::NoMatch { need: 0 });
        live.set_node_labels(nb, labels(&[("toolchain", "nightly")]));
        step(&mut live);
        assert_ne!(on(&live, j), vec![ra]);
    }
}
