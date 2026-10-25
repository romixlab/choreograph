//! Every job set in `examples/` loads and validates, and the baseline planner can run it to the end.

use choreograph::{FirstFit, JobStatus, Planner, State, Time};

#[test]
fn examples_load_and_run() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../examples");
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "toml"))
        .collect();
    files.sort();
    assert!(!files.is_empty());
    for f in files {
        let text = std::fs::read_to_string(&f).unwrap();
        let m = choreograph::jobset::from_toml(&text)
            .unwrap_or_else(|e| panic!("{}: {e}", f.display()));
        // Run to completion with every phase taking its p50, everything arriving at once.
        let mut s = State::new(&m, Time::ZERO);
        for j in m.job_ids() {
            s.arrive(j).unwrap();
        }
        let mut steps = 0;
        while !s.all_done() {
            steps += 1;
            assert!(steps < 10_000, "{}: no progress", f.display());
            let plan = FirstFit.plan(&m, &s);
            s.start(&m, &plan.batch()).unwrap();
            // Advance the running job whose phase ends first.
            let next = s
                .running()
                .filter_map(|j| match s.status(j) {
                    JobStatus::Running {
                        phase, phase_since, ..
                    } => Some((*phase_since + m.job(j).phases.get(*phase).p50, j)),
                    _ => None,
                })
                .min();
            let Some((t, j)) = next else {
                panic!(
                    "{}: nothing runs and not all done: {:?}",
                    f.display(),
                    plan.waits
                );
            };
            s.set_now(t).unwrap();
            s.advance(&m, j).unwrap();
        }
    }
}
