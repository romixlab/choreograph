# Handoff: CACHE-1 result-cache

Done:
- `choreograph/src/cache.rs`: CacheKey (sha2 over name + inputs, pinned test), Verdict, Expiry, Entry, CachePolicy (max_age, rerun_failures), ResultCache (record, lookup, settle, prune), unit tests.
- `state.rs`: Outcome::CachedFail, State::mark_cached_fail (agreed with choreo-planner, choreo-sim, choreo-estimates).

Next:
- Once SIM-1 is on main: rebase, add `cache: Option<ResultCache>` to SimConfig; before each planner call `settle` and push Event::Finished{outcome}; record finished runs (Ok->Pass, Failed->Fail, flaked runs->Flaky; needs a per-job flake count in the sim); SimRun returns the cache; SimStats.cached; a test that a second run with the cache from the first skips every job.
- SIM-1 adds a queued/running index in State: mark_cached_fail needs the same one-line index update as mark_cached after rebase.

Decisions:
- Job name is part of the key (two tests with equal input maps never share a result); jobs without inputs are never cached.
- A cached job settles only after its deps are done; group members settle together or not at all.
- sha2 not blake3: blake3 always build-depends on cc.
