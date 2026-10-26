//! The journal on disk: `journal.jsonl` in the state directory, one JSON [`Line`] per line, appended and
//! synced before the service answers.
//!
//! The authority's own entries ([`choreograph::Entry`]) carry the scheduling state; the service adds what it
//! keeps beside them: which client submitted each job under which id ([`Line::Jobs`]) and which ids a
//! compaction removed ([`Line::Compacted`]). The first line is always a [`Line::Snapshot`]. At start and after
//! [`Settings::snapshot_every`](crate::config::Settings::snapshot_every) lines the whole journal is replaced by
//! a fresh snapshot: written to `journal.jsonl.new`, synced, renamed over the old one.
//!
//! A last line cut short (the process died while writing it, so nobody got an answer for it) is dropped on
//! reading; anything else unreadable stops the service, since only a person can decide what to do with it.
//! A lock on `lock` keeps a second service off the same directory.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use choreograph::Entry;
use serde::{Deserialize, Serialize};

use crate::api::JobRef;

/// The version of the service's journal lines. A journal with a newer one is refused.
pub const STATE_VERSION: u32 = 1;

/// One line of the journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Line {
    /// Everything at one time: the authority's snapshot entry and the service's job table.
    Snapshot {
        version: u32,
        /// The id the next job gets.
        next_ref: JobRef,
        /// One per job in the authority's model, in its job id order.
        jobs: Vec<JobMeta>,
        entry: Entry,
    },
    /// An accepted change of the authority.
    Entry(Entry),
    /// Jobs just submitted, in the order the authority added them.
    Jobs(Vec<JobMeta>),
    /// The jobs a compaction removed.
    Compacted { removed: Vec<JobRef> },
}

/// What the service keeps about a job beside the authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobMeta {
    pub id: JobRef,
    /// The client that submitted it.
    pub client: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{path}: {err}")]
    Io { path: PathBuf, err: std::io::Error },
    #[error("{path}: another choreographd holds this state directory")]
    Locked { path: PathBuf },
    #[error("{path}:{line}: unreadable journal line: {err}")]
    Corrupt {
        path: PathBuf,
        line: usize,
        err: String,
    },
}

impl StoreError {
    /// Whether a person has to act before the service can start (exit 78): an unreadable journal. A held
    /// lock or an IO error may go away by itself.
    pub fn needs_a_person(&self) -> bool {
        matches!(self, StoreError::Corrupt { .. })
    }
}

/// The open journal.
#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    file: File,
    /// Held for the store's life.
    _lock: File,
    /// Lines written since the last snapshot.
    lines: u64,
}

impl Store {
    /// Opens the state directory (made if missing), takes its lock and reads the journal: its lines, or none
    /// when there is no journal yet.
    pub fn open(dir: &Path) -> Result<(Store, Vec<Line>), StoreError> {
        let io = |path: &Path| {
            let path = path.to_path_buf();
            move |err| StoreError::Io { path, err }
        };
        std::fs::create_dir_all(dir).map_err(io(dir))?;
        let lock_path = dir.join("lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(io(&lock_path))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(StoreError::Locked { path: dir.into() });
            }
            Err(std::fs::TryLockError::Error(err)) => return Err(io(&lock_path)(err)),
        }
        let path = dir.join("journal.jsonl");
        let (lines, good) = read(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io(&path))?;
        // Drop a cut last line, so the next line does not continue it.
        if file.metadata().map_err(io(&path))?.len() > good {
            file.set_len(good).map_err(io(&path))?;
        }
        Ok((
            Store {
                dir: dir.into(),
                file,
                _lock: lock,
                lines: lines.len() as u64,
            },
            lines,
        ))
    }

    /// Reads a journal without opening it for writing or taking the lock (for `choreographd check`).
    pub fn read_only(dir: &Path) -> Result<Vec<Line>, StoreError> {
        read(&dir.join("journal.jsonl")).map(|(lines, _)| lines)
    }

    /// The state version of a journal's snapshot (its first line), without reading the rest: `None` when there
    /// is no journal yet. For the self-check, which must answer even for a journal it cannot read.
    pub fn data_version(dir: &Path) -> Result<Option<u32>, StoreError> {
        let path = dir.join("journal.jsonl");
        let f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StoreError::Io { path, err }),
        };
        let mut first = String::new();
        BufReader::new(f)
            .read_line(&mut first)
            .map_err(|err| StoreError::Io {
                path: path.clone(),
                err,
            })?;
        if first.trim().is_empty() {
            return Ok(None);
        }
        let v: serde_json::Value =
            serde_json::from_str(&first).map_err(|e| StoreError::Corrupt {
                path: path.clone(),
                line: 1,
                err: e.to_string(),
            })?;
        match v.pointer("/snapshot/version").and_then(|x| x.as_u64()) {
            Some(n) => Ok(Some(n as u32)),
            None => Err(StoreError::Corrupt {
                path,
                line: 1,
                err: "not a snapshot with a version".into(),
            }),
        }
    }

    fn path(&self) -> PathBuf {
        self.dir.join("journal.jsonl")
    }

    /// Lines written since the last snapshot.
    pub fn lines(&self) -> u64 {
        self.lines
    }

    /// Appends lines and syncs them to disk.
    pub fn append(&mut self, lines: &[Line]) -> Result<(), StoreError> {
        if lines.is_empty() {
            return Ok(());
        }
        let buf = encode(lines);
        let path = self.path();
        self.file
            .write_all(&buf)
            .and_then(|()| self.file.sync_data())
            .map_err(|err| StoreError::Io { path, err })?;
        self.lines += lines.len() as u64;
        Ok(())
    }

    /// Replaces the journal with these lines (a snapshot): written beside it, synced, renamed over it.
    pub fn replace(&mut self, lines: &[Line]) -> Result<(), StoreError> {
        let path = self.path();
        let new = self.dir.join("journal.jsonl.new");
        let io = |p: &Path| {
            let p = p.to_path_buf();
            move |err| StoreError::Io { path: p, err }
        };
        let mut f = File::create(&new).map_err(io(&new))?;
        f.write_all(&encode(lines))
            .and_then(|()| f.sync_all())
            .map_err(io(&new))?;
        std::fs::rename(&new, &path).map_err(io(&path))?;
        File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .map_err(io(&self.dir))?;
        self.file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(io(&path))?;
        self.lines = lines.len() as u64;
        Ok(())
    }
}

fn encode(lines: &[Line]) -> Vec<u8> {
    let mut buf = Vec::new();
    for l in lines {
        serde_json::to_writer(&mut buf, l).expect("a journal line always serialises");
        buf.push(b'\n');
    }
    buf
}

/// The lines of a journal and the length in bytes of the part that was read whole.
fn read(path: &Path) -> Result<(Vec<Line>, u64), StoreError> {
    let f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(err) => {
            return Err(StoreError::Io {
                path: path.into(),
                err,
            });
        }
    };
    let mut out = Vec::new();
    let mut r = BufReader::new(f);
    let mut buf = String::new();
    let mut n = 0;
    let mut good = 0u64;
    loop {
        buf.clear();
        let got = r.read_line(&mut buf).map_err(|err| StoreError::Io {
            path: path.into(),
            err,
        })?;
        if got == 0 {
            break;
        }
        n += 1;
        let whole = buf.ends_with('\n');
        // Cut short while it was written (even before just its newline): the change was never answered, so it
        // never happened.
        if !whole {
            break;
        }
        match serde_json::from_str::<Line>(buf.trim_end()) {
            Ok(l) => {
                out.push(l);
                good += got as u64;
            }
            Err(e) => {
                return Err(StoreError::Corrupt {
                    path: path.into(),
                    line: n,
                    err: e.to_string(),
                });
            }
        }
    }
    Ok((out, good))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jobs(id: JobRef) -> Line {
        Line::Jobs(vec![JobMeta {
            id,
            client: "c".into(),
        }])
    }

    #[test]
    fn lines_come_back_and_a_cut_last_line_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut s, lines) = Store::open(dir.path()).unwrap();
            assert!(lines.is_empty());
            s.append(&[jobs(1), jobs(2)]).unwrap();
            assert!(matches!(
                Store::open(dir.path()),
                Err(StoreError::Locked { .. })
            ));
        }
        let path = dir.path().join("journal.jsonl");
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(br#"{"jobs":[{"id":3,"cl"#).unwrap();
        let (mut s, lines) = Store::open(dir.path()).unwrap();
        assert_eq!(lines, vec![jobs(1), jobs(2)]);
        s.append(&[jobs(4)]).unwrap();
        drop(s);
        let (mut s, lines) = Store::open(dir.path()).unwrap();
        assert_eq!(
            lines,
            vec![jobs(1), jobs(2), jobs(4)],
            "the cut line is gone"
        );
        s.replace(&[jobs(7)]).unwrap();
        s.append(&[jobs(8)]).unwrap();
        assert_eq!(s.lines(), 2);
        drop(s);
        assert_eq!(
            Store::read_only(dir.path()).unwrap(),
            vec![jobs(7), jobs(8)]
        );
    }

    #[test]
    fn a_bad_line_in_the_middle_needs_a_person() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("journal.jsonl"),
            "{\"jobs\":[]}\nnonsense\n{\"jobs\":[]}\n",
        )
        .unwrap();
        let e = Store::open(dir.path()).unwrap_err();
        assert!(e.needs_a_person(), "{e}");
        assert!(e.to_string().contains(":2:"), "{e}");
    }
}
