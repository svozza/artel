//! Decide whether a path should be included in a workspace sync.
//!
//! Three layers, in order:
//!
//! 1. Hardcoded skips (`.git`, `target`, `node_modules`, `.DS_Store`,
//!    `*.swp`, `*.tmp`). Highest priority — these never sync regardless
//!    of `.gitignore` config.
//! 2. Symlink check. We deliberately do not follow links.
//! 3. `.gitignore` patterns at the workspace root, parsed via the
//!    `ignore` crate. Honoured for everything *except* the
//!    `.gitignore` file itself.
//! 4. Size cap (1 MiB). Larger files are surfaced to consumers as
//!    [`FilterDecision::Skip(SkipReason::TooLarge)`] so they can decide
//!    whether to log/notify.
//!
//! Ported near-verbatim from harness's `session/workspace/filter.rs`.

use std::fs;
use std::path::{Path, PathBuf};

use ignore::gitignore::Gitignore;

/// Files larger than this are skipped. Keeps the doc small enough that
/// memory-store + naïve full-replay stay tractable for the MVP.
pub const MAX_FILE_SIZE: u64 = 1 << 20;

/// Pre-parsed view of the workspace root + its `.gitignore` (if any).
///
/// Cheap to construct (one stat, one parse). Kept around so the
/// watcher and applier can call [`Self::check`] without re-parsing
/// the gitignore on every event.
#[derive(Debug)]
pub struct WorkspaceFilter {
    pub root: PathBuf,
    gitignore: Option<Gitignore>,
}

/// Outcome of [`WorkspaceFilter::check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilterDecision {
    /// File passes all rules and should be synced.
    Include,
    /// File is skipped; the variant explains why.
    Skip(SkipReason),
}

/// Why a path was skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// Matched one of the hardcoded skips.
    Hardcoded,
    /// Matched a `.gitignore` pattern.
    Gitignored,
    /// Path is a symlink. We never follow them.
    Symlink,
    /// File exceeded [`MAX_FILE_SIZE`].
    TooLarge {
        /// Actual size in bytes; surfaces to consumers so they can log.
        size: u64,
    },
}

impl WorkspaceFilter {
    /// Build a filter for `root`. If `root/.gitignore` exists, it's
    /// parsed eagerly; parse errors are ignored silently to match the
    /// reference impl.
    #[must_use]
    pub fn new(root: &Path) -> Self {
        let root = root.to_path_buf();
        let gitignore_path = root.join(".gitignore");
        let gitignore = if gitignore_path.exists() {
            let mut builder = ignore::gitignore::GitignoreBuilder::new(&root);
            // `add` returns Option<Error>; ignore parse errors silently.
            let _ = builder.add(&gitignore_path);
            builder.build().ok()
        } else {
            None
        };
        Self { root, gitignore }
    }

    /// Decide whether `abs_path` should sync. Cheap; expects
    /// `abs_path` to live under [`Self::root`] (it's tolerant if not).
    #[must_use]
    pub fn check(&self, abs_path: &Path) -> FilterDecision {
        // Compute path relative to root, when possible. Fall back to
        // the raw path for hardcoded-component matching if it isn't
        // under root.
        let rel = abs_path.strip_prefix(&self.root).unwrap_or(abs_path);

        // 1. Hardcoded matches (highest priority).
        if is_hardcoded_skip(rel) {
            return FilterDecision::Skip(SkipReason::Hardcoded);
        }

        // 2. Symlink check.
        if let Ok(meta) = fs::symlink_metadata(abs_path)
            && meta.file_type().is_symlink()
        {
            return FilterDecision::Skip(SkipReason::Symlink);
        }

        // 3. Gitignore — but never flag the .gitignore file itself.
        let is_gitignore_file = rel.file_name().and_then(|n| n.to_str()) == Some(".gitignore");
        if !is_gitignore_file && let Some(gi) = &self.gitignore {
            let is_dir = fs::metadata(abs_path).is_ok_and(|m| m.is_dir());
            if gi.matched_path_or_any_parents(abs_path, is_dir).is_ignore() {
                return FilterDecision::Skip(SkipReason::Gitignored);
            }
        }

        // 4. Size check.
        if let Ok(meta) = fs::metadata(abs_path)
            && meta.is_file()
        {
            let size = meta.len();
            if size > MAX_FILE_SIZE {
                return FilterDecision::Skip(SkipReason::TooLarge { size });
            }
        }

        FilterDecision::Include
    }
}

fn is_hardcoded_skip(rel: &Path) -> bool {
    for component in rel.components() {
        let os = component.as_os_str();
        let Some(s) = os.to_str() else { continue };
        if matches!(s, ".git" | "target" | "node_modules" | ".DS_Store") {
            return true;
        }
        // Editor and OS-temp files. Case-sensitive on purpose — these
        // match the lowercase forms vim/vi/etc. produce on Unix; we
        // don't want to over-skip files a user actually named e.g.
        // `Foo.SWP`.
        #[allow(clippy::case_sensitive_file_extension_comparisons)]
        if s.ends_with(".swp") || s.ends_with(".tmp") {
            return true;
        }
    }
    false
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use std::fs;
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    fn make_root() -> TempDir {
        TempDir::new().expect("tempdir")
    }

    fn write_file(root: &Path, rel: &str, contents: &[u8]) -> PathBuf {
        let p = root.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(&p).unwrap();
        f.write_all(contents).unwrap();
        p
    }

    #[test]
    fn includes_normal_file() {
        let dir = make_root();
        let p = write_file(dir.path(), "src/main.rs", b"hello");
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(filter.check(&p), FilterDecision::Include);
    }

    #[test]
    fn skips_dot_git() {
        let dir = make_root();
        let p = write_file(dir.path(), ".git/HEAD", b"ref: refs/heads/main\n");
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::Hardcoded)
        );
    }

    #[test]
    fn skips_target() {
        let dir = make_root();
        let p = write_file(dir.path(), "target/debug/foo", b"x");
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::Hardcoded)
        );
    }

    #[test]
    fn skips_node_modules() {
        let dir = make_root();
        let p = write_file(dir.path(), "node_modules/foo/index.js", b"x");
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::Hardcoded)
        );
    }

    #[test]
    fn skips_swp_and_tmp_and_ds_store() {
        let dir = make_root();
        let filter = WorkspaceFilter::new(dir.path());
        for rel in ["foo.swp", "foo.tmp", "sub/.DS_Store"] {
            let p = write_file(dir.path(), rel, b"x");
            assert_eq!(
                filter.check(&p),
                FilterDecision::Skip(SkipReason::Hardcoded),
                "{rel} should be hardcoded-skipped",
            );
        }
    }

    #[test]
    fn respects_gitignore() {
        let dir = make_root();
        write_file(dir.path(), ".gitignore", b"build/\n*.log\n");
        let p1 = write_file(dir.path(), "build/foo", b"x");
        let p2 = write_file(dir.path(), "app.log", b"x");
        let p3 = write_file(dir.path(), "src/main.rs", b"x");
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(
            filter.check(&p1),
            FilterDecision::Skip(SkipReason::Gitignored),
        );
        assert_eq!(
            filter.check(&p2),
            FilterDecision::Skip(SkipReason::Gitignored),
        );
        assert_eq!(filter.check(&p3), FilterDecision::Include);
    }

    #[test]
    fn gitignore_file_itself_is_synced() {
        let dir = make_root();
        let p = write_file(dir.path(), ".gitignore", b"*.log\n");
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(filter.check(&p), FilterDecision::Include);
    }

    #[test]
    fn rejects_oversize() {
        let dir = make_root();
        let big = vec![0u8; (MAX_FILE_SIZE as usize) + 1];
        let p = write_file(dir.path(), "big.bin", &big);
        let filter = WorkspaceFilter::new(dir.path());
        match filter.check(&p) {
            FilterDecision::Skip(SkipReason::TooLarge { size }) => {
                assert_eq!(size as usize, big.len());
            }
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[test]
    fn accepts_at_size_limit() {
        let dir = make_root();
        let exactly = vec![0u8; MAX_FILE_SIZE as usize];
        let p = write_file(dir.path(), "edge.bin", &exactly);
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(filter.check(&p), FilterDecision::Include);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink() {
        let dir = make_root();
        let target = write_file(dir.path(), "real.txt", b"x");
        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let filter = WorkspaceFilter::new(dir.path());
        assert_eq!(
            filter.check(&link),
            FilterDecision::Skip(SkipReason::Symlink)
        );
    }
}
