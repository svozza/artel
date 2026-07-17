//! Decide whether a path should be included in a workspace sync.
//!
//! Four layers, in order:
//!
//! 1. Hardcoded skips (`.git`, `target`, `node_modules`, `.DS_Store`,
//!    `.artel-fs` — the workspace's own state dir — plus `*.swp`,
//!    `*.tmp`). Highest priority — these never sync regardless of
//!    configuration. They are the substrate's *self*-protection
//!    (its own state dir, VCS/build event storms, editor churn), not
//!    consumer policy.
//! 2. Symlink check. We deliberately do not follow links.
//! 3. Exclude rules ([`ExcludeRules`]). Consumer-owned: defaults to
//!    skipping hidden (dot-prefixed) entries; an explicit glob list
//!    from [`crate::WorkspaceConfig::exclude`] replaces the default
//!    entirely. Surfaced as `FilterDecision::Skip(SkipReason::Excluded)`
//!    so callers emit an observable event — an excluded path must
//!    never vanish silently.
//! 4. Size cap (1 MiB). Larger files are surfaced to consumers as
//!    `FilterDecision::Skip(SkipReason::TooLarge)` so they can decide
//!    whether to log/notify.
//!
//! There is deliberately **no** `.gitignore` layer. Honouring it made
//! the substrate interpret another tool's policy file, and because the
//! `.gitignore` file itself synced, a host's ignore rules propagated to
//! every joiner and then filtered the same paths everywhere — the
//! silent-sync-death bug this module's redesign fixed. What is
//! project-relevant (and what is secret) is consumer policy: embedders
//! that want gitignore semantics should read it themselves and pass
//! equivalent globs via [`crate::WorkspaceConfig::exclude`].

use std::fs;
use std::path::{Path, PathBuf};

use globset::{Glob, GlobSet, GlobSetBuilder};
use unicode_normalization::UnicodeNormalization;

use crate::rules::{PathRulesError, path_to_match_string, validate_glob};

/// Files larger than this are skipped.
///
/// Whole-file reads and publishes (no streaming, no chunked diffing)
/// keep the pipeline simple, and a cap keeps each of those operations
/// cheap; revisit when large-file sync becomes a real need.
pub const MAX_FILE_SIZE: u64 = 1 << 20;

/// Compiled, consumer-owned exclusion rules — filter layer 3.
///
/// Built from [`crate::WorkspaceConfig::exclude`] at workspace
/// construction:
///
/// - `None` → [`Self::Dotfiles`]: any path with a hidden
///   (dot-prefixed) component is skipped. A filesystem convention,
///   not a policy interpretation — it requires parsing nothing and is
///   predictable from the filename alone.
/// - `Some(globs)` → [`Self::Globs`]: exactly that list, **replace
///   not merge**. `Some(vec![])` compiles to a set matching nothing —
///   everything syncs, dotfiles included.
///
/// Local to each node (never ticket-borne): the exclude list is this
/// node's hygiene, and nothing about it rides the synced tree.
#[derive(Debug, Clone, Default)]
pub enum ExcludeRules {
    /// Skip any path containing a hidden (dot-prefixed) component.
    #[default]
    Dotfiles,
    /// Skip paths matching an explicit glob set. Globs use the same
    /// workspace-relative, forward-slash, NFC-normalised shape as
    /// [`crate::PathRules`] globs.
    Globs(GlobSet),
}

impl ExcludeRules {
    /// Compile the [`crate::WorkspaceConfig::exclude`] value.
    ///
    /// `None` yields the dotfile default; `Some(globs)` validates and
    /// compiles the explicit list (replace-not-merge — see type docs).
    ///
    /// # Errors
    ///
    /// Returns the same [`PathRulesError`] variants as
    /// [`crate::PathRules::validate`] for a malformed glob: empty,
    /// absolute, parent traversal, or a pattern `globset` won't
    /// compile.
    pub fn compile(exclude: Option<&[String]>) -> Result<Self, PathRulesError> {
        let Some(globs) = exclude else {
            return Ok(Self::Dotfiles);
        };
        let mut builder = GlobSetBuilder::new();
        for g in globs {
            validate_glob(g)?;
            let normalised: String = g.nfc().collect();
            let glob = Glob::new(&normalised).map_err(|e| PathRulesError::InvalidGlob {
                glob: g.clone(),
                reason: e.to_string(),
            })?;
            builder.add(glob);
        }
        let set = builder.build().map_err(|e| PathRulesError::InvalidGlob {
            glob: String::new(),
            reason: e.to_string(),
        })?;
        Ok(Self::Globs(set))
    }

    /// Does `rel` (workspace-relative path) match these rules?
    ///
    /// Paths that escape the workspace (parent traversal, absolute
    /// prefix) match nothing under [`Self::Globs`] — defensive
    /// fall-through, mirroring [`crate::PathRules`]. Under
    /// [`Self::Dotfiles`] every `Normal` component is checked
    /// regardless.
    #[must_use]
    pub fn matches(&self, rel: &Path) -> bool {
        match self {
            Self::Dotfiles => rel.components().any(|component| {
                matches!(component, std::path::Component::Normal(part)
                    if part.to_str().is_some_and(|s| s.starts_with('.')))
            }),
            Self::Globs(set) => {
                path_to_match_string(rel).is_some_and(|hay| set.is_match(hay.as_str()))
            }
        }
    }
}

/// Pre-parsed view of the workspace root + its exclude rules.
///
/// Cheap to construct (the exclude rules are compiled once at
/// workspace construction and cloned in). Kept around so the watcher
/// and applier can call [`Self::check`] without recompiling per event.
#[derive(Debug)]
pub struct WorkspaceFilter {
    pub root: PathBuf,
    exclude: ExcludeRules,
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
    /// Matched the workspace's [`ExcludeRules`] (the dotfile default
    /// or an explicit [`crate::WorkspaceConfig::exclude`] list).
    /// Callers must surface this via
    /// [`crate::WorkspaceEvent::SkippedExcluded`] — never silently.
    Excluded,
    /// Path is a symlink. We never follow them.
    Symlink,
    /// File exceeded [`MAX_FILE_SIZE`].
    TooLarge {
        /// Actual size in bytes; surfaces to consumers so they can log.
        size: u64,
    },
}

impl WorkspaceFilter {
    /// Build a filter for `root` with the given exclude rules.
    #[must_use]
    pub fn new(root: &Path, exclude: ExcludeRules) -> Self {
        Self {
            root: root.to_path_buf(),
            exclude,
        }
    }

    /// Decide whether `abs_path` should sync. Cheap; expects
    /// `abs_path` to live under [`Self::root`] (it's tolerant if not).
    #[must_use]
    pub fn check(&self, abs_path: &Path) -> FilterDecision {
        // Compute path relative to root, when possible. Fall back to
        // the raw path for component matching if it isn't under root.
        let rel = abs_path.strip_prefix(&self.root).unwrap_or(abs_path);

        // 1. Hardcoded matches (highest priority).
        if Self::is_hardcoded_skip(rel) {
            return FilterDecision::Skip(SkipReason::Hardcoded);
        }

        // 2. Symlink check.
        if let Ok(meta) = fs::symlink_metadata(abs_path)
            && meta.file_type().is_symlink()
        {
            return FilterDecision::Skip(SkipReason::Symlink);
        }

        // 3. Exclude rules (consumer-owned; dotfiles by default).
        if self.exclude.matches(rel) {
            return FilterDecision::Skip(SkipReason::Excluded);
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

    /// Single source of truth for the hardcoded-skip predicate.
    ///
    /// Used by [`Self::check`] (the watcher / applier filter) and by
    /// [`crate::workspace::AttachPolicy::RequireEmpty`]'s emptiness
    /// check, so the two never drift on what counts as "filesystem
    /// noise we should ignore" (`.git/`, `target/`, `*.swp`, etc.).
    ///
    /// Accepts any path: relative, absolute, top-level entry, deep
    /// nested. Walks every component looking for a match.
    #[must_use]
    pub fn is_hardcoded_skip(rel: &Path) -> bool {
        for component in rel.components() {
            let os = component.as_os_str();
            let Some(s) = os.to_str() else { continue };
            if matches!(
                s,
                ".git" | "target" | "node_modules" | ".DS_Store" | ".artel-fs"
            ) {
                return true;
            }
            // Editor and OS-temp files. Case-sensitive on purpose —
            // these match the lowercase forms vim/vi/etc. produce on
            // Unix; we don't want to over-skip files a user actually
            // named e.g. `Foo.SWP`.
            #[allow(clippy::case_sensitive_file_extension_comparisons)]
            if s.ends_with(".swp") || s.ends_with(".tmp") {
                return true;
            }
        }
        false
    }
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

    /// Filter with the default (dotfiles) exclude — the common case.
    fn default_filter(root: &Path) -> WorkspaceFilter {
        WorkspaceFilter::new(root, ExcludeRules::default())
    }

    /// Filter that excludes nothing (`Some(vec![])` in config terms).
    fn sync_everything_filter(root: &Path) -> WorkspaceFilter {
        WorkspaceFilter::new(root, ExcludeRules::compile(Some(&[])).unwrap())
    }

    #[test]
    fn includes_normal_file() {
        let dir = make_root();
        let p = write_file(dir.path(), "src/main.rs", b"hello");
        let filter = default_filter(dir.path());
        assert_eq!(filter.check(&p), FilterDecision::Include);
    }

    #[test]
    fn skips_dot_git() {
        let dir = make_root();
        let p = write_file(dir.path(), ".git/HEAD", b"ref: refs/heads/main\n");
        let filter = default_filter(dir.path());
        assert_eq!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::Hardcoded)
        );
    }

    #[test]
    fn skips_target() {
        let dir = make_root();
        let p = write_file(dir.path(), "target/debug/foo", b"x");
        let filter = default_filter(dir.path());
        assert_eq!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::Hardcoded)
        );
    }

    #[test]
    fn skips_node_modules() {
        let dir = make_root();
        let p = write_file(dir.path(), "node_modules/foo/index.js", b"x");
        let filter = default_filter(dir.path());
        assert_eq!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::Hardcoded)
        );
    }

    #[test]
    fn skips_swp_and_tmp_and_ds_store() {
        let dir = make_root();
        let filter = default_filter(dir.path());
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
    fn skips_artel_fs_state_dir() {
        // The workspace's own state dir lives under `<root>/.artel-fs`
        // by default; without this skip, the watcher would loop on
        // its own redb / blobs writes.
        let dir = make_root();
        let filter = default_filter(dir.path());
        for rel in [
            ".artel-fs/iroh.key",
            ".artel-fs/doc-id",
            ".artel-fs/docs/docs.redb",
            ".artel-fs/blobs/blobs.db",
        ] {
            let p = write_file(dir.path(), rel, b"x");
            assert_eq!(
                filter.check(&p),
                FilterDecision::Skip(SkipReason::Hardcoded),
                "{rel} should be hardcoded-skipped",
            );
        }
    }

    #[test]
    fn default_excludes_dotfiles_at_any_depth() {
        let dir = make_root();
        let filter = default_filter(dir.path());
        for rel in [
            ".env",
            ".harness/log/peer.jsonl",
            "sub/.hidden",
            "sub/.state/db.sqlite",
        ] {
            let p = write_file(dir.path(), rel, b"x");
            assert_eq!(
                filter.check(&p),
                FilterDecision::Skip(SkipReason::Excluded),
                "{rel} should be excluded by the dotfile default",
            );
        }
        // Non-hidden neighbours still sync.
        let p = write_file(dir.path(), "sub/visible.txt", b"x");
        assert_eq!(filter.check(&p), FilterDecision::Include);
    }

    #[test]
    fn empty_exclude_list_syncs_dotfiles() {
        // `Some(vec![])` = replace the default with nothing: sync
        // everything, dotfiles included.
        let dir = make_root();
        let filter = sync_everything_filter(dir.path());
        for rel in [".env", ".harness/log/peer.jsonl", "sub/.hidden"] {
            let p = write_file(dir.path(), rel, b"x");
            assert_eq!(
                filter.check(&p),
                FilterDecision::Include,
                "{rel} should sync under an empty exclude list",
            );
        }
    }

    #[test]
    fn custom_exclude_replaces_default_not_merges() {
        // An explicit list is exactly that list: `*.secret` is
        // excluded, but dotfiles (the replaced default) now sync.
        let dir = make_root();
        let exclude =
            ExcludeRules::compile(Some(&["**/*.secret".to_string(), "*.secret".to_string()]))
                .unwrap();
        let filter = WorkspaceFilter::new(dir.path(), exclude);

        let p = write_file(dir.path(), "api.secret", b"x");
        assert_eq!(filter.check(&p), FilterDecision::Skip(SkipReason::Excluded));
        let p = write_file(dir.path(), "sub/deep.secret", b"x");
        assert_eq!(filter.check(&p), FilterDecision::Skip(SkipReason::Excluded));

        let p = write_file(dir.path(), ".env", b"x");
        assert_eq!(
            filter.check(&p),
            FilterDecision::Include,
            "dotfiles sync once the default list is replaced",
        );
    }

    #[test]
    fn exclude_does_not_override_hardcoded_skips() {
        // Even a sync-everything exclude can't resurrect the
        // substrate's self-protection skips.
        let dir = make_root();
        let filter = sync_everything_filter(dir.path());
        for rel in [".git/HEAD", ".artel-fs/iroh.key", "target/debug/foo"] {
            let p = write_file(dir.path(), rel, b"x");
            assert_eq!(
                filter.check(&p),
                FilterDecision::Skip(SkipReason::Hardcoded),
                "{rel} must stay hardcoded-skipped regardless of exclude config",
            );
        }
    }

    #[test]
    fn exclude_does_not_override_size_cap() {
        let dir = make_root();
        let big = vec![0u8; (MAX_FILE_SIZE as usize) + 1];
        let p = write_file(dir.path(), "big.bin", &big);
        let filter = sync_everything_filter(dir.path());
        assert!(matches!(
            filter.check(&p),
            FilterDecision::Skip(SkipReason::TooLarge { .. })
        ));
    }

    #[test]
    fn gitignore_is_not_consulted_and_is_itself_just_a_dotfile() {
        // Regression test for the silent-sync-death bug: a
        // `.gitignore` listing a path must NOT stop that path from
        // syncing. And the `.gitignore` file itself gets no special
        // treatment any more — it's a dotfile, excluded by the
        // default, synced under an explicit empty list.
        let dir = make_root();
        write_file(dir.path(), ".gitignore", b"build/\n*.log\n.state/\n");
        let ignored_by_git = [
            write_file(dir.path(), "build/foo", b"x"),
            write_file(dir.path(), "app.log", b"x"),
            write_file(dir.path(), "state-adjacent.log", b"x"),
        ];

        let filter = default_filter(dir.path());
        for p in &ignored_by_git {
            assert_eq!(
                filter.check(p),
                FilterDecision::Include,
                "{} is gitignored but must sync anyway",
                p.display(),
            );
        }
        assert_eq!(
            filter.check(&dir.path().join(".gitignore")),
            FilterDecision::Skip(SkipReason::Excluded),
            ".gitignore is an ordinary dotfile under the default",
        );

        let filter = sync_everything_filter(dir.path());
        assert_eq!(
            filter.check(&dir.path().join(".gitignore")),
            FilterDecision::Include,
            ".gitignore syncs like any other file under an empty exclude list",
        );
    }

    #[test]
    fn compile_rejects_malformed_globs() {
        for bad in ["", "/abs/**", "../up/**"] {
            assert!(
                ExcludeRules::compile(Some(&[bad.to_string()])).is_err(),
                "{bad:?} should be rejected",
            );
        }
    }

    #[test]
    fn glob_exclude_ignores_paths_outside_workspace() {
        // Parent-traversal / absolute paths match no glob — defensive
        // fall-through mirroring PathRules.
        let exclude = ExcludeRules::compile(Some(&["etc/**".to_string()])).unwrap();
        assert!(!exclude.matches(Path::new("../etc/passwd")));
        assert!(!exclude.matches(Path::new("/etc/passwd")));
        assert!(exclude.matches(Path::new("etc/passwd")));
    }

    #[test]
    fn rejects_oversize() {
        let dir = make_root();
        let big = vec![0u8; (MAX_FILE_SIZE as usize) + 1];
        let p = write_file(dir.path(), "big.bin", &big);
        let filter = default_filter(dir.path());
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
        let filter = default_filter(dir.path());
        assert_eq!(filter.check(&p), FilterDecision::Include);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink() {
        let dir = make_root();
        let target = write_file(dir.path(), "real.txt", b"x");
        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let filter = default_filter(dir.path());
        assert_eq!(
            filter.check(&link),
            FilterDecision::Skip(SkipReason::Symlink)
        );
    }
}
