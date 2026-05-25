//! Per-path read/write rules attached to a workspace at
//! originate-time.
//!
//! Rules live on [`crate::WorkspaceConfig::rules`], ride the
//! `workspace.ticket` envelope to joiners
//! ([`crate::ticket::WorkspaceTicketEnvelope`]), and are stored on
//! [`crate::Workspace::rules`]. Enforcement (watcher / applier
//! consultation) lands in a follow-up slice; this module is the
//! data type and the wire-side validation only.
//!
//! Globs match against the **workspace-relative**, forward-slash,
//! NFC-normalised path shape — the same shape [`crate::keys::path_to_key`]
//! produces for doc keys. This keeps "what the rules match" identical
//! to "what the wire publishes", so a rule like `"docs/**"` written
//! by a consumer matches what a peer sees.

use std::path::Path;

use globset::Glob;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

/// Whether a path-class is writable by peers, or only readable.
///
/// [`Self::ReadWrite`] is the v1 default. [`Self::ReadOnly`] declares
/// the path-class is **not subject to peer-driven mutation** —
/// honoured by well-behaved peers; cryptographic enforcement is
/// deferred (ADR-001 capabilities).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode {
    /// Path-class can be written by both this peer and remote peers.
    ReadWrite,
    /// Path-class is read-only for remote peers; this peer must not
    /// publish changes outward, and incoming changes from peers are
    /// dropped on the floor.
    ReadOnly,
}

/// One rule: a glob matched against the workspace-relative path,
/// plus the [`Mode`] applied if it matches.
///
/// Globs are workspace-relative, forward-slash only, and matched
/// post-NFC-normalisation. See module doc for shape rationale.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRule {
    /// Glob pattern, e.g. `"docs/**"`, `"*.lock"`, `"src/**/*.rs"`.
    pub glob: String,
    /// Mode applied to paths matching `glob`.
    pub mode: Mode,
}

/// Ordered set of [`PathRule`]s plus a fall-through [`Mode`].
///
/// Rule order matters — **first match wins**. Consumers should write
/// rules most-specific first; subsequent rules that overlap an
/// earlier one are unreachable.
///
/// `PathRules` is bound at originate-time and travels with the
/// workspace.ticket. The host's rules are authoritative; a joiner's
/// `WorkspaceConfig::rules` is ignored on join (the host wins).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRules {
    /// Mode applied to paths matched by no rule.
    pub default: Mode,
    /// Ordered rule list; first match wins.
    pub rules: Vec<PathRule>,
}

impl PathRules {
    /// Default-permissive: every path is [`Mode::ReadWrite`], no rules.
    /// This is what an unrestricted workspace looks like, and what
    /// the joiner sees when a host attaches without configuring rules.
    #[must_use]
    pub const fn read_write() -> Self {
        Self {
            default: Mode::ReadWrite,
            rules: Vec::new(),
        }
    }

    /// Default-deny: every path is [`Mode::ReadOnly`], no rules. Use
    /// to publish a tree outward without accepting peer-driven edits.
    #[must_use]
    pub const fn read_only() -> Self {
        Self {
            default: Mode::ReadOnly,
            rules: Vec::new(),
        }
    }

    /// Resolve the [`Mode`] for `rel` (workspace-relative path).
    ///
    /// First-match-wins on `self.rules`; falls through to
    /// `self.default` if no rule matches. Paths that escape the
    /// workspace (parent traversal, absolute prefix, drive letter)
    /// match nothing — they fall through to `default` rather than
    /// silently inheriting a permissive rule. Defensive: callers
    /// should pass already-stripped relative paths.
    #[must_use]
    pub fn mode_for(&self, rel: &Path) -> Mode {
        let Some(hay) = path_to_match_string(rel) else {
            return self.default;
        };
        for rule in &self.rules {
            if glob_matches(&rule.glob, &hay) {
                return rule.mode;
            }
        }
        self.default
    }

    /// Reject malformed rule sets at originate-time and at decode-time.
    ///
    /// Run on the host before encoding the workspace.ticket envelope,
    /// and on the joiner after decoding it. Belt-and-braces — a corrupt
    /// host refuses to publish bad rules; a corrupt wire is rejected
    /// at decode.
    pub fn validate(&self) -> Result<(), PathRulesError> {
        for rule in &self.rules {
            validate_glob(&rule.glob)?;
        }
        Ok(())
    }
}

fn validate_glob(g: &str) -> Result<(), PathRulesError> {
    if g.is_empty() {
        return Err(PathRulesError::EmptyGlob);
    }
    if g.starts_with('/') || g.starts_with('\\') {
        return Err(PathRulesError::AbsoluteGlob(g.to_string()));
    }
    for seg in g.split('/') {
        if seg == ".." {
            return Err(PathRulesError::ParentTraversalGlob(g.to_string()));
        }
    }
    let normalised: String = g.nfc().collect();
    Glob::new(&normalised).map_err(|e| PathRulesError::InvalidGlob {
        glob: g.to_string(),
        reason: e.to_string(),
    })?;
    Ok(())
}

/// NFC + forward-slash + reject-traversal. Mirrors
/// [`crate::keys::path_to_key`]'s normalisation so glob shapes line
/// up with doc-key shapes.
fn path_to_match_string(rel: &Path) -> Option<String> {
    let mut parts = Vec::new();
    for component in rel.components() {
        match component {
            std::path::Component::Normal(part) => {
                let s = part.to_str()?;
                let normalised: String = s.nfc().collect();
                parts.push(normalised);
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => return None,
        }
    }
    Some(parts.join("/"))
}

fn glob_matches(glob_str: &str, hay: &str) -> bool {
    let normalised_glob: String = glob_str.nfc().collect();
    let normalised_hay: String = hay.nfc().collect();
    Glob::new(&normalised_glob).is_ok_and(|g| g.compile_matcher().is_match(&normalised_hay))
}

/// Why a [`PathRules`] failed validation.
#[derive(Debug, thiserror::Error)]
pub enum PathRulesError {
    /// Empty glob is meaningless — match nothing or match everything?
    /// Reject so consumers can't write order-dependent surprises.
    #[error("empty glob is not allowed")]
    EmptyGlob,
    /// Globs are workspace-relative; absolute prefixes (`/`, `\`)
    /// would never match the workspace-relative match string.
    #[error("absolute glob {0:?} is not allowed (rules are workspace-relative)")]
    AbsoluteGlob(String),
    /// Parent traversal can't reach above the workspace root through
    /// the rule layer — reject so consumers can't write rules that
    /// look like they reference a sibling tree.
    #[error("glob {0:?} contains parent traversal (..)")]
    ParentTraversalGlob(String),
    /// `globset::Glob::new` rejected the pattern (unbalanced bracket
    /// expression, etc.). The error string from globset is forwarded.
    #[error("invalid glob {glob:?}: {reason}")]
    InvalidGlob {
        /// The offending pattern.
        glob: String,
        /// `globset`'s error message.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn mode_for_no_rules_returns_default() {
        let r = PathRules::read_write();
        assert_eq!(r.mode_for(Path::new("a.txt")), Mode::ReadWrite);
        let r = PathRules::read_only();
        assert_eq!(r.mode_for(Path::new("a.txt")), Mode::ReadOnly);
    }

    #[test]
    fn mode_for_first_match_wins() {
        // Both rules match `docs/secret/foo.txt`. First wins.
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
                PathRule {
                    glob: "docs/secret/**".into(),
                    mode: Mode::ReadOnly,
                },
            ],
        };
        assert_eq!(
            r.mode_for(Path::new("docs/secret/foo.txt")),
            Mode::ReadWrite
        );
        // Reorder; now the more-specific rule wins.
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/secret/**".into(),
                    mode: Mode::ReadOnly,
                },
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
            ],
        };
        assert_eq!(
            r.mode_for(Path::new("docs/secret/foo.txt")),
            Mode::ReadOnly
        );
    }

    #[test]
    fn mode_for_no_match_falls_through_to_default() {
        let r = PathRules {
            default: Mode::ReadOnly,
            rules: vec![PathRule {
                glob: "docs/**".into(),
                mode: Mode::ReadWrite,
            }],
        };
        assert_eq!(r.mode_for(Path::new("src/main.rs")), Mode::ReadOnly);
    }

    #[test]
    fn mode_for_glob_matches_relative_path() {
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "*.lock".into(),
                    mode: Mode::ReadOnly,
                },
                PathRule {
                    glob: "src/**/*.rs".into(),
                    mode: Mode::ReadWrite,
                },
            ],
        };
        assert_eq!(r.mode_for(Path::new("Cargo.lock")), Mode::ReadOnly);
        assert_eq!(r.mode_for(Path::new("src/lib.rs")), Mode::ReadWrite);
        assert_eq!(r.mode_for(Path::new("src/a/b/c.rs")), Mode::ReadWrite);
    }

    #[test]
    fn mode_for_glob_does_not_match_paths_outside_workspace() {
        // Paths with parent traversal must fall through to default
        // rather than silently matching a glob.
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![PathRule {
                glob: "etc/**".into(),
                mode: Mode::ReadOnly,
            }],
        };
        assert_eq!(
            r.mode_for(Path::new("../etc/passwd")),
            Mode::ReadWrite,
            "parent-traversal path should not match glob",
        );
        assert_eq!(
            r.mode_for(Path::new("/etc/passwd")),
            Mode::ReadWrite,
            "absolute path should not match relative glob",
        );
    }

    #[test]
    fn mode_for_unicode_path_normalisation_consistent() {
        // "café" composed (NFC) and decomposed (NFD) must hit the same
        // rule. Mirrors the keys.rs NFC contract.
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![PathRule {
                glob: "caf\u{00e9}/**".into(),
                mode: Mode::ReadOnly,
            }],
        };
        let composed = PathBuf::from("caf\u{00e9}").join("x.txt");
        let decomposed = PathBuf::from("cafe\u{0301}").join("x.txt");
        assert_eq!(r.mode_for(&composed), Mode::ReadOnly);
        assert_eq!(r.mode_for(&decomposed), Mode::ReadOnly);
    }

    #[test]
    fn validate_rejects_empty_glob() {
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![PathRule {
                glob: String::new(),
                mode: Mode::ReadOnly,
            }],
        };
        assert!(matches!(r.validate(), Err(PathRulesError::EmptyGlob)));
    }

    #[test]
    fn validate_rejects_absolute_glob() {
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![PathRule {
                glob: "/etc/**".into(),
                mode: Mode::ReadOnly,
            }],
        };
        assert!(matches!(
            r.validate(),
            Err(PathRulesError::AbsoluteGlob(_))
        ));
    }

    #[test]
    fn validate_rejects_parent_traversal_glob() {
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![PathRule {
                glob: "../etc/**".into(),
                mode: Mode::ReadOnly,
            }],
        };
        assert!(matches!(
            r.validate(),
            Err(PathRulesError::ParentTraversalGlob(_))
        ));
    }

    #[test]
    fn validate_accepts_typical_globs() {
        let r = PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadOnly,
                },
                PathRule {
                    glob: "*.lock".into(),
                    mode: Mode::ReadOnly,
                },
                PathRule {
                    glob: "src/**/*.rs".into(),
                    mode: Mode::ReadWrite,
                },
            ],
        };
        assert!(r.validate().is_ok());
    }

    #[test]
    fn path_rules_round_trip_postcard() {
        let r = PathRules {
            default: Mode::ReadOnly,
            rules: vec![
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
                PathRule {
                    glob: "*.lock".into(),
                    mode: Mode::ReadOnly,
                },
            ],
        };
        let bytes = postcard::to_allocvec(&r).expect("encode");
        let back: PathRules = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(r, back);
    }
}
