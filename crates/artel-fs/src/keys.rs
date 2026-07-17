//! Path ↔ doc-key translation.
//!
//! Files in the workspace root map to entries in an iroh-docs document
//! under a fixed prefix. The translation is stable, NFC-normalised, and
//! refuses anything that would let a peer escape the workspace root
//! (parent traversal, absolute paths, drive letters).

use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};

use unicode_normalization::UnicodeNormalization;

use crate::error::WorkspaceError;

/// Doc-key prefix every workspace file lives under.
///
/// All keys are `path/<utf8 forward-slash relative path>`. The prefix
/// reserves the rest of the doc namespace for future non-file metadata
/// without a migration.
pub const KEY_PREFIX: &str = "path/";

/// Convert an absolute path inside `root` into a doc key.
///
/// # Errors
///
/// Returns [`WorkspaceError::InvalidPath`] if the path is not under
/// `root`, contains a parent-directory component, has non-UTF-8
/// components, or somehow ended up absolute after stripping `root`.
pub fn path_to_key(root: &Path, abs_path: &Path) -> Result<Vec<u8>, WorkspaceError> {
    let rel = abs_path.strip_prefix(root).map_err(|_| {
        WorkspaceError::InvalidPath(format!(
            "path {} is not inside root {}",
            abs_path.display(),
            root.display()
        ))
    })?;

    let mut parts: Vec<String> = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => {
                let s = part.to_str().ok_or_else(|| {
                    WorkspaceError::InvalidPath(format!(
                        "non-utf8 path component in {}",
                        abs_path.display()
                    ))
                })?;
                let normalised: String = s.nfc().collect();
                parts.push(normalised);
            }
            Component::CurDir => {
                // Skip "." — `Path::strip_prefix` can leave them in.
            }
            Component::ParentDir => {
                return Err(WorkspaceError::InvalidPath(format!(
                    "path {} contains parent traversal",
                    abs_path.display()
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspaceError::InvalidPath(format!(
                    "path {} is not relative after stripping root",
                    abs_path.display()
                )));
            }
        }
    }

    let joined = parts.join("/");
    let mut out = Vec::with_capacity(KEY_PREFIX.len() + joined.len());
    out.extend_from_slice(KEY_PREFIX.as_bytes());
    out.extend_from_slice(joined.as_bytes());
    Ok(out)
}

/// Convert a doc key back into an absolute path inside `root`.
///
/// # Errors
///
/// Returns [`WorkspaceError::InvalidPath`] if `key` does not start with
/// [`KEY_PREFIX`], is not valid UTF-8, would escape `root` via parent
/// traversal, or represents an absolute path / Windows drive prefix.
pub fn key_to_path(root: &Path, key: &[u8]) -> Result<PathBuf, WorkspaceError> {
    let prefix_bytes = KEY_PREFIX.as_bytes();
    if !key.starts_with(prefix_bytes) {
        return Err(WorkspaceError::InvalidPath(format!(
            "key does not start with {KEY_PREFIX:?}"
        )));
    }

    let rel_bytes = &key[prefix_bytes.len()..];
    let rel_str = std::str::from_utf8(rel_bytes)
        .map_err(|e| WorkspaceError::InvalidPath(format!("key is not valid utf-8: {e}")))?;

    if rel_str.starts_with('/') || rel_str.starts_with('\\') {
        return Err(WorkspaceError::InvalidPath(format!(
            "key relative portion {rel_str:?} is absolute"
        )));
    }

    if has_windows_drive_prefix(rel_str) {
        return Err(WorkspaceError::InvalidPath(format!(
            "key relative portion {rel_str:?} is absolute"
        )));
    }

    for segment in rel_str.split('/') {
        if segment == ".." {
            return Err(WorkspaceError::InvalidPath(format!(
                "key {rel_str:?} contains parent traversal"
            )));
        }
    }

    // Walk the constructed PathBuf too, in case a backslash on a
    // hypothetical non-Unix platform creeps in. We're Unix-only today,
    // but keeping the check costs nothing and matches the reference
    // implementation.
    let candidate = PathBuf::from(rel_str);
    let mut normal_components = 0usize;
    for component in candidate.components() {
        match component {
            Component::ParentDir => {
                return Err(WorkspaceError::InvalidPath(format!(
                    "key {rel_str:?} contains parent traversal"
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspaceError::InvalidPath(format!(
                    "key relative portion {rel_str:?} is absolute"
                )));
            }
            Component::Normal(_) => normal_components += 1,
            Component::CurDir => {}
        }
    }

    // A key must name an actual entry UNDER the root. `path/` and
    // `path/.` resolve to the root itself; downstream that path's
    // "sibling temp file" (`apply_entry_streaming`) would land
    // OUTSIDE the workspace — a peer-controlled key must never
    // produce a path we'd touch outside root, so fail closed here.
    if normal_components == 0 {
        return Err(WorkspaceError::InvalidPath(format!(
            "key relative portion {rel_str:?} names no path under the root"
        )));
    }

    let native: String = if MAIN_SEPARATOR == '/' {
        rel_str.to_string()
    } else {
        rel_str.replace('/', std::path::MAIN_SEPARATOR_STR)
    };

    Ok(root.join(native))
}

fn has_windows_drive_prefix(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    matches!(chars.next(), Some(':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\workspace")
        } else {
            PathBuf::from("/workspace")
        }
    }

    #[test]
    fn round_trip_simple_path() {
        let r = root();
        let p = r.join("src").join("main.rs");
        let key = path_to_key(&r, &p).unwrap();
        assert_eq!(key, b"path/src/main.rs");
        let back = key_to_path(&r, &key).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn round_trip_root_level_file() {
        let r = root();
        let p = r.join("README.md");
        let key = path_to_key(&r, &p).unwrap();
        assert_eq!(key, b"path/README.md");
        let back = key_to_path(&r, &key).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn round_trip_nested_path() {
        let r = root();
        let p = r.join("a").join("b").join("c").join("d.txt");
        let key = path_to_key(&r, &p).unwrap();
        assert_eq!(key, b"path/a/b/c/d.txt");
        let back = key_to_path(&r, &key).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn rejects_path_outside_root() {
        let r = root();
        let outside = if cfg!(windows) {
            PathBuf::from(r"C:\elsewhere\foo.txt")
        } else {
            PathBuf::from("/elsewhere/foo.txt")
        };
        assert!(path_to_key(&r, &outside).is_err());
    }

    #[test]
    fn rejects_parent_traversal_in_key() {
        let r = root();
        let key = b"path/../etc/passwd";
        assert!(key_to_path(&r, key).is_err());
    }

    #[test]
    fn rejects_key_without_prefix() {
        let r = root();
        assert!(key_to_path(&r, b"src/main.rs").is_err());
    }

    #[test]
    fn rejects_absolute_key() {
        let r = root();
        let key = if cfg!(windows) {
            b"path/C:/etc/passwd".to_vec()
        } else {
            b"path//etc/passwd".to_vec()
        };
        assert!(key_to_path(&r, &key).is_err());
    }

    #[test]
    fn nfc_normalises_components() {
        // "café" composed (NFC, 4 chars) vs decomposed (NFD, 5 chars)
        // should produce the same key — otherwise macOS-vs-Linux
        // workspaces drift apart silently.
        let r = root();
        let composed = r.join("caf\u{00e9}").join("x.txt");
        let decomposed = r.join("cafe\u{0301}").join("x.txt");
        let k1 = path_to_key(&r, &composed).unwrap();
        let k2 = path_to_key(&r, &decomposed).unwrap();
        assert_eq!(
            k1, k2,
            "NFC normalisation should make composed and decomposed forms equal",
        );
    }

    #[test]
    fn rejects_non_utf8_key() {
        let r = root();
        let key = b"path/\xff\xfe".to_vec();
        assert!(key_to_path(&r, &key).is_err());
    }

    #[test]
    fn drive_letter_is_rejected() {
        let r = root();
        assert!(key_to_path(&r, b"path/D:/leak").is_err());
    }

    #[test]
    fn rejects_keys_that_resolve_to_the_root_itself() {
        // `path/` and `path/.` map to the workspace root. Applying
        // such an entry would compute the streaming temp file as a
        // *sibling of the root* — outside the workspace — so these
        // keys must be rejected outright (adversarial-review finding,
        // issue #33 streaming slice).
        let r = root();
        for key in [b"path/".as_slice(), b"path/.", b"path/./."] {
            assert!(
                key_to_path(&r, key).is_err(),
                "{:?} must be rejected — it names no entry under the root",
                String::from_utf8_lossy(key),
            );
        }
    }
}
