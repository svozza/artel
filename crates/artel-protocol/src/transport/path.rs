//! Default filesystem paths for the artel daemon.
//!
//! The daemon and clients agree on a per-user state directory under the
//! invoking user's home, containing the IPC socket and the daemon's
//! bookkeeping files (PID, log, sessions). This module computes those
//! paths; it does not create or open them.
//!
//! Paths can be overridden via the [`ARTEL_HOME_ENV`] environment
//! variable, which is useful for tests and for users who don't want to
//! pollute `$HOME`. When set, it is used as the state directory directly
//! (i.e. the socket is at `$ARTEL_HOME/daemon.sock`).

use std::env;
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;

/// Environment variable that overrides the default state directory.
///
/// When set to a non-empty path, this is used in place of `~/.artel`.
pub const ARTEL_HOME_ENV: &str = "ARTEL_HOME";

/// Name of the per-user state directory beneath `$HOME`.
pub const STATE_DIR_NAME: &str = ".artel";

/// File name of the daemon's IPC socket within the state directory.
pub const SOCKET_FILE: &str = "daemon.sock";

/// File name of the daemon's PID file within the state directory.
pub const PID_FILE: &str = "daemon.pid";

/// Compute the per-user artel state directory using the process
/// environment.
///
/// Resolution order:
///
/// 1. If [`ARTEL_HOME_ENV`] is set to a non-empty value, that path is
///    used directly.
/// 2. Otherwise, `$HOME/.artel`.
///
/// Returns [`io::ErrorKind::NotFound`] if neither is available.
pub fn default_dir() -> io::Result<PathBuf> {
    resolve_dir(&ProcessEnv)
}

/// Compute the default IPC socket path: [`default_dir`] joined with
/// [`SOCKET_FILE`].
pub fn default_socket_path() -> io::Result<PathBuf> {
    Ok(default_dir()?.join(SOCKET_FILE))
}

/// Compute the default PID file path: [`default_dir`] joined with
/// [`PID_FILE`].
pub fn default_pid_path() -> io::Result<PathBuf> {
    Ok(default_dir()?.join(PID_FILE))
}

/// Source of the env vars [`resolve_dir`] consults.
///
/// Production code uses [`ProcessEnv`]; tests use a struct that returns
/// fixed values, so they don't have to mutate process state.
pub(crate) trait EnvSource {
    fn lookup(&self, key: &str) -> Option<OsString>;
}

/// Reads from the actual process environment via [`std::env::var_os`].
pub(crate) struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn lookup(&self, key: &str) -> Option<OsString> {
        env::var_os(key)
    }
}

pub(crate) fn resolve_dir(env: &dyn EnvSource) -> io::Result<PathBuf> {
    if let Some(over) = env.lookup(ARTEL_HOME_ENV).filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(over));
    }
    let home = env
        .lookup("HOME")
        .filter(|v: &OsString| !v.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("neither {ARTEL_HOME_ENV} nor HOME is set"),
            )
        })?;
    Ok(PathBuf::from(home).join(STATE_DIR_NAME))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::OsStr;

    use pretty_assertions::assert_eq;

    use super::*;

    /// Test [`EnvSource`] backed by a fixed map.
    struct FakeEnv(HashMap<String, OsString>);

    impl FakeEnv {
        fn new() -> Self {
            Self(HashMap::new())
        }

        fn with(mut self, key: &str, value: impl AsRef<OsStr>) -> Self {
            self.0.insert(key.to_owned(), value.as_ref().to_os_string());
            self
        }
    }

    impl EnvSource for FakeEnv {
        fn lookup(&self, key: &str) -> Option<OsString> {
            self.0.get(key).cloned()
        }
    }

    #[test]
    fn artel_home_takes_precedence_over_home() {
        let env = FakeEnv::new()
            .with(ARTEL_HOME_ENV, "/tmp/artel-test-override")
            .with("HOME", "/home/should-be-ignored");
        let dir = resolve_dir(&env).unwrap();
        assert_eq!(dir, PathBuf::from("/tmp/artel-test-override"));
    }

    #[test]
    fn falls_back_to_home_dot_artel() {
        let env = FakeEnv::new().with("HOME", "/home/alice");
        let dir = resolve_dir(&env).unwrap();
        assert_eq!(dir, PathBuf::from("/home/alice/.artel"));
    }

    #[test]
    fn empty_artel_home_is_ignored() {
        let env = FakeEnv::new()
            .with(ARTEL_HOME_ENV, "")
            .with("HOME", "/home/alice");
        let dir = resolve_dir(&env).unwrap();
        assert_eq!(dir, PathBuf::from("/home/alice/.artel"));
    }

    #[test]
    fn empty_home_is_ignored() {
        let env = FakeEnv::new().with("HOME", "");
        let err = resolve_dir(&env).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn errors_when_nothing_is_set() {
        let env = FakeEnv::new();
        let err = resolve_dir(&env).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn socket_and_pid_paths_join_correctly() {
        let env = FakeEnv::new().with(ARTEL_HOME_ENV, "/tmp/artel-ut");
        let dir = resolve_dir(&env).unwrap();
        assert_eq!(
            dir.join(SOCKET_FILE),
            PathBuf::from("/tmp/artel-ut/daemon.sock")
        );
        assert_eq!(
            dir.join(PID_FILE),
            PathBuf::from("/tmp/artel-ut/daemon.pid")
        );
    }
}
