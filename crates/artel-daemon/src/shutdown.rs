//! Cooperative cancellation triggered by SIGINT / SIGTERM.
//!
//! [`Shutdown`] is the source of truth, owned by the top-level daemon
//! task. [`ShutdownToken`] is the cheap clonable handle that connection
//! tasks, the accept loop, and any other long-running work hold; they
//! await [`ShutdownToken::cancelled`] alongside their own work via
//! `tokio::select!`.

use std::sync::Arc;

use tokio::sync::watch;

/// Source of the cancellation signal. One per daemon process.
#[derive(Debug)]
pub struct Shutdown {
    tx: watch::Sender<bool>,
}

/// Cheap handle to the cancellation signal. Cloneable, lightweight.
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    rx: watch::Receiver<bool>,
}

impl Shutdown {
    /// Create a new shutdown signal. Initially un-cancelled.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx }
    }

    /// Issue a cheap clonable token.
    #[must_use]
    pub fn token(&self) -> ShutdownToken {
        ShutdownToken {
            rx: self.tx.subscribe(),
        }
    }

    /// Trigger cancellation. Idempotent.
    pub fn trigger(&self) {
        // send_replace ignores the case where there are no receivers.
        let _ = self.tx.send_replace(true);
    }

    /// Whether shutdown has already been triggered.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.tx.borrow()
    }

    /// Spawn a task that waits for SIGINT or SIGTERM and triggers
    /// shutdown when either fires. The returned [`Arc<Shutdown>`] is the
    /// same handle the caller passes in; it is cloned into the spawned
    /// task so the signal listener outlives nothing in particular.
    ///
    /// Returns an `io::Error` if either signal stream fails to install
    /// (extremely unlikely on Unix; would mean the process has no signal
    /// handling at all).
    pub fn install_signal_handlers(self: &Arc<Self>) -> std::io::Result<()> {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let me = Arc::clone(self);
        tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("received SIGINT, shutting down");
                }
                _ = sigterm.recv() => {
                    tracing::info!("received SIGTERM, shutting down");
                }
            }
            me.trigger();
        });
        Ok(())
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownToken {
    /// Resolve when shutdown is triggered. If already triggered,
    /// resolves immediately.
    pub async fn cancelled(&mut self) {
        // If already triggered, return immediately without waiting on
        // the watch channel.
        if *self.rx.borrow() {
            return;
        }
        // Wait for the next change. `changed` returns Err only when the
        // sender is dropped, which we treat as "shut down" too — there's
        // no source of cancellation left.
        let _ = self.rx.changed().await;
    }

    /// Snapshot of the current state. Cheap.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn token_resolves_after_trigger() {
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        assert!(!token.is_triggered());
        shutdown.trigger();
        // Generous timeout; resolves immediately in practice.
        timeout(Duration::from_millis(100), token.cancelled())
            .await
            .expect("token should resolve");
        assert!(token.is_triggered());
    }

    #[tokio::test]
    async fn token_resolves_immediately_if_already_triggered() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        let mut token = shutdown.token();
        timeout(Duration::from_millis(100), token.cancelled())
            .await
            .expect("token should resolve immediately");
    }

    #[tokio::test]
    async fn multiple_tokens_observe_one_trigger() {
        let shutdown = Shutdown::new();
        let mut a = shutdown.token();
        let mut b = shutdown.token();
        let mut c = shutdown.token();
        shutdown.trigger();
        timeout(Duration::from_millis(100), async move {
            a.cancelled().await;
            b.cancelled().await;
            c.cancelled().await;
        })
        .await
        .expect("all tokens should resolve");
    }

    #[tokio::test]
    async fn trigger_is_idempotent() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.trigger();
        shutdown.trigger();
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn token_does_not_resolve_before_trigger() {
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        let res = timeout(Duration::from_millis(20), token.cancelled()).await;
        assert!(res.is_err(), "token should not resolve before trigger");
    }

    #[tokio::test]
    async fn shutdown_drop_resolves_outstanding_tokens() {
        // If the source is dropped, tokens still wake up — no use
        // hanging forever.
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        drop(shutdown);
        timeout(Duration::from_millis(100), token.cancelled())
            .await
            .expect("token should resolve when source drops");
    }

    #[tokio::test]
    async fn select_pattern_preempts_long_work() {
        // The realistic usage: select between cancellation and work.
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        let work = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            "should not happen"
        };

        shutdown.trigger();
        let outcome = tokio::select! {
            () = token.cancelled() => "cancelled",
            r = work => r,
        };
        assert_eq!(outcome, "cancelled");
    }
}
