use tokio::sync::Semaphore;

// Note: semaphore.add_permits requires this to be less than usize::MAX >> 3
const MAX_SEMAPHORE_PERMITS: usize = 1000;

/// A shutdown mechanism that supports three use-cases:
///  - sending a shutdown signal,
///  - polling if the shutdown signal has been sent and
///  - blocking to await a shutdown signal.
///
/// I implemented the shutdown mechanism using a semaphore. The semaphore is initialised to 0, and
/// it is incremented to 1 once the shutdown signal has been sent. Blocking until the shutdown
/// signal is sent is implemented by waiting on the semaphore. The semaphore also supports
/// non-blocking polling, therefore to see if the signal has been sent, I can poll the semaphore to
/// see if its counter is above 0.
#[derive(Debug)]
pub struct ShutdownSignal {
    semaphore: Semaphore,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self {
            semaphore: Semaphore::new(0),
        }
    }

    pub fn poll_terminated(&self) -> bool {
        self.semaphore.try_acquire().is_ok()
    }

    pub fn shutdown(&self) {
        self.semaphore.add_permits(MAX_SEMAPHORE_PERMITS);
    }

    pub async fn await_shutdown_signal(&self) {
        let _permit = self.semaphore.acquire().await;
    } // _permit dropped here
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future::join;
    use tokio::time::delay_for;

    use super::*;

    #[test]
    fn test_shutdown() {
        let signal = ShutdownSignal::new();
        signal.shutdown();
    }

    #[tokio::test]
    async fn test_async() {
        use std::sync::Arc;

        let signal = Arc::new(ShutdownSignal::new());
        let s1 = signal.clone();
        let th1 = tokio::spawn(async move {
            delay_for(Duration::from_millis(200)).await;
            let signal = s1;
            signal.shutdown();
        });
        join(th1, signal.await_shutdown_signal()).await;
    }
}
