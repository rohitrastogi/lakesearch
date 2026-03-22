//! Async runtime bridge for CPU-bound work.
//!
//! `LakeRuntime` provides a rayon thread pool for CPU-intensive operations
//! (tokenization, segment building, posting list intersection) with an async
//! bridge to tokio via oneshot channels.
//!
//! Gated behind the `runtime` feature to keep core dependency-light for
//! callers that don't need async.

use rayon::ThreadPool;
use tokio::sync::oneshot;

/// Bridges tokio async code and rayon CPU-bound work.
///
/// All CPU-intensive operations (tokenization, FST construction, posting list
/// encoding/decoding, boolean set ops, BM25 scoring) should run via
/// [`cpu()`](Self::cpu) to avoid blocking tokio worker threads.
pub struct LakeRuntime {
    cpu_pool: ThreadPool,
}

impl LakeRuntime {
    /// Creates a new runtime with the given number of CPU threads.
    pub fn new(cpu_threads: usize) -> Self {
        Self {
            cpu_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(cpu_threads)
                .thread_name(|i| format!("lakesearch-cpu-{i}"))
                .build()
                .expect("failed to build rayon thread pool"),
        }
    }

    /// Run a CPU-bound closure on the rayon pool, returning the result
    /// to the tokio async context via a oneshot channel.
    pub async fn cpu<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.cpu_pool.spawn(move || {
            let result = f();
            let _ = tx.send(result);
        });
        rx.await.expect("cpu task panicked")
    }
}

impl Default for LakeRuntime {
    fn default() -> Self {
        Self::new(num_cpus())
    }
}

/// Returns the number of available CPU cores.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cpu_returns_result() {
        let rt = LakeRuntime::new(2);
        let result = rt.cpu(|| 2 + 2).await;
        assert_eq!(result, 4);
    }

    #[tokio::test]
    async fn cpu_concurrent_tasks() {
        let rt = LakeRuntime::new(4);
        let (a, b, c) = tokio::join!(rt.cpu(|| 1 + 1), rt.cpu(|| 2 * 3), rt.cpu(|| 10 - 4),);
        assert_eq!(a, 2);
        assert_eq!(b, 6);
        assert_eq!(c, 6);
    }

    #[tokio::test]
    async fn cpu_moves_data() {
        let rt = LakeRuntime::new(2);
        let data = [1, 2, 3, 4, 5];
        let sum = rt.cpu(move || data.iter().sum::<i32>()).await;
        assert_eq!(sum, 15);
    }
}
