use std::thread;

use crate::ThreadPool;
use crate::Result;

/// It is actually not a thread pool. It spawns a new thread every time
/// the `spawn` method is called.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of threads.
    /// Returns an error if any thread fails to spawn. All previously-spawned threads are terminated.
    fn new(_threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    ///Spawn a function into the threadpool.
    /// Spawning always succeeds, but if the function panics the threadpool continues to operate with the same number of threads 
    /// — the thread count is not reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        thread::spawn(job);
    }
}

