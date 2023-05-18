use crossbeam::channel::{self, Sender};
use log::debug;
use std::panic::catch_unwind;
use std::thread;

use crate::Result;
use crate::ThreadPool;

/// A thread pool using a shared queue inside.
///
/// If a spawned task panics, the old thread will be destroyed and a new one will be
/// created. It fails silently when any failure to create the thread at the OS level
/// is captured after the thread pool is created. So, the thread number in the pool
/// can decrease to zero, then spawning a task to the thread pool will panic.
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of threads.
    /// Returns an error if any thread fails to spawn. All previously-spawned threads are terminated.
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        for _ in 0..threads {
            start_thread(&rx).expect("spawn thread failed.");
        }
        Ok(SharedQueueThreadPool { tx })
    }

    /// Spawn a function into the threadpool.
    /// Spawning always succeeds, but if the function panics the threadpool continues to operate with the same number of threads
    /// — the thread count is not reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx
            .send(Box::new(job))
            .expect("the thread pool is empty.")
    }
}

fn start_thread(rx: &crossbeam::Receiver<Box<(dyn FnOnce() + Send + 'static)>>) -> Result<()> {
    let rx = rx.clone();
    thread::Builder::new()
        .spawn(move || {
            let res = catch_unwind(|| run_task(&rx));
            if res.is_err() {
                start_thread(&rx).expect("spawn thread failed.");
            }
        })
        .expect("spawn thread failed.");
    Ok(())
}

fn run_task(rx: &crossbeam::Receiver<Box<(dyn FnOnce() + Send + 'static)>>) {
    loop {
        match rx.recv() {
            Ok(job) => job(),
            Err(err) => debug!("the channel is empty and disconnected: {}", err),
        }
    }
}
