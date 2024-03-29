//! This module provides various thread pools. All thread pools should implement
//! the `ThreadPool` trait.

use crate::Result;

mod naive;
mod drop_join;
mod rayon;

pub use self::naive::NaiveThreadPool;
pub use self::drop_join::DropJoinThreadPool;
pub use self::rayon::RayonThreadPool;

/// The trait that all thread pools should implement.
pub trait ThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of
    /// threads.
    ///
    /// Returns an error if any thread fails to spawn. All previously-spawned threads
    /// are terminated.
    fn new(num_threads: usize) -> Result<Self>
    where
        Self: Sized;
    /// Spawns a function into the thread pool.
    ///
    /// Spawning always succeeds, but if the function panics the threadpool continues
    /// to operate with the same number of threads &mdash; the thread count is not
    /// reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
