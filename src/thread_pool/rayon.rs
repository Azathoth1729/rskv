use crate::KvsError;

use super::ThreadPool;

/// Wrapper of rayon::ThreadPool
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
     fn new(num_threads: usize) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|e| KvsError::StringError(e.to_string()))?;

        Ok(RayonThreadPool(pool))
    }

    fn spawn<OP>(&self, job: OP)
    where
        OP: FnOnce() + Send + 'static,
    {
        self.0.spawn(job)
    }
}

impl RayonThreadPool {
    ///
    pub fn scope<'a, OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce(&rayon::Scope<'a>) -> R + Send,
        R: Send,
    {
        self.0.scope(op)
    }
}
