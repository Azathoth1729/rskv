use std::panic::AssertUnwindSafe;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc, Mutex};
use std::{panic, thread};

use log::error;

use crate::KvsError;

use super::ThreadPool;

type Job = Box<dyn FnOnce() + Send + 'static>;

///
pub struct DropJoinThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

impl ThreadPool for DropJoinThreadPool {
    fn new(num_threads: usize) -> crate::Result<Self> {
        if num_threads <= 0 {
            return Err(KvsError::StringError(
                "num_threads must greater than zero".to_owned(),
            ));
        }

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }

        Ok(DropJoinThreadPool {
            workers,
            sender: Some(sender),
        })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender
            .as_ref()
            .unwrap()
            .send(job)
            .expect("The thread pool has no thread.");
    }
}

/// When drop, join all threads in the pool.
impl Drop for DropJoinThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        self.workers.iter_mut().for_each(|worker| {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        })
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(receiver: Arc<Mutex<Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();

            match message {
                Ok(job) => {
                    if let Err(e) = panic::catch_unwind(AssertUnwindSafe(job)) {
                        error!("executes a job with error {:?}", e)
                    }
                }
                Err(_) => {
                    break;
                }
            }
        });

        Worker {
            thread: Some(thread),
        }
    }
}
