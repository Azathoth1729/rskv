use std::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
};

use crate::KvsError;

use super::ThreadPool;

type Job = Box<dyn FnOnce() + Send + 'static>;

///
pub struct NaiveThreadPool {
    sender: Sender<Job>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(num_threads: usize) -> crate::Result<Self>
    where
        Self: Sized,
    {
        if num_threads <= 0 {
            return Err(KvsError::StringError(
                "num_threads must greater than zero".to_owned(),
            ));
        }
        let (tx, rx) = channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));

        (0..num_threads).for_each(|_| {
            spawn_in_pool(rx.clone());
        });

        Ok(NaiveThreadPool { sender: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).unwrap()
    }
}

fn spawn_in_pool(job: Arc<Mutex<Receiver<Job>>>) {
    thread::spawn(move || loop {
        let msg = job.lock().unwrap().recv();
        match msg {
            Ok(job) => job(),
            Err(_) => break,
        }
    });
}

// #[derive(Default)]
// pub struct Builder {
//     num_threads: Option<usize>,
//     thread_name: Option<String>,
//     thread_stack_size: Option<usize>,
// }

// impl Builder {
//     pub fn new() -> Builder {
//         Builder {
//             num_threads: None,
//             thread_name: None,
//             thread_stack_size: None,
//         }
//     }

//     pub fn num_threads(mut self, num_threads: usize) -> Builder {
//         assert!(num_threads > 0);
//         self.num_threads = Some(num_threads);
//         self
//     }

//     pub fn thread_name(mut self, name: String) -> Builder {
//         self.thread_name = Some(name);
//         self
//     }

//     pub fn thread_stack_size(mut self, size: usize) -> Builder {
//         self.thread_stack_size = Some(size);
//         self
//     }

//     pub fn build(self) -> crate::Result<NaiveThreadPool> {
//         NaiveThreadPool::new(self.num_threads.unwrap())
//     }
// }
