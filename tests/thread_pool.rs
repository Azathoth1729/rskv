use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_utils::sync::WaitGroup;
use rskv::{thread_pool::*, Result};

fn spawn_counter<P: ThreadPool>(pool: P) -> Result<()> {
    const TASK_NUM: usize = 20;
    const ADD_COUNT: usize = 1000;

    let wg = WaitGroup::new();
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..TASK_NUM {
        let counter = Arc::clone(&counter);
        let wg = wg.clone();
        pool.spawn(move || {
            for _ in 0..ADD_COUNT {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            drop(wg);
        })
    }

    wg.wait();
    assert_eq!(counter.load(Ordering::SeqCst), TASK_NUM * ADD_COUNT);
    Ok(())
}

fn spawn_mutex_counter<P: ThreadPool>(pool: P) -> Result<()> {
    const TASK_NUM: usize = 20;

    let counter = Arc::new(Mutex::new(0));

    (0..TASK_NUM).for_each(|_| {
        let counter = counter.clone();

        pool.spawn(move || {
            let mut num = counter.lock().unwrap();
            *num += 1;
        })
    });
    drop(pool);

    assert_eq!(*counter.lock().unwrap(), TASK_NUM);
    Ok(())
}

fn spawn_panic_task<P: ThreadPool>() -> Result<()> {
    const TASK_NUM: usize = 1000;

    let pool = P::new(4)?;
    for _ in 0..TASK_NUM {
        pool.spawn(move || {
            // It suppresses flood of panic messages to the console.
            // You may find it useful to comment this out during development.
            panic_control::disable_hook_in_current_thread();

            panic!();
        })
    }

    spawn_counter(pool)
}

#[test]
fn drop_join_thread_pool_spawn_counter() -> Result<()> {
    let pool = DropJoinThreadPool::new(4)?;
    spawn_counter(pool)?;

    let pool = DropJoinThreadPool::new(4)?;
    spawn_mutex_counter(pool)
}

#[test]
fn shared_queue_thread_pool_panic_task() -> Result<()> {
    spawn_panic_task::<DropJoinThreadPool>()
}

#[test]
fn rayon_thread_pool_spawn_counter() -> Result<()> {
    let pool = RayonThreadPool::new(4)?;
    spawn_counter(pool)?;

    let pool = RayonThreadPool::new(4)?;
    const TASK_NUM: usize = 20;

    let counter = Arc::new(Mutex::new(0));

    (0..TASK_NUM).for_each(|_| {
        let counter = counter.clone();

        pool.scope(move |_| {
            let mut num = counter.lock().unwrap();
            *num += 1;
        })
    });
    drop(pool);

    assert_eq!(*counter.lock().unwrap(), TASK_NUM);
    Ok(())
}

#[test]
fn my_thread_pool_spawn_counter() -> Result<()> {
    let pool = NaiveThreadPool::new(4)?;
    spawn_counter(pool)
}
