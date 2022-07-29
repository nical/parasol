/// Shutdown
///
/// the shutdown step isn't particularly complicated. Workers can check whether the
/// thread pool is shutting down by reading an atomic that is set when the shutdown
/// starts. Then we have a simple mutex/condvar pair tracking the remaining number of
/// workers to shut down that we can wait on.

use crate::sync::{Ordering, AtomicBool, Mutex, Condvar, Arc};

use crate::core::Shared;

pub(crate) struct Shutdown {
    pub is_shutting_down: AtomicBool,
    pub shutdown_mutex: Mutex<u32>,
    pub shutdown_cond: Condvar,
}

impl Shutdown {
    pub fn new(num_threads: u32) -> Self {
        Shutdown {
            is_shutting_down: AtomicBool::new(false),
            shutdown_mutex: Mutex::new(num_threads),
            shutdown_cond: Condvar::new(),
        }
    }

    pub fn begin_shut_down(shared: Arc<Shared>) -> ShutdownHandle {
        shared.shutdown.is_shutting_down.store(true, Ordering::SeqCst);

        shared.sleep.wake_all();

        ShutdownHandle { shared }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    pub fn wait_shutdown(&self) {
        let mut num_workers = self.shutdown_mutex.lock().unwrap();
        while *num_workers > 0 {
            num_workers = self.shutdown_cond.wait(num_workers).unwrap();
        }

        self.is_shutting_down.store(false, Ordering::Release);
    }

    pub fn worker_has_shut_down(&self) {
        let mut num_workers = self.shutdown_mutex.lock().unwrap();
        *num_workers -= 1;
        if *num_workers == 0 {
            self.shutdown_cond.notify_all();
        }
    }
}

pub struct ShutdownHandle {
    shared: Arc<Shared>
}

impl ShutdownHandle {
    pub fn wait(self) {
        self.shared.shutdown.wait_shutdown();
    }
}

#[test]
fn test_shutdown() {
    use std::sync::atomic::AtomicU32;
    use crate::ThreadPool;
    static INITIALIZED_WORKERS: AtomicU32 = AtomicU32::new(0);
    static SHUTDOWN_WORKERS: AtomicU32 = AtomicU32::new(0);

    for _ in 0..100 {
        for num_threads in 1..31 {
            INITIALIZED_WORKERS.store(0, Ordering::SeqCst);
            SHUTDOWN_WORKERS.store(0, Ordering::SeqCst);

            let pool = ThreadPool::builder()
                .with_worker_threads(num_threads)
                .with_start_handler(|_id| { INITIALIZED_WORKERS.fetch_add(1, Ordering::SeqCst); })
                .with_exit_handler(|_id| { SHUTDOWN_WORKERS.fetch_add(1, Ordering::SeqCst); })
                .build();

            let handle = pool.shut_down();
            handle.wait();

            assert_eq!(INITIALIZED_WORKERS.load(Ordering::SeqCst), num_threads);
            assert_eq!(SHUTDOWN_WORKERS.load(Ordering::SeqCst), num_threads);
        }
    }
}
