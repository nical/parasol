pub mod job;
pub mod context;
pub mod event;
pub mod thread_pool;
pub mod shutdown;
/// basic std::sync types reexported here so that we can hook loom into them for
/// testing.
pub mod sync;

use crossbeam_deque::{Stealer, Steal, Worker as WorkerQueue};
use crossbeam_utils::{CachePadded, sync::{Parker, Unparker}};

use sync::{Arc, Ordering, AtomicU32, thread};
use job::{JobRef, Priority};
use thread_pool::{ThreadPool, ThreadPoolBuilder, ThreadPoolId};
use context::{Context, ContextId, ContextPool};
use shutdown::Shutdown;

// Use std's atomic type explicitly here because loom's doesn't support static initialization.
static NEXT_THREADPOOL_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Data accessible by all contexts from any thread.
///
/// If you are familiar with rayon's code, this is somewhat equivalent to their
/// `Registry` struct.
pub(crate) struct Shared {
    /// Number of dedicated worker threads.
    pub num_workers: u32,
    /// Number of contexts. Always greater than the number of workers.
    pub num_contexts: u32,
    ///
    pub stealers: Stealers,
    /// keeps track of active worker threads.
    pub activity: Activity,
    /// State and logic to put worker threads to sleep and wake them up.
    pub sleep: Sleep,
    /// The unused contexts that can be requested by threads.
    pub context_pool: ContextPool,
    /// A unique ID per thread pool to sanity-check that we aren't trying
    /// to move work from a pool to another if there several of them.
    pub id: ThreadPoolId,
    /// state and logic to handle shutting down.
    pub shutdown: Shutdown,
    // A few hooks to register work
    handlers: ThreadPoolHooks,
}

pub(crate) fn init(params: ThreadPoolBuilder) -> ThreadPool {
    let num_threads = params.num_threads as usize;
    let num_contexts = num_threads + params.num_contexts as usize;

    let mut stealers = Vec::with_capacity(num_contexts);
    let mut queues = Vec::with_capacity(num_threads);
    for _ in 0..num_contexts {
        let hp = WorkerQueue::new_fifo();
        let lp = WorkerQueue::new_fifo();
        stealers.push(CachePadded::new([
            hp.stealer(),
            lp.stealer(),
        ]));
        queues.push(Some([hp, lp]));
    }

    let (sleep, mut parkers) = Sleep::new(num_threads, num_contexts);

    let shared = Arc::new(Shared {
        num_workers: num_threads as u32,
        num_contexts: num_contexts as u32,

        activity: Activity::new(num_contexts),

        stealers: Stealers {
            stealers,
        },

        sleep,

        handlers: ThreadPoolHooks {
            start: params.start_handler,
            exit: params.exit_handler,
        },

        shutdown: Shutdown::new(num_threads as u32),

        id: ThreadPoolId(NEXT_THREADPOOL_ID.fetch_add(1, Ordering::Relaxed)),

        context_pool: ContextPool::with_capacity(params.num_contexts as usize),
    });

    for i in 0..num_threads {
        let mut worker = Worker {
            ctx: Context::new_worker(
                i as u32, num_contexts as u32,
                queues[i].take().unwrap(),
                shared.clone()
            ),
            parker: parkers[i].take().unwrap(),
        };

        let mut builder = thread::Builder::new()
            .name((params.name_handler)(i as u32));

        if let Some(stack_size) = params.stack_size {
            builder = builder.stack_size(stack_size);
        }

        let _ = builder.spawn(move || {
            profiling::register_thread!("Worker");

            worker.run();

        }).unwrap();
    }

    for i in num_threads..num_contexts {
        shared.context_pool.recycle(Context::new(
            i as u32,
            num_contexts as u32,
            queues[i].take().unwrap(),
            shared.clone(),
        ));
    }

    ThreadPool { shared }
}


struct SleepState {
    unparker: Unparker,
    // The index of the context this one will start searching at next time it tries to steal.
    // Can be used as hint of which context last woke this worker.
    // Since it only guides a heuristic, it doesn't need to be perfectly accurate.
    next_target: AtomicU32,
}

pub(crate) struct Sleep {
    /// Atomic bitfield. Setting the Nth bit to one means the Nth worker thread is sleepy.
    sleepy_workers: AtomicU32,
    sleep_states: Vec<CachePadded<SleepState>>,
}

impl Sleep {
    fn new(num_threads: usize, num_contexts: usize) -> (Self, Vec<Option<Parker>>) {
        let mut parkers = Vec::with_capacity(num_threads);
        let mut sleep_states = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let parker = Parker::new();
            sleep_states.push(CachePadded::new(SleepState {
                unparker: parker.unparker().clone(),
                next_target: AtomicU32::new(((i + 1) % num_contexts) as u32),
            }));
            parkers.push(Some(parker));
        }

        let sleepy_worker_bits = (1 << (num_threads as u32)) - 1;

        (
            Sleep {
                sleepy_workers: AtomicU32::new(sleepy_worker_bits),
                sleep_states,
            },
            parkers,
        )
    }

    /// Wake up to n worker threads (stop when they are all awake).
    ///
    /// This function is fairly expensive when it causes a thread to
    /// wake up (most of the time is spent dealing with the condition
    /// variable).
    /// However it is fairly cheap if all workers are already awake.
    pub fn wake(&self, mut n: u32, waker: ContextId) {
        while n > 0 {
            //profiling::scope!("wake workers");
            let mut sleepy_bits = self.sleepy_workers.load(Ordering::Acquire);

            profiling::scope!(&format!("wake {} workers ({:b})", n, sleepy_bits));

            if sleepy_bits == 0 {
                // Everyone is already awake.
                return;
            }

            for i in 0..(self.sleep_states.len() as u32) {
                let bit = 1 << i;
                if sleepy_bits & bit == 0 {
                    continue;
                }

                let prev = self.sleepy_workers.fetch_and(!bit, Ordering::Release);
                if prev & bit == 0 {
                    // Someone else woke the thread up before we got to it.
                    // A good time to refresh our view of the sleep thread bits.
                    sleepy_bits = self.sleepy_workers.load(Ordering::Acquire);

                    if sleepy_bits == 0 {
                        return;
                    }

                    continue;
                }

                let sleep_state = &self.sleep_states[i as usize];
                sleep_state.next_target.store(waker.0, Ordering::Relaxed);

                profiling::scope!("unpark");
                sleep_state.unparker.unpark();

                n -= 1;
                break;
            }
        }
    }

    fn mark_sleepy(&self, worker: u32) -> u32 {
        let sleepy_bit = 1 << worker;
        self.sleepy_workers.fetch_or(sleepy_bit, Ordering::SeqCst) | sleepy_bit
    }

    /// Who woke me up? They are likely the best candidate for work stealing.
    fn get_waker_hint(&self, worker_index: usize) -> usize {
        self.sleep_states[worker_index]
            .next_target
            .load(Ordering::Relaxed) as usize
    }

    /// Tell the worker at the given index what context to try to steal from first.
    fn set_waker_hint(&self, worker_index: usize, waker: usize) {
        self.sleep_states[worker_index]
            .next_target
            .store(waker as u32, Ordering::Release);
    }

    /// Wake all workers.
    ///
    /// This is a bit heavy handed and mostly intended for the shutdown code. In the majority
    /// of cases (other than shutdown), using `wake` is better.
    fn wake_all(&self) {
        for state in &self.sleep_states {
            state.unparker.unpark();
        }
    }
}

pub(crate) struct Stealers {
    pub stealers: Vec<CachePadded<[Stealer<JobRef>; 2]>>,
}

impl Stealers {
    /// Attempt to steal one job.
    ///
    /// Can be called by any context, including non-worker ones. Useful when
    /// stealing jobs only to keep busy while waiting for somthing else to happen.
    pub fn steal_one(&self, stealer_index: usize, priority: Priority, activity: &Activity) -> Option<JobRef> {
        let start = stealer_index;

        let mut stolen = None;
        activity.for_each_active_context(priority, start as u32, &mut |idx| {
            for _ in 0..50 {
                let stealer = &self.stealers[idx as usize][priority.index()];
                match stealer.steal() {
                    Steal::Success(job) => {
                        // We'll try to steal from here again next time.
                        stolen = Some(job);
                        return true;
                    }
                    Steal::Empty => {
                        return false;
                    }
                    Steal::Retry => {}
                }
            }

            false
        });

        stolen
    }

    /// Attempt to steal multiple jobs, returning one of them.
    ///
    /// Only called by Worker contexts.
    /// Similar to steal above, but will try to steal a batch of jobs instead of just one,
    /// and uses the waker hint to start stealing from the last thread that woke us up.
    pub fn steal_batch(&self, ctx: &Context, sleep: &Sleep, priority: Priority, activity: &Activity) -> Option<JobRef> {
        let stealer_index = ctx.index();
        let start = sleep.get_waker_hint(stealer_index);

        let mut stolen = None;
        activity.for_each_active_context(priority, start as u32, &mut |idx| {
            let idx = idx as usize;
            if idx == stealer_index {
                return false;
            }

            //println!("--[ctx#{}] steal from {:?} ({:b})", ctx.index(), idx, activity.activity[priority.index()][0].load(Ordering::Acquire));

            for _ in 0..50 {
                let stealer = &self.stealers[idx][priority.index()];
                match stealer.steal_batch_and_pop(ctx.get_queue(priority)) {
                    Steal::Success(job) => {
                        // We'll try to steal from here again next time.
                        sleep.set_waker_hint(stealer_index, idx);
                        stolen = Some(job);
                        return true;
                    }
                    Steal::Empty => {
                        return false;
                    }
                    Steal::Retry => {}
                }
            }

            false
        });

        stolen
    }
}


/// Keep track of which contexts may have work in its queue.
///
/// This is only used as an optimization heuristic for work-stealing. There is no
/// correctness or safety guarantee that depends on any particular ordering of access
/// of this, however for performance it is better to mark a context active before making
/// work available in its queue rather than the opposite.
pub(crate) struct Activity {
    // An bitfiled per priority where each bit corresponds to a context.
    // if the bit is not set we know that there is no work in the context's queue
    // and therefore we can skip trying to steal from it.
    //
    // TODO: right now this limits us to 32 contexts total, which may or may turn out to
    // be too limiting in practice. To increase the limit we just need to have an array
    // of atomics per priority instead of just one atomic and do some bit shifting. It
    // doesn't matter whether the bits are stored in different atomics because we don't
    // need any form of synchronization between the bits. Packing the bits helps with
    // performance, though.
    // Storing them inline instead of on the heap is measurably faster.
    activity: [AtomicU32; 2],
    num_contexts: u32,
}

impl Activity {
    pub fn new(num_contexts: usize) -> Self {
        Activity {
            activity: [AtomicU32::new(0), AtomicU32::new(0)],
            num_contexts: num_contexts as u32,
        }
    }

    pub fn mark_context_active(&self, index: u32, priority: Priority) {
        debug_assert!(index < self.num_contexts);
        let bit = 1 << index;
        self.activity[priority.index()].fetch_or(bit, Ordering::Release);
    }

    pub fn mark_context_inactive(&self, index: u32, priority: Priority) {
        debug_assert!(index < self.num_contexts);
        let bit = 1 << index;
        self.activity[priority.index()].fetch_and(!bit, Ordering::Release);
    }

    pub fn for_each_active_context<F>(&self, priority: Priority, start: u32, cb: &mut F)
    where F: FnMut(u32) -> bool
    {
        let bits = self.activity[priority.index()].load(Ordering::Acquire);
        for i in 0..self.num_contexts {
            let idx = (start + i) % self.num_contexts;
            let bit = 1 << idx;

            if bits & bit == 0 {
                continue;
            }

            if cb(idx) {
                return;
            }
        }
    }
}

struct Worker {
    ctx: Context,
    parker: Parker,
}

impl Worker {
    fn run(&mut self) {
        let ctx = &mut self.ctx;
        let shared = Arc::clone(&ctx.shared);

        if let Some(handler) = &shared.handlers.start {
            handler.run(ctx.id().0);
        }

        'main: loop {
            for priority in [Priority::High, Priority::Low] {
                // First see if we have work to do in our own queues.
                while let Some(job) = ctx.fetch_local_job(priority) {
                    unsafe {
                        ctx.execute_job(job);
                    }
                }

                // See if there is work we can steal from other contexts.
                if let Some(job) = shared.stealers.steal_batch(ctx, &shared.sleep, priority, &shared.activity) {
                    unsafe {
                        ctx.execute_job(job);
                    }

                    // If we found anything to do via work-stealing, go back to checking the local
                    // queue again.
                    continue 'main;
                }
            }

            // Only the worker can install work in its own queues, so no other can install work in our
            // context without us noticing and as result if we get here the queue should be empty.
            debug_assert!(ctx.queues_are_empty());

            if shared.shutdown.is_shutting_down() {
                break;
            }

            // Couldn't find work to do in our or another context's queue, so
            // it's sleepy time.

            let _sleepy_bits = shared.sleep.mark_sleepy(ctx.id().0);

            self.parker.park();
        }

        // Shutdown phase.

        if let Some(handler) = &ctx.shared.handlers.exit {
            handler.run(ctx.id().0);
        }

        shared.shutdown.worker_has_shut_down();
    }
}

pub(crate) struct ThreadPoolHooks {
    start: Option<Box<dyn WorkerHook>>,
    exit: Option<Box<dyn WorkerHook>>,
}

pub trait WorkerHook: Send + Sync {
    fn run(&self, worker_id: u32);
}

impl<F> WorkerHook for F where F: Fn(u32) + Send + Sync + 'static {
    fn run(&self, worker_id: u32) { self(worker_id) }
}

