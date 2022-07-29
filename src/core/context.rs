use crate::sync::{Mutex, Arc};

use crossbeam_deque::{Worker as WorkerQueue};

use super::Shared;
use super::job::{HeapJob, JobRef, Priority};
use super::event::Event;
use super::thread_pool::{ThreadPool, ThreadPoolId};
// In principle there should not be this dependency, but it's nice to be able
// to do ctx.for_each(...). It'll probably move into an extension trait.
use crate::array::{ForEachBuilder, new_for_each};
use crate::join::{new_join};
use crate::helpers::*;
use crate::task::{task_builder, TaskBuilder, TaskDependency};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContextId(pub(crate) u32);

impl ContextId {
    pub fn index(&self) -> usize { self.0 as usize }
}

/// The main entry point for submitting and processing work in parallel.
pub struct Context {
    id: u32,
    is_worker: bool,
    steal_from_other_contexts: bool,
    num_contexts: u8,
    queues: [WorkerQueue<JobRef>; 2],
    // Keep track of whether we may have work in our local queues. This is simple
    // because only the context can insert into its queues, so we can update this
    // when pushing and popping from the queues.
    // Only when this value changes do we propagate this information to a shared array
    // of atomics that is visible from other threads (a more expensive operation).
    may_have_work: [bool; 2],

    pub(crate) shared: Arc<Shared>,
    pub(crate) stats: Stats,
}

unsafe impl Send for Context {}

impl Context {
    // Get some stats for debugging purposes.
    pub fn stats(&self) -> &Stats { &self.stats }

    pub fn id(&self) -> ContextId { ContextId(self.id) }

    pub fn thread_pool_id(&self) -> ThreadPoolId {
        self.shared.id
    }

    pub fn is_worker_thread(&self) -> bool {
        self.is_worker
    }

    pub fn num_worker_threads(&self) -> u32 { self.shared.num_workers }

    /// Returns the total number of contexts, including worker threads.
    pub fn num_contexts(&self) -> u32 { self.num_contexts as u32 }

    /// Returns a reference to this context's thread pool.
    pub fn thread_pool(&self) -> ThreadPool {
        ThreadPool {
            shared: self.shared.clone(),
        }
    }

    pub fn wait(&mut self, event: &Event) {
        event.wait(self);
    }

    /// Run a simple function asynchronously, without providing a way to wait
    /// until it terminates.
    pub fn run<F>(&mut self, job: F, priority: Priority) where F: FnOnce(&mut Context) + Send {
        unsafe {
            self.schedule_job(HeapJob::new_ref(job).with_priority(priority));
        }
    }

    /// TODO: move this into an extension trait.
    #[inline]
    pub fn for_each<'a, 'c, Item: Send>(&'c mut self, items: &'a mut [Item]) -> ForEachBuilder<'a, 'static, 'static, 'c, Item, (), (), ()> {
        new_for_each(self.with_priority(Priority::High), items)
    }

    /// TODO: move this into an extension trait.
    #[inline]
    pub fn join<'c, F1, F2>(&'c mut self, f1: F1, f2: F2)
    where
        F1: FnOnce(&mut Context) + Send,
        F2: FnOnce(&mut Context) + Send,
    {
        new_join(
            self.with_priority(Priority::High),
            |ctx, _| f1(ctx),
            |ctx, _| f2(ctx),
        ).run();
    }

    #[inline]
    pub fn with_priority<'c>(&'c mut self, priority: Priority) -> Parameters<'c, 'static, 'static, (), ()> {
        // TODO: miri does not like how we create a `&mut ()` out of a null pointer when no
        // conetxt data is passed. We can trick it into not tracking the pointer by crafting
        // casting a slice of size zero with a non-zero address casted from an integer.
        // Unfortunately that's super scary and maybe even more prone to turn into actual UB.
        //let context_data: &'static mut [()] = unsafe {
        //    std::slice::from_raw_parts_mut(
        //        std::mem::transmute(0xdeadbeefusize),
        //        0,
        //    )
        //};
        Parameters {
            ctx: self,
            context_data: &mut[],
            immutable_data: &(),
            priority
        }
    }

    #[inline]
    pub fn with_context_data<'c, 'cd, Data: Send>(&'c mut self, data: &'cd mut [Data]) -> Parameters<'c, 'cd, 'static, Data, ()> {
        self.with_priority(Priority::High).with_context_data(data)
    }

    /// TODO: move this into an extension trait.
    #[inline]
    pub fn task(&mut self) -> TaskBuilder<(), (), ()> {
        task_builder(self)
    }

    /// TODO: move this into an extension trait.
    #[inline]
    pub fn then<Dep>(&mut self, dep: Dep) -> TaskBuilder<Dep, (), ()>
    where Dep: TaskDependency
    {
        self.task().with_input(dep)
    }

    pub fn create_context_data<T: Send>(&self, data: Vec<T>) -> HeapContextData<T> {
        HeapContextData::from_vec(data, self.id())
    }

    /// Attempt to fetch or steal one job and execute it.
    ///
    /// Return false if we couldn't find a job to execute.
    /// Useful when the current thread needs to wait for something to happen and we would rather
    /// process work than put the thread to sleep.
    pub fn keep_busy(&mut self) -> bool {
        for priority in [Priority::High, Priority::Low] {
            if let Some(job) = self.fetch_local_job(priority) {
                unsafe {
                    self.execute_job(job);
                    return true;
                }
            }

            if !self.steal_from_other_contexts {
                continue;
            }

            if let Some(job) = self.shared.stealers.steal_one(self.index(), priority, &self.shared.activity) {
                unsafe {
                    self.execute_job(job);
                    return true;
                }
            }
        }

        false
    }

    // When executing a workload we are guaranteed that only the context that
    // submitted the work and the worker contexts can access the jobs for that
    // particular workload. This function returns an index that is always inferior
    // to the number of worker context + 1, and is guaranteed to never be used
    // on multiple threads at the same time.
    // Some higher level utilities like the parallel for_each take advantage of
    // that to manage some per-context data.
    //
    // TODO: I'm not sure what the best/cleanest way to expose this in the API.
    // Maybe it's fine in its current form.
    #[doc(hidden)]
    pub fn data_index(&self) -> usize {
        if self.is_worker_thread() {
            self.id as usize
        } else {
            self.num_worker_threads() as usize
        }
    }

    pub(crate) fn index(&self) -> usize { self.id as usize }

    pub(crate) fn new_worker(id: u32, num_contexts: u32, queues: [WorkerQueue<JobRef>; 2], shared: Arc<Shared>) -> Self {
        Context {
            id,
            is_worker: true,
            steal_from_other_contexts: true,
            num_contexts: num_contexts as u8,
            queues,
            may_have_work: [false, false],
            shared,
            stats: Stats::new(),
        }
    }

    pub(crate) fn new(id: u32, num_contexts: u32, queues: [WorkerQueue<JobRef>; 2], shared: Arc<Shared>) -> Self {
        Context {
            id,
            is_worker: false,
            steal_from_other_contexts: false,
            num_contexts: num_contexts as u8,
            queues,
            may_have_work: [false, false],
            shared,
            stats: Stats::new(),
        }
    }

    pub(crate) fn get_queue(&self, priority: Priority) -> &WorkerQueue<JobRef> {
        &self.queues[priority.index()]
    }

    pub(crate) fn schedule_job(&mut self, job: JobRef) {
        profiling::scope!("schedule_job");

        self.enqueue_job(job);

        self.wake(1);
    }

    pub(crate) fn enqueue_job(&mut self, job: JobRef) {
        let priority = job.priority().index();
        if !self.may_have_work[priority] {
            self.may_have_work[priority] = true;
            self.shared.activity.mark_context_active(self.id().0, job.priority());
        }

        self.queues[priority].push(job);
    }

    pub(crate) fn fetch_local_job(&mut self, priority: Priority) -> Option<JobRef> {
        let priority_idx = priority.index();
        let job = self.queues[priority_idx].pop();

        if job.is_none() && self.may_have_work[priority_idx] {
            self.may_have_work[priority_idx] = false;
            self.shared.activity.mark_context_inactive(self.id().0, priority);
        }

        job
    }

    pub(crate) unsafe fn execute_job(&mut self, mut job: JobRef) {
        if let Some(next) = job.split() {
            self.enqueue_job(next);
            self.wake(1);
        }

        job.execute(self);
        self.stats.jobs_executed += 1;
    }

    /// Wake up to n worker threads (stop when they are all awake).
    ///
    /// This function is fairly expensive when it causes a thread to
    /// wake up (most of the time is spent dealing with the condition
    /// variable).
    /// However it is fairly cheap if all workers are already awake.
    pub(crate) fn wake(&mut self, n: u32) {
        self.shared.sleep.wake(n, self.id());
    }

    pub(crate) fn queues_are_empty(&self) -> bool {
        self.queues[0].is_empty() && self.queues[1].is_empty()
    }
}

// We don't store the context itself when recycling it to avoid a reference cycle
// with the shared struct.
struct InactiveContext {
    id: u32,
    is_worker: bool,
    steal_from_other_contexts: bool,
    num_contexts: u8,
    queues: [WorkerQueue<JobRef>; 2],
}


pub(crate) struct ContextPool {
    contexts: Mutex<Vec<InactiveContext>>,
}

impl ContextPool {
    pub fn with_capacity(cap: usize) -> ContextPool {
        ContextPool {
            contexts: Mutex::new(Vec::with_capacity(cap))
        }
    }

    pub fn pop(shared: Arc<Shared>) -> Option<Context> {
        let mut contexts = shared.context_pool.contexts.lock().unwrap();
        let shared = shared.clone();
        contexts.pop().map(|ctx| Context {
            id: ctx.id,
            is_worker: ctx.is_worker,
            steal_from_other_contexts: ctx.steal_from_other_contexts,
            num_contexts: ctx.num_contexts,
            queues: ctx.queues,
            may_have_work: [false, false],
            shared,
            stats: Stats::new(),
        })
    }

    pub fn recycle(&self, ctx: Context) {
        let mut contexts = self.contexts.lock().unwrap();
        contexts.push(InactiveContext {
            id: ctx.id,
            is_worker: ctx.is_worker,
            steal_from_other_contexts: ctx.steal_from_other_contexts,
            num_contexts: ctx.num_contexts,
            queues: ctx.queues,
        });
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Stats {
    /// number of jobs executed.
    pub jobs_executed: u64,
    /// How many times waiting on an event didn't involve waiting on a condvar.
    pub fast_wait: u64,
    /// How many times waiting on an event involved waiting on a condvar.
    pub cond_wait: u64,
    /// number of spinned iterations
    pub spinned: u64,
    /// Number of times we spinning was necessary after waiting for a condvar.
    pub cond_wait_spin: u64,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            jobs_executed: 0,
            fast_wait: 0,
            cond_wait: 0,
            spinned: 0,
            cond_wait_spin: 0,
        }
    }
}
