use super::sync::{Ordering, AtomicI32, AtomicPtr, Mutex, Condvar};
use super::Context;
use super::job::JobRef;
use super::thread_pool::ThreadPoolId;

use crossbeam_utils::Backoff;
use aliasable::boxed::AliasableBox;

// For debugging.
// Use std's atomic type explicitly here because loom's doesn't support static initialization.
static NEXT_EVENT_ID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(12345);

const STATE_DEFAULT: i32 = 0;
const STATE_SIGNALING: i32 = 1;
const STATE_SIGNALED: i32 = 2;

/// Use this instead of Box<Event>.
/// 
/// This is very much like a Box<Event>, however, it allows mixing `*const Event` and and `&Event`
/// access to its content, which according to miri's current borrowing model is not allowed for
/// the content of a standard Box.
pub type BoxedEvent = AliasableBox<Event>;

/// The main synchronization utility.
///
/// An event is can be used to wait until a given number of dependencies are met.
///
/// When waiting, the event tries to steal and execute jobs to avoid putting the current
/// thread to sleep.
///
/// The completion of dependencies is communicated to the event by calling `Event::signal`.
///
/// A job can be automatically scheduled by the event as soon as all dependencies are met
/// by registering it via `Event::then`.
pub struct Event {
    // The number of unresolved dependency.
    deps: AtomicI32,
    // Whether the dependencies has been met AND it is safe to deallocate the event.
    // We can't simply use deps, because we need to keep the object alive for a little
    // bit after deps reach zero.
    //
    // No read or write to the event is safe after state is set to STATE_SIGNALED,
    // except for the threads that owns the event (the one that calls wait).
    state: AtomicI32,
    // A list of jobs to schedule when the dependencies are met.
    waiting_jobs: AtomicLinkedList<JobRef>,
    // As a last resort, a condition variable and its mutex to wait on if we couldn't
    // keep busy while the dependencies are being processed.
    mutex: Mutex<()>,
    cond: Condvar,

    thread_pool_id: ThreadPoolId,
    // An ID for debugging
    id: i32,
}

impl Event {
    pub const MAX_DEPENDECIES: u32 = std::i32::MAX as u32;

    pub fn new(deps: u32, pool_id: ThreadPoolId) -> Self {
        debug_assert!(deps <= Self::MAX_DEPENDECIES);

        let state = if deps == 0 {
            STATE_SIGNALED
        } else {
            STATE_DEFAULT
        };

        Event {
            deps: AtomicI32::new(deps as i32),
            waiting_jobs: AtomicLinkedList::new(),
            state: AtomicI32::new(state),
            mutex: Mutex::new(()),
            cond: Condvar::new(),
            thread_pool_id: pool_id,
            id: NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    // pub(crate) fn log_deps(&self) {
    //     println!("event {:?}: {}", self as *const _, self.deps.load(Ordering::SeqCst));
    // }
    
    /// Creates a boxed event.
    pub fn new_boxed(deps: u32, pool_id: ThreadPoolId) -> BoxedEvent {
        AliasableBox::from_unique(Box::new(Event::new(deps, pool_id)))
    }

    pub fn reset(&mut self, deps: u32) {
        assert!(self.state.load(Ordering::Acquire) == STATE_SIGNALED);

        let state = if deps == 0 { STATE_SIGNALED } else { STATE_DEFAULT };
        self.state.store(state, Ordering::Release);
        self.deps.store(deps as i32, Ordering::Release);
    }

    pub fn signal_one(&self, ctx: &mut Context) -> bool {
        self.signal(ctx, 1)
    }

    pub fn signal(&self, ctx: &mut Context, n: u32) -> bool {
        unsafe {
            Self::signal_ptr(self as *const Self, ctx, n)
        }
    }

    /// Same as Event::signal but only needs a *const pointer to self instead of &Self.
    ///
    /// Prefer this over Event::signal when signaling from a thread that does not own the
    /// event. Since the event memory can go away at any time once after decrementing deps
    /// or setting state to STATE_SIGNALED, it is important for self to be a *const pointer
    /// so that miri allows the memory to disappear before the pointer.
    ///
    /// One of the ways Event can be used is by being on the stack of the thread that dispatch
    /// jobs and using Event::wait on that thread to ensure the even and job data lives long
    /// enough. In this situation:
    ///  - On any other thread, there *must* be no access to the event nor any job data which
    ///    lifetime is tied to the even after calling signal_ptr.
    ///    Signaling should always be the very last thing that a job does since it can unblock
    ///    threads that could deallocate the event itself and associated job data if any.
    pub unsafe fn signal_ptr(this: *const Self, ctx: &mut Context, n: u32) -> bool {
        debug_assert!(!(*this).is_signaled(), "already signaled {:?}:{:?}", this, (*this).id);
        debug_assert!((*this).deps.load(Ordering::SeqCst) >= 1);
        assert_eq!((*this).thread_pool_id, ctx.thread_pool_id());

        profiling::scope!("signal");
        let n = n as i32;
        let deps = (*this).deps.fetch_add(-n, Ordering::Relaxed) - n;

        if deps > 0 {
            // After reading deps, it isn't guaranteed that self is valid except for the
            // one thread which signaled the last dependency (the one thread not taking this
            // branch).
            return false;
        }

        debug_assert!(deps == 0, "signaled too many time");

        (*this).state.store(STATE_SIGNALING, Ordering::SeqCst);

        // Executing the first job ourselves avoids the overhead of going
        // through the job queue.
        // TODO: this can create an unbounded recursion.
        // We could track the recursion level in Context and decide whether to
        // execute the first job ourselves based on that.
        let mut first = None;
        (*this).waiting_jobs.pop_all(&mut |job| {
            if first.is_none() {
                first = Some(job);
            } else {
                ctx.schedule_job(job)
            }
        });

        {
            std::mem::drop((*this).mutex.lock().unwrap());

            (*this).cond.notify_all();
        }

        // It is important to mark this atomic boolean after setting the event.
        // when waiting we can only assume that the wait is over when this atomic
        // is set to true waiting on the event is not sufficient. This is because
        // we have to make sure this store can safely happen.
        // If we'd do the store before setting the event, then setting the event
        // would not be safe because the waiting thread might have continued from
        // an early-out on the state check. The waiting thread is responsible
        // for keeping the event alive state has been set to true.
        (*this).state.store(STATE_SIGNALED, Ordering::Release);

        // After the state store above, self isn't guaranteed to be valid.

        if let Some(job) = first {
            job.execute(ctx);
        }

        true
    }

    pub unsafe fn signal2(this: *const Self, n: u32) -> Option<AtomicLinkedList<JobRef>> {
        debug_assert!(!(*this).is_signaled(), "already signaled {:?}:{:?}", this, (*this).id);
        debug_assert!((*this).deps.load(Ordering::SeqCst) >= 1);

        profiling::scope!("signal");
        let n = n as i32;
        let deps = (*this).deps.fetch_add(-n, Ordering::Relaxed) - n;

        if deps > 0 {
            // After reading deps, it isn't guaranteed that self is valid except for the
            // one thread which signaled the last dependency (the one thread not taking this
            // branch).
            return None;
        }

        debug_assert!(deps == 0, "signaled too many time");

        (*this).state.store(STATE_SIGNALING, Ordering::SeqCst);

        let waiting_gobs = (*this).waiting_jobs.take();

        {
            std::mem::drop((*this).mutex.lock().unwrap());

            (*this).cond.notify_all();
        }

        // It is important to mark this atomic boolean after setting the event.
        // when waiting we can only assume that the wait is over when this atomic
        // is set to true waiting on the event is not sufficient. This is because
        // we have to make sure this store can safely happen.
        // If we'd do the store before setting the event, then setting the event
        // would not be safe because the waiting thread might have continued from
        // an early-out on the state check. The waiting thread is responsible
        // for keeping the event alive state has been set to true.
        (*this).state.store(STATE_SIGNALED, Ordering::Release);

        // After the state store above, self isn't guaranteed to be valid.

        Some(waiting_gobs)
    }

    #[inline]
    fn has_unresolved_dependencies(&self) -> bool {
        self.deps.load(Ordering::Acquire) > 0
    }

    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_SIGNALED
    }

    #[allow(unused)]
    pub(crate) fn log(&self, msg: &str) {
        println!("event {:?}:{} {}", self as *const _, self.id, msg);
    }

    #[allow(unused)]
    pub(crate) fn then(&self, ctx: &mut Context, job: JobRef) {
        if self.is_signaled() {
            ctx.schedule_job(job);
            return;
        }

        self.waiting_jobs.push(job);

        // This can be called concurrently with `signal`, its possible for `deps` to be read here
        // before decrementing it in `signal` but submitting the jobs happens before `push`.
        // This sequence means the event ends up signaled with a job sitting in the waiting list.
        // To prevent that we check `deps` a second time and submit again if it has reached zero in
        // the mean time.
        if !self.has_unresolved_dependencies() {
            self.waiting_jobs.pop_all(&mut |job| {
                ctx.schedule_job(job);
            });
        }
    }


    fn try_wait(&self, ctx: &mut Context) -> bool {
        profiling::scope!("steal jobs");
        loop {
            if self.is_signaled() {
                // Fast path: the event's dependencies were all met before
                // we had to block on the condvar.

                ctx.stats.fast_wait += 1;
                return true;
            }

            // Steal a job and execute it. If we are lucky our dependencies will
            // be met by the time we run out of useful things to do.
            if !ctx.keep_busy() {
                return false;
            }
        }
    }

    /// Wait until all dependencies of this event are met, and until
    /// it is safe to destroy the event (no other threads are going to read or write
    /// into it)
    pub(crate) fn wait(&self, ctx: &mut Context) {
        profiling::scope!("wait");

        //assert_eq!(self.thread_pool_id, ctx.thread_pool_id());

        // TODO: would it be possible to block on the worker thread's condition variable if there is
        // one instead of always blocking on the event's? That would allow the worker to resume
        // working if there is new work.

        {
            // Note: this important to guarantee the scheduler never deadlocks: A thread
            // waiting on some work to finish must not go to sleep unless its job queues
            // are empty. The try_wait call here ensures that.
            if self.try_wait(ctx) {
                return;
            }
        }

        // Slower path: using the condition variable.

        ctx.stats.cond_wait += 1;

        {
            profiling::scope!("wait(condvar)");
            'outer: loop {
                let mut guard = self.mutex.lock().unwrap();
                while self.state.load(Ordering::Acquire) == STATE_DEFAULT {
                    guard = self.cond.wait(guard).unwrap();
                }

                break 'outer;
            }
        }

        // We have to spin until state has been stored to ensure that it is safe
        // for the signaling thread to do the store operation.

        let backoff = Backoff::new();

        for i in 0..200 {
            if self.state.load(Ordering::Acquire) == STATE_SIGNALED {
                if i != 0 {
                    ctx.stats.cond_wait_spin += 1;
                    ctx.stats.spinned += i;
                }
                return;
            }

            backoff.spin();

            #[cfg(loom)]
            loom::thread::yield_now();
        }

        // The majority of the time we only check state once. If we are unlucky we
        // can end up spinning for a longer time, so get back to trying to steal some jobs.
        let mut i = 200;
        while self.state.load(Ordering::Acquire) != STATE_SIGNALED {
            ctx.keep_busy();
            i += 1;

            #[cfg(loom)]
            loom::thread::yield_now();
        }

        ctx.stats.cond_wait_spin += 1;
        ctx.stats.spinned += i;
    }

    pub fn unsafe_ref(&self) -> EventRef {
        EventRef { event: self }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EventRef {
    event: *const Event,
}

unsafe impl Send for EventRef {}

impl EventRef {
    pub unsafe fn signal(&self, ctx: &mut Context, n: u32) -> bool {
        Event::signal_ptr(self.event, ctx, n)
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        debug_assert_eq!(self.deps.load(Ordering::Acquire), 0);
        debug_assert!(self.state.load(Ordering::Acquire) == STATE_SIGNALED);
    }
}

unsafe impl Sync for Event {}
unsafe impl Send for Event {}


pub struct AtomicLinkedList<T> {
    first: AtomicPtr<Node<T>>,
}

struct Node<T> {
    payload: Option<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> AtomicLinkedList<T> {
    pub fn new() -> Self {
        AtomicLinkedList {
            first: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.first.load(Ordering::Relaxed) == std::ptr::null_mut()
    }

    pub fn push(&self, payload: T) {
        let node = Box::into_raw(Box::new(Node {
            payload: Some(payload),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }));

        unsafe {
            loop {
                let first = self.first.load(Ordering::Acquire);
                (*node).next.store(first, Ordering::Release);

                if self.first.compare_exchange(first, node, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                    break;
                }
            }
        }
    }

    pub fn pop_all(&self, cb: &mut dyn FnMut(T)) {
        // First atomically swap out the first node.
        let mut node;
        loop {
            node = self.first.load(Ordering::Acquire);
            let res = self.first.compare_exchange(node, std::ptr::null_mut(), Ordering::SeqCst, Ordering::Relaxed);
            if res.is_ok() {
                break;
            }
        }

        // Now that we have exclusive access to the nodes, we can execute the callback.
        while !node.is_null() {
            unsafe {
                if let Some(payload) = (*node).payload.take() {
                    cb(payload);
                }

                let next = (*node).next.load(Ordering::Relaxed);
                {
                    let _ = Box::from_raw(node);
                }

                node = next;
            }
        }
    }

    pub fn take(&self) -> Self {
        let list = AtomicLinkedList {
            first: AtomicPtr::new(self.first.swap(std::ptr::null_mut(), Ordering::Relaxed)),
        };

        list
    }
}

impl<T> Drop for AtomicLinkedList<T> {
    fn drop(&mut self) {
        self.pop_all(&mut |_| {
            panic!("Leaked job !");
        });
    }
}

#[test]
fn test_event() {
    use crate::helpers::Sendable;

    use crate::ThreadPool;
    let pool = ThreadPool::builder()
        .with_worker_threads(3)
        .with_contexts(1)
        .build();

    let mut ctx = pool.pop_context().unwrap();

    for _ in 0..1000 {
        let mut memory = 0u32;
        let mut memory_ref = Sendable::new(&mut memory as *mut u32);
        let event = Event::new(1, pool.id());
        let event_ref = event.unsafe_ref();

        ctx.run(
            // Note: it's easy to forget to make the callback move
            // and end up capturing locals by ref which mostly works
            // but does not make miri happy.
            move |ctx| {
                unsafe {
                    **memory_ref.get_mut() = 1;
                    event_ref.signal(ctx, 1);
                }
            },
            crate::Priority::High
        );

        event.wait(&mut ctx);
        assert!(event.is_signaled());
        assert_eq!(memory, 1);
    }

    pool.shut_down().wait();
}
