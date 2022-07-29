use crate::core::event::{Event, EventRef};
use crate::core::job::{JobRef, Job, Priority};
use crate::helpers::{Parameters, ConcurrentDataRef};
use crate::Context;

use std::mem;
use std::ops::Range;
use std::cell::UnsafeCell;


pub struct Args<'l, ContextData, ImmutableData> {
    pub context_data: &'l mut ContextData,
    pub immutable_data: &'l ImmutableData,
}

pub struct JoinBuilder<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2> {
    inner: Parameters<'c, 'cd, 'id, ContextData, ImmutableData>,
    f1: F1,
    f2: F2,
}

#[inline]
pub (crate) fn new_join<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2>(inner: Parameters<'c, 'cd, 'id, ContextData, ImmutableData>, f1: F1, f2: F2) -> JoinBuilder<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2> 
where
    F1: FnOnce(&mut Context, Args<ContextData, ImmutableData>) + Send,
    F2: FnOnce(&mut Context, Args<ContextData, ImmutableData>) + Send,
{
    JoinBuilder {
        inner,
        f1,
        f2,
    }
}

impl<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2> JoinBuilder<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2>
{
    /// Specify some per-context data that can be mutably accessed by the run function.
    ///
    /// This can be useful to store and reuse some scratch buffers and avoid memory allocations in the
    /// run function.
    ///
    /// The length of the slice must be at least equal to the number of worker threads plus one.
    ///
    /// For best performance make sure the size of the data is a multiple of L1 cache line size (see `CachePadded`).
    #[inline]
    pub fn with_context_data<'cd2, CtxData: Send>(self, context_data: &'cd2 mut [CtxData]) -> JoinBuilder<'c, 'cd2, 'id, CtxData, ImmutableData, F1, F2> {
        JoinBuilder {
            inner: self.inner.with_context_data(context_data),
            f1: self.f1,
            f2: self.f2,
        }
    }

    #[inline]
    pub fn with_immutable_data<'id2, Data: Sync>(self, immutable_data: &'id2 Data) -> JoinBuilder<'c, 'cd, 'id2, ContextData, Data, F1, F2> {
        JoinBuilder {
            inner: self.inner.with_immutable_data(immutable_data),
            f1: self.f1,
            f2: self.f2,
        }
    }

    #[inline]
    pub fn with_priority(self, priority: Priority) -> Self {
        JoinBuilder {
            inner: self.inner.with_priority(priority),
            f1: self.f1,
            f2: self.f2,
        }
    }
}

impl<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2> JoinBuilder<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2> 
where
    F1: FnOnce(&mut Context, Args<ContextData, ImmutableData>) + Send,
    F2: FnOnce(&mut Context, Args<ContextData, ImmutableData>) + Send,
{
    pub fn run(mut self) {
        unsafe {
            let event = Event::new(1, self.inner.context().thread_pool_id());

            let job = JoinJob {
                data: ConcurrentDataRef::from_ref(&mut self.inner),
                function: UnsafeCell::new(Some(self.f2)),
                event: event.unsafe_ref(),
            };

            let priority = self.inner.priority();
            self.inner.context_mut().enqueue_job(job.as_job_ref().with_priority(priority));

            let (context_data, immutable_data) = job.data.get(self.inner.context());

            (self.f1)(self.inner.context_mut(), Args { context_data, immutable_data });

            self.inner.context_mut().wait(&event);
        }
    }
}



struct JoinJob<ContextData, ImmutableData, Func> {
    data: ConcurrentDataRef<ContextData, ImmutableData>,
    function: UnsafeCell<Option<Func>>,
    event: EventRef,
}

impl<ContextData, ImmutableData, Func> Job for JoinJob<ContextData, ImmutableData, Func>
where
    Func: FnOnce(&mut Context, Args<ContextData, ImmutableData>) + Send,
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, _range: Range<u32>) {
        let this: &Self = mem::transmute(this);

        let (context_data, immutable_data) = this.data.get(ctx);

        (*this.function.get()).take().map(|f| f(ctx, Args { context_data, immutable_data }));

        this.event.signal(ctx, 1);
    }
}

impl<ContextData, ImmutableData, Func> JoinJob<ContextData, ImmutableData, Func>
where
    Func: FnOnce(&mut Context, Args<ContextData, ImmutableData>) + Send,
{
    #[inline]
    unsafe fn as_job_ref(&self) -> JobRef {
        JobRef::new(self)
    }
}


#[test]
fn test_simple_join() {
    use crate::ThreadPool;

    let pool = ThreadPool::builder().with_worker_threads(3).build();
    let mut ctx = pool.pop_context().unwrap();
    use std::sync::atomic::{AtomicU32, Ordering};

    let count = AtomicU32::new(0);

    for _ in 0..100 {
        ctx.join(
            |ctx| {
                ctx.join(
                    |ctx| {
                        ctx.join(
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                        );
                    },
                    |ctx| {
                        ctx.join(
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                        );
                    },
                );
            },
            |ctx| {
                ctx.join(
                    |ctx| {
                        ctx.join(
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                        );
                    },
                    |ctx| {
                        ctx.join(
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                            |_| { count.fetch_add(1, Ordering::Relaxed); },
                        );
                    },
                );
            }
        );

        assert_eq!(count.load(Ordering::Relaxed), 8);
        count.store(0, Ordering::Relaxed);
    }

    pool.shut_down().wait();
}
