use crate::core::event::Event;
use crate::core::job::{JobRef, Job, Priority};
use crate::Context;
use crate::sync::Arc;
use crate::helpers::*;
use crate::handle::*;
use crate::task::{TaskBuilder, TaskDependency};
use crate::ref_counted::{RefCounted, RefPtr};

use std::mem;
use std::ops::{Range, Deref, DerefMut};
use std::cell::UnsafeCell;

/// Input parameter of parallel `for_each` closures.
pub struct Args<'l, Item, ContextData, ImmutableData> {
    pub item: &'l mut Item,
    pub item_index: u32,
    pub context_data: &'l mut ContextData,
    pub immutable_data: &'l ImmutableData,
}

impl<'l, Item, ContextData, ImmutableData> Deref for Args<'l, Item, ContextData, ImmutableData> {
    type Target = Item;
    fn deref(&self) -> &Item { self.item }
}

impl<'l, Item, ContextData, ImmutableData> DerefMut for Args<'l, Item, ContextData, ImmutableData> {
    fn deref_mut(&mut self) -> &mut Item { self.item }
}

pub struct ForEachBuilder<'a, 'b, 'g, 'c, Item, ContextData, ImmutableData, Func> {
    pub items: &'a mut [Item],
    pub inner: Parameters<'c, 'b, 'g, ContextData, ImmutableData>,
    pub function: Func,
    pub range: Range<u32>,
    pub group_size: u32,
    pub parallel: Ratio,
}

pub(crate) fn new_for_each<'i, 'cd, 'id, 'c, Item, ContextData, ImmutableData>(
    inner: Parameters<'c, 'cd, 'id, ContextData, ImmutableData>,
    items: &'i mut [Item],
) -> ForEachBuilder<'i, 'cd, 'id, 'c, Item, ContextData, ImmutableData, ()> {
    assert!(items.len() <= Event::MAX_DEPENDECIES as usize);
    ForEachBuilder {
        range: 0..items.len() as u32,
        items,
        inner,
        function: (),
        group_size: 1,
        parallel: Ratio::DEFAULT,
    }
}

impl<'a, 'b, 'g, 'c, Item, ContextData, ImmutableData, F> ForEachBuilder<'a, 'b, 'g, 'c, Item, ContextData, ImmutableData, F>
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
    pub fn with_context_data<'w, CtxData: Send>(self, context_data: &'w mut [CtxData]) -> ForEachBuilder<'a, 'w, 'g, 'c, Item, CtxData, ImmutableData, F> {
        ForEachBuilder {
            items: self.items,
            inner: self.inner.with_context_data(context_data),
            function: self.function,
            range: self.range,
            group_size: self.group_size,
            parallel: self.parallel,
        }
    }

    #[inline]
    pub fn with_immutable_data<'i, Data: Sync>(self, immutable_data: &'i Data) -> ForEachBuilder<'a, 'b, 'i, 'c, Item, ContextData, Data, F> {
        ForEachBuilder {
            items: self.items,
            inner: self.inner.with_immutable_data(immutable_data),
            function: self.function,
            range: self.range,
            group_size: self.group_size,
            parallel: self.parallel,
        }
    }

    /// Restrict processing to a range of the data.
    pub fn with_range(mut self, range: Range<u32>) -> Self {
        assert!(range.end >= range.start);
        assert!(range.end <= self.items.len() as u32);
        self.range = range;

        self
    }

    /// Specify the number below which the scheduler doesn't attempt to split the workload.
    #[inline]
    pub fn with_group_size(mut self, group_size: u32) -> Self {
        self.group_size = group_size.max(1).min(self.items.len() as u32);

        self
    }

    /// Specify the priority of this workload.
    #[inline]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.inner = self.inner.with_priority(priority);

        self
    }

    // TODO: "parallel ratio" isn't a great name.

    /// Specify the proportion of items that we want to expose to worker threads.
    ///
    /// The remaining items will be processed on this thread.
    #[inline]
    pub fn with_parallel_ratio(mut self, ratio: Ratio) -> Self {
        self.parallel = ratio;

        self
    }

    /// Run this workload with the help of worker threads.
    ///
    /// This function returns after the workload has completed.
    #[inline]
    pub fn run<Func>(self, function: Func)
    where
        Func: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Sync + Send,
        Item: Sync + Send,
    {
        if !self.items.is_empty() {
            for_each(self.apply(function));
        }
    }

    #[inline]
    fn apply<Func>(self, function: Func) -> ForEachBuilder<'a, 'b, 'g, 'c, Item, ContextData, ImmutableData, Func>
    where
        Func: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Sync + Send,
    {
        ForEachBuilder {
            items: self.items,
            inner: self.inner,
            function,
            range: self.range,
            group_size: self.group_size,
            parallel: self.parallel,
        }
    }

    fn dispatch_parameters(&self) -> DispatchParameters {
        DispatchParameters::new(self.inner.context(), self.range.clone(), self.group_size, self.parallel)
    }
}

fn for_each<Item, ContextData, ImmutableData, F>(mut params: ForEachBuilder<Item, ContextData, ImmutableData, F>)
where
    F: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Sync + Send,
    Item: Sync + Send,
{
    profiling::scope!("for_each");

    unsafe {
        let first_item = params.range.start;
        let parallel = params.dispatch_parameters();

        let event = Event::new(params.range.end - params.range.start, params.inner.context().thread_pool_id());

        // Once we start converting this into jobrefs, it MUST NOT move or be destroyed until
        // we are done waiting on the event.
        let job_data: ArrayJob<Item, ContextData, ImmutableData, F> = ArrayJob::new(
            params.items,
            params.group_size,
            ConcurrentDataRef::from_ref(&mut params.inner),
            params.function,
            event,
        );

        let priority = params.inner.priority();
        let ctx = params.inner.context_mut();

        // This makes `parallel.range` items available for the thread pool to steal from us.
        // and wakes some workers up if need be.
        // The items from `params.range.start` to `parallel.range.start` are reserved. we will
        // execute them last without exposing them to the worker threads.
        for_each_dispatch(
            ctx,
            job_data.as_job_ref(priority),
            &parallel,
            params.group_size,
        );

        // Now the interesting bits: We kept around a range of items that for this thread to
        // execute, that workers threads cannot steal. The goal here is to make it as likely
        // as possible for this thread to be the last one finishing a job from this workload.
        // The reason is that if we run out of jobs to execute we have no other choice but to
        // block the thread on a condition variable, and that's bad for two reasons:
        //  - It's rather expensive.
        //  - There's extra latency on top of that from not necessarily being re-scheduled by the
        //    OS as soon as the condvar is signaled.

        // Pull work from our queue and execute it.
        while !job_data.event.is_signaled() && ctx.keep_busy() {}

        // Execute the reserved batch of items that we didn't make available to worker threads.
        // Doing this removes some potential for parallelism, but greatly increase the likelihood
        // of not having to block the thread so it usually a win.
        if parallel.range.start > first_item {
            profiling::scope!("mt:job group");
            let (context_data, immutable_data) = job_data.data.get(ctx);
            for item_index in first_item..parallel.range.start {
                let item = &mut params.items[item_index as usize];
                profiling::scope!("mt:job");
                let args = Args {
                    item,
                    item_index,
                    context_data,
                    immutable_data,
                };

                (job_data.function)(ctx, args);
            }

            job_data.event.signal(ctx, parallel.range.start - first_item);
        }

        // Hopefully by now all items have been processed. If so, wait will return quickly,
        // otherwise we'll have to block on a condition variable.
        ctx.wait(&job_data.event);
    }
}

unsafe fn for_each_dispatch(
    ctx: &mut Context,
    job_ref: JobRef,
    dispatch: &DispatchParameters,
    group_size: u32,
) {
    let mut actual_group_count = dispatch.group_count;
    for group_idx in 0..dispatch.group_count {
        let start = dispatch.range.start + dispatch.initial_group_size * group_idx;
        let end = (start + dispatch.initial_group_size).min(dispatch.range.end);

        // TODO: handle this in DispatchParameters::new
        if start >= end {
            actual_group_count -= 1;
            continue;
        }

        ctx.enqueue_job(job_ref.with_range(start..end, group_size));
    }

    // Waking up worker threads is the expensive part.
    // There's a balancing act between waking more threads now (which means they probably will get
    // to pick work up sooner but we spend time waking them up) or waking fewer of them.
    // Right now we wake at most half of the workers. Workers themselves will wake other workers
    // up if they have enough work.
    ctx.wake(actual_group_count.min((ctx.num_worker_threads() + 1) / 2));
}

#[derive(Debug)]
struct DispatchParameters {
    range: Range<u32>,
    group_count: u32,
    initial_group_size: u32,
}

impl DispatchParameters {
    fn new(ctx: &Context, item_range: Range<u32>, group_size: u32, mut parallel_ratio: Ratio) -> Self {
        if parallel_ratio.dividend == 0 {
            parallel_ratio.dividend = 1;
        }
        if parallel_ratio.divisor < parallel_ratio.dividend {
            parallel_ratio.divisor = parallel_ratio.dividend;
        }

        let n = item_range.end - item_range.start;

        let num_parallel = (parallel_ratio.dividend as u32 * n) / parallel_ratio.divisor as u32;
        let first_parallel = item_range.start + n - num_parallel;
        let group_count = div_ceil(num_parallel, group_size).min(ctx.num_worker_threads() * 2);
        let initial_group_size = if group_count == 0 { 0 } else { div_ceil(num_parallel, group_count) };

        DispatchParameters {
            range: first_parallel..item_range.end,
            group_count,
            initial_group_size,
        }
    }
}

/// A job that represents an array-like workload, for example updating a slice of items in parallel.
///
/// Once the workload starts, the object must NOT move or be dropped until the workload completes.
/// Typically this structure leaves on the stack if we know the workload to end within this stack
/// frame (see `for_each`) or on the heap otherwise.
struct ArrayJob<Item, ContextData, ImmutableData, Func> {
    items: *mut Item,
    data: ConcurrentDataRef<ContextData, ImmutableData>,
    function: Func,
    range: Range<u32>,
    split_thresold: u32,
    event: Event,
}

struct HeapArrayJob<Item, ContextData, ImmutableData, Func> {
    array_job: ArrayJob<Item, ContextData, ImmutableData, Func>,
    // Context and immtable data string references to maintain the memory
    // alive.
    #[allow(dead_code)] context_data: HeapContextData<ContextData>,
    #[allow(dead_code)] immutable_data: Option<Arc<ImmutableData>>,
    output: DataSlot<Vec<Item>>,
    strong_ref: Option<*const dyn RefCounted>,
}

struct DeferredArrayJob<Dep, Item, ContextData, ImmutableData, Func> {
    heap_job: UnsafeCell<HeapArrayJob<Item, ContextData, ImmutableData, Func>>,
    input: Dep,
    priority: Priority,
    group_size: u32,
}


impl<Item, ContextData, ImmutableData, Func> Job for HeapArrayJob<Item, ContextData, ImmutableData, Func>
where
    Func: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Send,
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, range: Range<u32>) {
        if execute_impl(&(*this).array_job, ctx, range) {
            if let Some(strong_ref) = &(*this).strong_ref {
                (&**strong_ref).release_ref();
            }
        }
    }
}

impl<Dep, Item, ContextData, ImmutableData, Func> Job for DeferredArrayJob<Dep, Item, ContextData, ImmutableData, Func>
where
    Dep: TaskDependency<Output = Vec<Item>>,
    Func: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Send,
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, _range: Range<u32>) {
        // Fetch the input from our dependency. We rely on being scheduled after
        // our dependency is done, and on having exclusive access to its output
        // for this to be safe.
        let mut items = (*this).input.get_output();
        assert!(items.len() <= Event::MAX_DEPENDECIES as usize);

        // We need to mutate the array_job to set some of the parameters, now that
        // we have the location and size of the workload.
        // It is safe because at this stage there is no other job pointing to this
        // memory location. It won't be safe to make new mutations during and after
        // the for_each_dispatch call.
        let heap_job: *mut _ = (*this).heap_job.get();
        let array_job: *mut _ = &mut (*heap_job).array_job;

        (*array_job).items = items.as_mut_ptr();

        // u32::MAX is the special value meaning we didn't specify a range.
        if (*array_job).range.start == std::u32::MAX {
            (*array_job).range.start = 0;
            (*array_job).range.end = items.len() as u32;
        } else {
            assert!((*array_job).range.end <= items.len() as u32);
        }

        // The event was initialized with u32::MAX dependency because we didn't
        // know when scheduling this what the size of the input would be, fix that
        // up now.
        let num_items = (*array_job).range.end - (*array_job).range.start;
        (*array_job).event.signal(ctx, Event::MAX_DEPENDECIES - num_items);

        // Store it directly in our output slot.
        (*heap_job).output.set(items);

        let dispatch = DispatchParameters::new(
            ctx,
            (*array_job).range.clone(),
            (*this).group_size,
            Ratio::ONE
        );

        for_each_dispatch(
            ctx,
            JobRef::new(&*heap_job).with_priority((*this).priority),
            &dispatch,
            (*this).group_size,
        );
    }
}

// Returns true if this execution was the one that put the vent in the signaled state.
unsafe fn execute_impl<I, CD, ID, F>(this: *const ArrayJob<I, CD, ID, F>, ctx: &mut Context, range: Range<u32>) -> bool
where
    F: Fn(&mut Context, Args<I, CD, ID>) + Send,

{
    let this: &ArrayJob<I, CD, ID, F> = mem::transmute(this);
    let n = range.end - range.start;

    assert!(range.start >= this.range.start && range.end <= this.range.end);

    let (context_data, immutable_data) = this.data.get(ctx);

    for item_idx in range {
        // SAFETY: The situation for the item pointer is the same as with context_data.
        // The pointer can be null, but when it is the case, the type is always ().
        let item = &mut *this.items.wrapping_offset(item_idx as isize);
        profiling::scope!("job");
        let args = Args {
            item,
            item_index: item_idx,
            context_data,
            immutable_data,
        };

        (this.function)(ctx, args);
    }

    this.event.signal(ctx, n)
}

impl<Item, ContextData, ImmutableData, Func> Job for ArrayJob<Item, ContextData, ImmutableData, Func>
where
    Func: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Send,
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, range: Range<u32>) {
        execute_impl(this, ctx, range);
    }
}

impl<Item, ContextData, ImmutableData, Func> ArrayJob<Item, ContextData, ImmutableData, Func>
where
    Func: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Send,
{
    pub unsafe fn new(
        items: &mut[Item],
        split_thresold: u32,
        data: ConcurrentDataRef<ContextData, ImmutableData>,
        function: Func,
        event: Event
    ) -> Self {
        ArrayJob {
            items: items.as_mut_ptr(),
            data,
            function,
            event,
            range: 0..(items.len() as u32),
            split_thresold,
        }
    }

    pub unsafe fn as_job_ref(&self, priority: Priority) -> JobRef {
        JobRef::new(self)
            .with_range(self.range.start .. self.range.end.max(1), self.split_thresold)
            .with_priority(priority)
    }
}

pub struct ForEachTaskBuilder<'l, Input, ContextData, ImmutableData> {
    builder: TaskBuilder<'l, Input, ContextData, ImmutableData>,
    range: Option<Range<u32>>,
    group_size: u32,
}

impl<'l, Dependency, ContextData, ImmutableData> ForEachTaskBuilder<'l, Dependency, ContextData, ImmutableData> {
    pub fn from(builder: TaskBuilder<'l, Dependency, ContextData, ImmutableData>) -> Self {
        ForEachTaskBuilder {
            builder,
            range: None,
            group_size: 1,
        }
    }

    #[inline]
    pub fn with_range(mut self, range: Range<u32>) -> Self {
        assert!(range.end >= range.start);
        self.range = Some(range);

        self
    }

    #[inline]
    pub fn with_group_size(mut self, group_size: u32) -> Self {
        self.group_size = group_size;

        self
    }

    pub fn run<F, Item>(self, function: F) -> OwnedHandle<Vec<Item>>
    where
        Dependency: TaskDependency<Output = Vec<Item>> + 'static,
        F: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Sync + Send + 'static,
        ContextData: 'static,
        ImmutableData: 'static,
        Item: 'static,
    {
        if self.builder.has_event_dependency() {
            self.schedule_after_dependency(function)
        } else {
            self.schedule_now(function)
        }
    }

    pub fn schedule_now<F, Item>(self, function: F) -> OwnedHandle<Vec<Item>>
    where
        Dependency: TaskDependency<Output = Vec<Item>>,
        F: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Sync + Send + 'static,
        ContextData: 'static,
        ImmutableData: 'static,
        Item: 'static,
    {
        unsafe {
            let (ctx, mut parameters) = self.builder.finish();
            let mut items = parameters.input.get_output();
            let range = self.range.unwrap_or(0..(items.len() as u32));
            assert!(range.end as usize <= items.len());

            let dispatch = DispatchParameters::new(ctx, range.clone(), self.group_size, Ratio::ONE);
            let priority = parameters.priority;

            let strong_ref: Option<*const dyn RefCounted> = None;
            let data = RefPtr::new(
                HeapArrayJob {
                    array_job: ArrayJob {
                        items: items.as_mut_ptr(),
                        range: 0..(items.len() as u32),
                        split_thresold: self.group_size,
                        data: ConcurrentDataRef::from_owned(&mut parameters, ctx),
                        function,
                        event: Event::new(range.end - range.start, ctx.thread_pool_id()),
                    },
                    output: DataSlot::new(),
                    context_data: parameters.context_data,
                    immutable_data: parameters.immutable_data,
                    // One reference for the AnyRefPtr and one that will be released by
                    // the last execute callback.
                    strong_ref,
                }
            );

            data.add_ref();
            (*RefPtr::mut_payload_unchecked(&data)).strong_ref = Some(RefPtr::as_raw(&data));

            // We can already place the item vector in the output slot even though the items will
            // be written to later during the execute callback.
            // This also serves the prupose of holding the vector.
            data.output.set(items);

            for_each_dispatch(
                ctx,
                JobRef::new(&data.array_job).with_priority(priority),
                &dispatch,
                self.group_size,
            );

            let event: *const Event = &data.array_job.event;
            let output: *const DataSlot<Vec<Item>> = &data.output;

            let data = data.into_any();

            OwnedHandle::new(data, event, output)
        }
    }

    pub fn schedule_after_dependency<F, Item>(self, function: F) -> OwnedHandle<Vec<Item>>
    where
        Dependency: TaskDependency<Output = Vec<Item>> + 'static,
        F: Fn(&mut Context, Args<Item, ContextData, ImmutableData>) + Sync + Send + 'static,
        ContextData: 'static,
        ImmutableData: 'static,
        Item: 'static,
    {
        unsafe {
            let (ctx, mut parameters) = self.builder.finish();
            // Since we don't know the size of the array yet, we
            // hack around it by treating u32::MAX as a special
            // value.
            let range = self.range.unwrap_or(std::u32::MAX..std::u32::MAX);
            let priority = parameters.priority;
            let strong_ref: Option<*const dyn RefCounted> = None;
            let data = RefPtr::new(DeferredArrayJob {
                heap_job: UnsafeCell::new(HeapArrayJob {
                    array_job: ArrayJob {
                        items: std::ptr::null_mut(),
                        range,
                        split_thresold: self.group_size,
                        data: ConcurrentDataRef::from_owned(&mut parameters, ctx),
                        function,
                        // Hack: we don't know yet how many items we will process so we initialize
                        // the event with the maximum number of dependencies and will adjust down
                        // during the setup task.
                        event: Event::new(Event::MAX_DEPENDECIES, ctx.thread_pool_id()),
                    },
                    output: DataSlot::new(),
                    context_data: parameters.context_data,
                    immutable_data: parameters.immutable_data,
                    // One reference for the AnyRefPtr and one that will be released by
                    // the last execute callback.
                    strong_ref,
                }),
                group_size: self.group_size,
                input: parameters.input,
                priority: parameters.priority,
            });

            let heap_job: *mut _ = (*RefPtr::mut_payload_unchecked(&data)).heap_job.get();
            data.add_ref();
            (*heap_job).strong_ref = Some(RefPtr::as_raw(&data));

            let event: *const Event = &(*data.heap_job.get()).array_job.event;
            let output: *const DataSlot<Vec<Item>> = &(*heap_job).output;

            // Schedule this task to run after its dependency is done.
            data.input
                .get_event()
                .unwrap()
                .then(ctx, JobRef::new(data.inner()).with_priority(priority));

            let data = data.into_any();

            OwnedHandle::new(data, event, output)
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Ratio {
    pub dividend: u8,
    pub divisor: u8,
}

impl Ratio {
    pub const DEFAULT: Ratio = Ratio { dividend: 4, divisor: 5 };
    pub const ONE: Ratio = Ratio { dividend: 1, divisor: 1 };
    pub const HALF: Ratio = Ratio { dividend: 1, divisor: 2 };
    pub const THREE_QUARTERS: Ratio = Ratio { dividend: 3, divisor: 4 };
    pub const TWO_THIRDS: Ratio = Ratio { dividend: 2, divisor: 3 };
    pub const ONE_THIRD: Ratio = Ratio { dividend: 1, divisor: 3 };
}

fn div_ceil(a: u32, b: u32) -> u32 {
    let d = a / b;
    let r = a % b;
    if r > 0 && b > 0 { d + 1 } else { d }
}

#[test]
fn test_heap_for_each() {
    use crate::ThreadPool;

    let pool = ThreadPool::builder().with_worker_threads(3).build();

    let mut ctx = pool.pop_context().unwrap();

    let mut items1 = vec![0u32; 8192];
    let mut items2 = vec![0u32; 8192];
    let mut items3 = vec![0u32; 8192];

    for i in 0..3000 {
        use std::mem::take;
        let handle_1 = ctx.task()
            .with_data(take(&mut items1))
            .for_each()
            .with_group_size(16)
            .run(|_, mut item| { *item += 1; });
        let handle_2 = ctx.task()
            .with_data(take(&mut items2))
            .for_each()
            .with_group_size(16)
            .run(|_, mut item| { *item += 1; });
        let handle_3 = ctx.task()
            .with_data(take(&mut items3))
            .for_each()
            .with_group_size(16)
            .run(|_, mut item| { *item += 1; });

        items1 = handle_1.resolve(&mut ctx);
        items2 = handle_2.resolve(&mut ctx);
        items3 = handle_3.resolve(&mut ctx);
        for item in &items1 {
            assert_eq!(*item, i + 1); // left: 0, right: 1625
        }
        for item in &items2 {
            assert_eq!(*item, i + 1);
        }
        for item in &items3 {
            assert_eq!(*item, i + 1);
        }
    }

    pool.shut_down().wait();
    // double-free vec (easier to reproduce when CPU under load)
}

#[test]
fn test_heap_range_for_each() {
    use crate::ThreadPool;

    let pool = ThreadPool::builder().with_worker_threads(3).build();

    let mut ctx = pool.pop_context().unwrap();

    let w = ctx.task()
        .with_immutable_data(Arc::new(1234u32))
        .range_for_each(20..100)
        .run(|_, args| {
            assert!(args.item_index >= 20, "Error: {} should be in 20..100", args.item_index);
            assert!(args.item_index < 100, "Error: {} should be in 20..100", args.item_index);
            assert_eq!(args.immutable_data, &1234);
        });

    w.wait(&mut ctx);

    pool.shut_down().wait();
}

#[test]
fn test_simple_for_each() {
    use crate::ThreadPool;
    use std::sync::atomic::{AtomicU32, Ordering};
    static INITIALIZED_WORKERS: AtomicU32 = AtomicU32::new(0);
    static SHUTDOWN_WORKERS: AtomicU32 = AtomicU32::new(0);

    let pool = ThreadPool::builder()
        .with_worker_threads(3)
        .with_contexts(1)
        .with_start_handler(|_id| { INITIALIZED_WORKERS.fetch_add(1, Ordering::SeqCst); })
        .with_exit_handler(|_id| { SHUTDOWN_WORKERS.fetch_add(1, Ordering::SeqCst); })
        .build();

    let mut ctx = pool.pop_context().unwrap();

    for _ in 0..200 {
        let input = &mut [0i32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let worker_data = &mut [0i32, 0, 0, 0];

        ctx.for_each(input)
            .with_context_data(worker_data)
            .run(|ctx, args| {
                let _v: i32 = *args;
                *args.context_data += 1;
                *args.item *= 2;
                //println!(" * worker {:} : {:?} * 2 = {:?}", ctx.id(), _v, item);

                for i in 0..10 {
                    let priority = if i % 2 == 0 { Priority::High } else { Priority::Low };
                    let nested_input = &mut [0i32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
                    ctx.with_priority(priority)
                        .for_each(nested_input)
                        .with_range(3..14)
                        .run(|_, mut args| { *args += 1; });

                    for item in &nested_input[0..3] {
                        assert_eq!(*item, 0);
                    }
                    for item in &nested_input[3..14] {
                        assert_eq!(*item, 1);
                    }
                    for item in &nested_input[14..16] {
                        assert_eq!(*item, 0);
                    }

                    for j in 0..100 {
                        let priority = if j % 2 == 0 { Priority::High } else { Priority::Low };
                        let nested_input = &mut [0i32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
                        ctx.for_each(nested_input)
                            .with_priority(priority)
                            .run(|_, mut item| { *item += 1; });
                        for item in nested_input {
                            let item = *item;
                            assert_eq!(item, 1);
                        }
                    }
                }
            });

        assert_eq!(input, &[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30]);
    }

    let handle = pool.shut_down();
    handle.wait();

    assert_eq!(INITIALIZED_WORKERS.load(Ordering::SeqCst), 3);
    assert_eq!(SHUTDOWN_WORKERS.load(Ordering::SeqCst), 3);
}

#[test]
fn test_few_items() {
    use crate::ThreadPool;

    let pool = ThreadPool::builder().with_worker_threads(3).build();
    let mut ctx = pool.pop_context().unwrap();
    let ctx_data = ctx.create_context_data(vec![(); 10]);
    for _ in 0..100 {
        for n in 0..8 {
            let mut input = vec![0i32; n];

            ctx.for_each(&mut input)
                .with_context_data(&mut [0u32, 0, 0, 0])
                .run(|_, mut item| { *item += 1; });

            let handle = ctx.task()
                .with_context_data(ctx_data.clone())
                .with_data(input)
                .for_each()
                .run(|_, mut item| { *item += 1; });
            let input = handle.resolve(&mut ctx);

            for val in &input {
                assert_eq!(*val, 2);
            }
        }
    }
}

#[test]
fn test_heap_for_each_dependencies() {
    let pool = crate::ThreadPool::builder()
    .with_worker_threads(2)
    .with_contexts(1)
    .build();

    let mut ctx = pool.pop_context().unwrap();

    let t1 = ctx.task().run(|_, _| { vec![10u32; 1024] });
    let t2 = ctx.task().with_input(t1).for_each().run(|_, mut item| *item += 1);
    let result = t2.resolve(&mut ctx);

    assert_eq!(result, vec![11u32; 1024]);

    // Some test but producing/consuming an empty array.
    let t1 = ctx.task().run(|_, _| { Vec::<u32>::new() });
    let t2 = ctx.task()
        .with_input(t1)
        .for_each()
        .run(|_, mut item| *item += 1);
    let result = t2.resolve(&mut ctx);

    assert_eq!(result, Vec::new());

    pool.shut_down().wait();
}

#[cfg(loom)]
#[test]
fn test_loom_for_each() {
    // TODO: this test deadlocks during shutdown, likely because crossbeam_utils::Parker
    // isn't part of loom's visible model. When shutdown starts we first store a shutdown
    // flag (SeqCst) and unpark the worker threads. When executing with loom we can see the
    // worker unparking but they don't see the flag (they should).
    // I've tried to compile crossbeam utils with the crossbeam_loom cfg but the build fails
    // all over the place.

    loom::model(move || {
        let pool = crate::ThreadPool::builder()
            .with_worker_threads(2)
            .with_contexts(1)
            .build();

        let mut ctx = pool.pop_context().unwrap();

        let input = &mut [0i32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let ctx_data = &mut [0i32, 0, 0];

        ctx.for_each(input)
            .with_context_data(ctx_data)
            .run(|_, args| {
                *args.context_data += 1;
                *args.item *= 2;
            });

        assert_eq!(input, &[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30]);
        assert_eq!(ctx_data[0] + ctx_data[1] + ctx_data[2], 16);

        pool.recycle_context(ctx);
    });
}
