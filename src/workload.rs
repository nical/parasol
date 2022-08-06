//! Dynamic workloads
//!
//! This is an experiment for array-like workloads where the number of items can grow while
//! the workload runs.
//!
//! The main ideas are: 
//!
//! - The storage for items is pre-determine and the workload cannot go process more than
//!   that initial limit (so that it can allocate storage once and never needs to
//!   reallocate).
//! - The even is initialized with the maximum possible number of items, and when finishing
//!   the workload, it is signaled with the remaining number of items.

use aliasable::boxed::AliasableBox;
use std::ops::Range;
use std::mem::ManuallyDrop;

use crate::core::job::{Job, JobRef};
use crate::core::event::{Event};
use crate::{Context, Priority};

pub struct ParamBuffer<T> {
    data: *mut T,
    cap: usize,
    range: Range<usize>,
}

impl<T: Clone + Default> ParamBuffer<T> {
    pub fn new(size: usize) -> Self {
        let mut slice = ManuallyDrop::new(vec![T::default(); size].into_boxed_slice());
        let data = slice.as_mut_ptr();
        let cap = slice.len();

        ParamBuffer { data, cap, range: 0..0 }
    }
}

impl<T> ParamBuffer<T> {
    pub fn push(&mut self, payload: T) -> Option<usize> {
        if self.range.end == self.cap {
            return None;
        }

        let idx = self.range.end;
        self.range.end += 1;

        unsafe {
            *self.data.add(idx) = payload;
        }

        Some(idx)
    }

    pub fn current_range(&self) -> &Range<usize> {
        &self.range
    }

    pub fn remaining_capacity(&self) -> usize {
        self.cap - self.range.end
    }

    pub fn unsafe_ref(&self, range: Range<usize>) -> UnsafeParamBufferRef<T> {
        assert!(range.start <= range.end);
        assert!(range.end <= self.range.end);

        unsafe {
            let ptr = self.data.add(range.start);
            let len = range.end - range.start;
            UnsafeParamBufferRef { ptr, len }
        }
    }

    pub fn reset_cursor(&mut self) {
        self.range.start = 0;
        self.range.end = 0;
    }

    pub fn as_ptr(this: *const Self) -> *const T {
        unsafe {
            (*this).data
        }
    }

    pub fn writer(&mut self) -> ParamBufferWriter<T> {
        ParamBufferWriter { inner: self as *mut ParamBuffer<T> }
    }
}

impl<T> Drop for ParamBuffer<T> {
    fn drop(&mut self) {
        unsafe {
            // we made sure it was Ok to consider that cap == length by
            // using into_boxed_slice in the constructor.
            let _ = Vec::from_raw_parts(self.data, self.cap, self.cap);
        }
    }
}

pub struct ParamBufferWriter<T> {
    inner: *mut ParamBuffer<T>,
}

impl<T> ParamBufferWriter<T> {
    pub unsafe fn push(&mut self, payload: T) -> Option<usize> {
        if (*self.inner).range.end == (*self.inner).cap {
            return None;
        }

        let idx = (*self.inner).range.end;
        (*self.inner).range.end += 1;

        *(*self.inner).data.add(idx) = payload;

        Some(idx)
    }

    pub unsafe fn flush_range(&mut self) -> Range<usize> {
        let range = (*self.inner).range.clone();
        (*self.inner).range.start = (*self.inner).range.end;

        range    
    }

    pub unsafe fn reset_cursor(&mut self) {
        (*self.inner).range.start = 0;
        (*self.inner).range.end = 0;
    }
}

pub struct UnsafeParamBufferRef<T> {
    ptr: *const T,
    len: usize,
}

impl<T> UnsafeParamBufferRef<T> {
    pub unsafe fn as_slice(&self) -> &[T] {
        std::slice::from_raw_parts(self.ptr, self.len)
    }
}

pub struct Workload<T, F> {
    data: AliasableBox<WorkloadData<T, F>>,
    // Keeping a second reference to data.jobs via the write looks like it is redundant,
    // however it makes it easier to get miri's blessing. The reason is that we will
    // need to make some mutations in the param buffer, and we must not make them through
    // a `&mut ParamBuffer<T>` pointer while some jobs are in flight, otherwise it invalidates
    // the entire 
    job_writer: ParamBufferWriter<T>,
}

pub struct WorkloadData<T, F> {
    jobs: ParamBuffer<T>,
    event: Event,
    function: F,
    priority: Priority,
}

impl<T: Clone + Default, F> Workload<T, F> 
where F: Fn(&mut Context, &T) + Send
{
    pub fn new(max_items: u32, ctx: &Context, function: F) -> Self {
        assert!(max_items <= Event::MAX_DEPENDECIES);
        let mut data = AliasableBox::from_unique(Box::new(WorkloadData {
            jobs: ParamBuffer::new(max_items as usize),
            event: Event::new(max_items, ctx.thread_pool_id()),
            function,
            priority: Priority::High,
        }));
        let job_writer = data.jobs.writer();
        Workload {
            data,
            job_writer,
        }
    }
}

impl<T, F> Workload<T, F> 
where F: Fn(&mut Context, &T) + Send
{
    pub fn push(&mut self, item: T) -> bool {
        unsafe {
            self.job_writer.push(item).is_some()
        }
    }

    pub fn flush(&mut self, ctx: &mut Context) {
        unsafe {
            let range = self.job_writer.flush_range();
            if range.end == range.start {
                return;
            }

            let job = JobRef::new(&*self.data)
                .with_range(range.start as u32 .. range.end as u32, 8)
                .with_priority(self.data.priority);

            ctx.schedule_job(job)
        }
    }

    pub fn finish(&mut self, ctx: &mut Context) {
        self.flush(ctx);
        unsafe {
            let n = self.data.jobs.remaining_capacity() as u32;
            Event::signal_ptr(&(*self.data).event, ctx, n);
            self.data.event.wait(ctx);
            self.job_writer.reset_cursor();

            let deps = self.data.jobs.remaining_capacity() as u32;
            self.data.event.reset(deps);
            // This seems redundant since the buffer's address hasn't changed, but
            // calling event.reset above has invalidated our *mut pointer to the
            // param buffer contained in data.
            self.job_writer = self.data.jobs.writer();
        }
    }
}

impl<T, F> Job for WorkloadData<T, F>
where
    F: Fn(&mut Context, &T) + Send
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, range: Range<u32>) {
        let n = range.end - range.start;
        let ptr: *const T = (*this).jobs.data;
        for idx in range {
            let input: &T = std::mem::transmute(ptr.add(idx as usize));

            ((*this).function)(ctx, input);
        }

        Event::signal_ptr(&(*this).event, ctx, n);
    }
}

impl<T, F> Drop for Workload<T, F> {
    fn drop(&mut self) {
        // TODO: ideally we would like to just signal the remaining capacity in the event
        // but we need a context for that.
        assert_eq!(self.data.jobs.current_range().end, 0, "Dropping a running workload");
        unsafe {
            let jobs = Event::signal2(&self.data.event, self.data.jobs.remaining_capacity() as u32);
            assert!(jobs.unwrap().is_empty());
        }
    }
}

#[test]
fn test_workload() {
    use std::sync::atomic::{Ordering, AtomicI32};
    use std::sync::Arc;

    use crate::ThreadPool;
    let pool = ThreadPool::builder()
        .with_worker_threads(3)
        .with_contexts(1)
        .build();

    let mut ctx = pool.pop_context().unwrap();

    let counter: Arc<AtomicI32> = Arc::new(AtomicI32::new(0));
    let result = counter.clone();

    let mut workload = Workload::new(2048, &mut ctx, move|_ctx, _val: &u32| {
        //println!("  - run {:?}", _val);
        counter.fetch_add(1, Ordering::Relaxed);
    });

    for _ in 0..3 {
        let a = 10;
        let b = 200;
        for i in 0..a {
            //println!("- wave {:?}", i);
            for j in 0..b {
                workload.push(j + i * 1000);
            }
            //println!("- flush wave {:?}", i);
            workload.flush(&mut ctx);
        }

        //println!("- finish");
        workload.finish(&mut ctx);
        //println!("- done");

        assert_eq!(result.load(Ordering::SeqCst), (a * b) as i32);
        result.store(0, Ordering::SeqCst);
    }

    let _dummy1 = Workload::new(2048, &mut ctx, move|_ctx, _val: &u32| {});

    let mut dummy2 = Workload::new(2048, &mut ctx, move|_ctx, _val: &u32| {});
    dummy2.finish(&mut ctx);

    pool.shut_down().wait();
}
