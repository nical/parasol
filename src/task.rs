use crate::core::event::{Event};
use crate::core::job::{Job, JobRef};
use crate::{Context, Priority};
use crate::helpers::*;
use crate::handle::*;
use crate::array::ForEachTaskBuilder;
use crate::ref_counted::{RefPtr, RefCounted, InlineRefCounted};
use crate::sync::Arc;

use std::mem;
use std::ops::{Range};

pub trait TaskDependency {
    type Output;
    fn get_output(&self) -> Self::Output;
    fn get_event(&self) -> Option<&Event>;
}

/// Similar to `Parameters`, but owns its data.and does not hold a reference to the context.
pub struct TaskParameters<Input, ContextData, ImmutableData> {
    pub context_data: HeapContextData<ContextData>,
    pub immutable_data: Option<Arc<ImmutableData>>,
    pub priority: Priority,
    pub input: Input,
}

pub struct TaskBuilder<'l, Input, ContextData, ImmutableData> {
    context_data: HeapContextData<ContextData>,
    immutable_data: Option<Arc<ImmutableData>>,
    priority: Priority,
    input: Input,
    ctx: &'l mut Context,
}

pub fn task_builder(ctx: &mut Context) -> TaskBuilder<(), (), ()> {
    TaskBuilder {
        context_data: heap_context_data(),
        immutable_data: None,
        priority: Priority::High,
        input: (),
        ctx,
    }
}

impl<'l, Input, ContextData, ImmutableData> TaskBuilder<'l, Input, ContextData, ImmutableData> {
    #[inline]
    pub fn with_context_data<T>(self, data: HeapContextData<T>) -> TaskBuilder<'l, Input, T, ImmutableData> {
        TaskBuilder {
            context_data: data,
            immutable_data: self.immutable_data,
            priority: self.priority,
            input: self.input,
            ctx: self.ctx,
        }
    }

    #[inline]
    pub fn with_immutable_data<T>(self, data: Arc<T>) -> TaskBuilder<'l, Input, ContextData, T> {
        TaskBuilder {
            context_data: self.context_data,
            immutable_data: Some(data),
            priority: self.priority,
            input: self.input,
            ctx: self.ctx,
        }
    }

    #[inline]
    pub fn with_input<Dep: TaskDependency>(self, dep: Dep) -> TaskBuilder<'l, Dep, ContextData, ImmutableData> {
        TaskBuilder {
            context_data: self.context_data,
            immutable_data: self.immutable_data,
            priority: self.priority,
            input: dep,
            ctx: self.ctx,
        }
    }

    #[inline]
    pub fn with_data<T>(self, data: T) -> TaskBuilder<'l, DataSlot<T>, ContextData, ImmutableData> {
        TaskBuilder {
            context_data: self.context_data,
            immutable_data: self.immutable_data,
            priority: self.priority,
            input: DataSlot::from(data),
            ctx: self.ctx,
        }
    }

    #[inline]
    pub fn after(self, handle: Handle) -> TaskBuilder<'l, Handle, ContextData, ImmutableData> {
        TaskBuilder {
            context_data: self.context_data,
            immutable_data: self.immutable_data,
            priority: self.priority,
            input: handle,
            ctx: self.ctx,
        }
    }

    #[inline]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;

        self
    }

    pub fn for_each(self) -> ForEachTaskBuilder<'l, Input, ContextData, ImmutableData> {
        ForEachTaskBuilder::from(self)
    }

    #[inline]
    pub fn priority(&self) -> Priority {
        self.priority
    }

    #[inline]
    pub fn context_data(&mut self) -> &mut HeapContextData<ContextData> {
        &mut self.context_data
    }

    #[inline]
    pub fn has_event_dependency(&self) -> bool where Input: TaskDependency {
        self.input.get_event().is_some()
    }

    #[inline]
    pub fn finish(self) -> (&'l mut Context, TaskParameters<Input, ContextData, ImmutableData>) {
        (
            self.ctx,
            TaskParameters {
                input: self.input,
                context_data: self.context_data,
                immutable_data: self.immutable_data,
                priority: self.priority,
            }
        )
    }
}

impl<'l, ContextData, ImmutableData> TaskBuilder<'l, (), ContextData, ImmutableData> {
    pub fn range_for_each(self, range: std::ops::Range<u32>) -> ForEachTaskBuilder<'l, DataSlot<Vec<()>>, ContextData, ImmutableData> {
        self.with_data(vec![(); range.end as usize])
            .for_each()
            .with_range(range)
    }
}

pub struct Args<'l, Input, ContextData, ImmutableData> {
    pub input: Input,
    pub context_data: &'l mut ContextData,
    pub immutable_data: &'l ImmutableData,
}

impl<Output, ContextData, ImmutableData, Dependency, Function> Job for InlineRefCounted<TaskJobData<Output, ContextData, ImmutableData, Dependency, Function>>
where
    Dependency: TaskDependency,
    Function: Fn(&mut Context, Args<Dependency::Output, ContextData, ImmutableData>) -> Output + Send,
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, range: Range<u32>) {
        TaskJobData::execute((*this).inner() as *const _, ctx, range);
        (*this).release_ref();
    }
}

impl<'l, Dependency, ContextData, ImmutableData> TaskBuilder<'l, Dependency, ContextData, ImmutableData> {
    #[inline]
    pub fn run<F, Output>(self, function: F) -> OwnedHandle<Output>
    where
        Dependency: TaskDependency + 'static,
        F: Fn(&mut Context, Args<Dependency::Output, ContextData, ImmutableData>) -> Output + Send + 'static,
        Output: Send + 'static,
        ContextData: 'static,
        ImmutableData: 'static,
    {
        let (ctx, mut parameters) = self.finish();
        let priority = parameters.priority;
        unsafe {

            let task_job: RefPtr<TaskJobData<Output, ContextData, ImmutableData, Dependency, F>> = RefPtr::new(
                TaskJobData {
                    data: ConcurrentDataRef::from_owned(&mut parameters, ctx),
                    parameters,
                    function,
                    output: DataSlot::new(),
                    event: Event::new(1, ctx.thread_pool_id()),
                }
            );

            // Add a self-reference that will be removed after executing the job.
            task_job.add_ref();

            let event: *const Event = &task_job.event;
            let output: *mut DataSlot<Output> = mem::transmute(&task_job.output);

            let job_ref = JobRef::new(task_job.inner()).with_priority(priority);

            if let Some(evt) = task_job.parameters.input.get_event() {
                evt.then(ctx, job_ref);
            } else {
                ctx.schedule_job(job_ref);
            }

            OwnedHandle::new(
                task_job.into_any(),
                event,
                output,
            )
        }
    }

}

struct TaskJobData<Output, ContextData, ImmutableData, Dependency, F> {
    #[allow(dead_code)] // Not really dead code, needed to keep the strong references alive.
    parameters: TaskParameters<Dependency, ContextData, ImmutableData>,
    function: F,
    data: ConcurrentDataRef<ContextData, ImmutableData>,
    output: DataSlot<Output>,
    event: Event,
}

impl<Output, ContextData, ImmutableData, Dependency, Func> Job for TaskJobData<Output, ContextData, ImmutableData, Dependency, Func>
where
    Dependency: TaskDependency,
    Func: Fn(&mut Context, Args<Dependency::Output, ContextData, ImmutableData>) -> Output + Send,
{
    unsafe fn execute(this: *const Self, ctx: &mut Context, _range: Range<u32>) {
        let (context_data, immutable_data) = (*this).data.get(ctx);
        (*this).output.set(((*this).function)(ctx, Args {
            input: (*this).parameters.input.get_output(),
            context_data,
            immutable_data,
        }));
        (*this).event.signal(ctx, 1);
    }
}

#[test]
fn simple_task() {
    use crate::sync::{Arc, AtomicI32, Ordering};
    use crate::ThreadPool;
    let pool = ThreadPool::builder()
        .with_worker_threads(3)
        .with_contexts(1)
        .build();

    let mut ctx = pool.pop_context().unwrap();

    let mut handles: Vec<OwnedHandle<u32>> = Vec::new();
    for _ in 0..100_000 {
        handles.push(
            ctx.task().run(|_ctx, _args| { 1u32 + 1 })
        );
    }

    for handle in handles {
        assert_eq!(handle.resolve(&mut ctx), 2);
    }

    // Task t1 produces an output which becomes which is the input of task t2.
    let input: f32 = 1.0;
    let t1 = ctx.task().with_data(input).run(|_, args| { args.input + 1.0 });
    let t2 = ctx.task().with_input(t1).run(|_, args| {
        args.input as u32 + 1
    });
    assert_eq!(t2.resolve(&mut ctx), 3);

    for _ in 0..100_000 {
        ctx.task().run(|_ctx, _args| { 1u32 + 1 });
    }

    let counter = Arc::new(AtomicI32::new(0));
    let c3 = counter.clone();
    let t3 = ctx.task().run(move|_, _| { c3.fetch_add(1, Ordering::Release)});
    let t3_done = t3.handle();

    let c = counter.clone();
    ctx.task().after(t3_done.clone()).run(move |_,_| { assert_eq!(c.load(Ordering::Acquire), 1); });
    let c = counter.clone();
    ctx.task().after(t3_done.clone()).run(move |_,_| { assert_eq!(c.load(Ordering::Acquire), 1); });
    let c = counter.clone();
    ctx.task().after(t3_done.clone()).run(move |_,_| { assert_eq!(c.load(Ordering::Acquire), 1); });

    pool.shut_down().wait();
}
