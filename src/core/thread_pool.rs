use super::{Shared, WorkerHook};
use super::sync::Arc;
use super::context::{Context, ContextPool};
use super::shutdown::{Shutdown, ShutdownHandle};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ThreadPoolId(pub(crate) u32);

/// A reference to a thread pool.
#[derive(Clone)]
pub struct ThreadPool {
    pub(crate) shared: Arc<Shared>,
}

impl ThreadPool {
    pub fn builder() -> ThreadPoolBuilder {
        ThreadPoolBuilder {
            num_threads: 3,
            num_contexts: 1,
            start_handler: None,
            exit_handler: None,
            name_handler: Box::new(|idx| format!("Worker#{}", idx)),
            stack_size: None,
        }
    }

    pub fn shut_down(&self) -> ShutdownHandle {
        Shutdown::begin_shut_down(Arc::clone(&self.shared))
    }

    pub fn pop_context(&self) -> Option<Context> {
        ContextPool::pop(Arc::clone(&self.shared))
    }

    pub fn recycle_context(&self, ctx: Context) {
        assert_eq!(ctx.shared.id, self.shared.id);
        self.shared.context_pool.recycle(ctx);
    }

    pub fn id(&self) -> ThreadPoolId {
        self.shared.id
    }

    pub fn num_worker_threads(&self) -> u32 { self.shared.num_workers }

    pub fn num_contexts(&self) -> u32 { self.shared.num_contexts }
}

pub struct ThreadPoolBuilder {
    pub(crate) num_threads: u32,
    pub(crate) num_contexts: u32,
    pub(crate) start_handler: Option<Box<dyn WorkerHook>>,
    pub(crate) exit_handler: Option<Box<dyn WorkerHook>>,
    pub(crate) name_handler: Box<dyn Fn(u32) -> String>,
    pub(crate) stack_size: Option<usize>,
}

impl ThreadPoolBuilder {
    pub fn with_start_handler<F>(self, handler: F) -> Self
    where F: Fn(u32) + Send + Sync + 'static
    {
        ThreadPoolBuilder {
            num_threads: self.num_threads,
            num_contexts: self.num_contexts,
            start_handler: Some(Box::new(handler)),
            exit_handler: self.exit_handler,
            name_handler: self.name_handler,
            stack_size: self.stack_size,
        }
    }

    pub fn with_exit_handler<F>(self, handler: F) -> Self
    where F: Fn(u32) + Send + Sync + 'static
    {
        ThreadPoolBuilder {
            num_threads: self.num_threads,
            num_contexts: self.num_contexts,
            start_handler: self.start_handler,
            exit_handler: Some(Box::new(handler)),
            name_handler: self.name_handler,
            stack_size: self.stack_size,
        }
    }

    pub fn with_thread_names<F>(self, handler: F) -> Self
    where F: Fn(u32) -> String + 'static
    {
        ThreadPoolBuilder {
            num_threads: self.num_threads,
            num_contexts: self.num_contexts,
            start_handler: self.start_handler,
            exit_handler: self.exit_handler,
            name_handler: Box::new(handler),
            stack_size: self.stack_size,
        }
    }

    pub fn with_worker_threads(mut self, num_threads: u32) -> Self {
        self.num_threads = num_threads.max(1);

        // We are currently limited to 32 workers, and that should be fine.
        assert!(self.num_threads < 32);
        // We are also currently limited to a total of 32 contexts including workers.
        // this might be more problematic in practice. If so the changes that need to
        // happen are in the Activity struct in core.rs.
        assert!(self.num_threads + self.num_contexts < 32);

        self
    }

    pub fn with_contexts(mut self, num_contexts: u32) -> Self {
        self.num_contexts = num_contexts.max(1);

        // We are currently limited to 32 workers, and that should be fine.
        assert!(self.num_threads < 32);
        // We are also currently limited to a total of 32 contexts including workers.
        // this might be more problematic in practice. If so the changes that need to
        // happen are in the Activity struct in core.rs.
        assert!(self.num_threads + self.num_contexts < 32);

        self
    }

    pub fn with_stack_size(mut self, size: usize) -> Self {
        self.stack_size = Some(size);

        self
    }

    pub fn build(self) -> ThreadPool {
        crate::core::init(self)
    }
}
