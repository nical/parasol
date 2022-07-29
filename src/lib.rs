//! An experimental parallel job scheduler with the goal of doing better than rayon
//! specifically in the types of workloads we have in Firefox.
//!
//! What we want:
//! - Allow running jobs outside of the thread pool.
//! - Avoid blocking the thread that submits the work if possible.
//! - No implicit global thread pool.
//! - Ways to safely manage per-worker data.
//! - Avoid hoarding CPU resources in worker threads that don't have work to execute (this
//!   is at the cost of higher latency).
//! - No need to scale to a very large number of threads. We prefer to have something that
//!   runs efficiently on up to 8 threads and not need to scale well above that.


// TODO: handle panics in worker threads
// TODO: everywhere we take a context as parameter there should be a check that the context
//       belongs to the same thread pool.

mod core;
mod array;
mod join;
pub mod util;
mod task;
pub mod helpers;
pub mod handle;
pub mod ref_counted;

pub use array::ForEachBuilder;
pub use join::*;
pub use crate::core::job::Priority;
pub use crate::core::context::*;
pub use crate::core::event::Event;
pub use crate::core::thread_pool::{ThreadPool, ThreadPoolId, ThreadPoolBuilder};
pub use crate::core::shutdown::ShutdownHandle;
pub use crate::core::sync;

pub use crossbeam_utils::CachePadded;

/*

TaskData<I, O> {
    output: Option<O>,
    event: Event,
}

TaskHandle<O> {
    output: *mut Option<O>,
    event: *mut Event,
}



let handle = ctx.job().run(|ctx, data| {
    let task = ctx.for_each(&data.elements).run(|ctx, args| {
        // ...
    });

    task.run_async();

    data
})

let data = handle.wait();



*/