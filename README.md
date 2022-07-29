# Parallel job scheduler

This crate contains a work in progress parallel job scheduler.


# Example


```rust
    let pool = ThreadPool::builder()
        .with_worker_threads(3)
        .with_contexts(1)
        .build();

    let mut ctx = pool.pop_context().unwrap();

    let mut input = vec![0u32; 10000];

    // Say our parallel computation requires a temporary vector and allocating a new one each time is
    // too expensive. In single-threaded code it's usually easy to keep the vector around an reuse it.
    // Here we will use some "per-context data" which is guaranteed to never be used concurrently.
    // For better performance a real-world use case would add some padding to avoid false-cache-sharing
    let mut per_context_data: Vec<Vec<u32>> = vec![Vec::new(), ctx.num_contexts() as usize];

    // This method schedules processing each element in `input` in parallel.
    ctx.for_each(&mut input)
        .with_context_data(&mut per_context_data)
        .group_size(10) // Don't bother splitting workloads into chunks smaller than 10 items.
        .run(|ctx, args| {
            some_compuation(args.item, args.context_data);
        });

    fn some_compuation(item: &mut u32, tmp: &mut Vec<u32>) {
        tmp.clear();
        // ...
    }
```

# Notes

## Overall design

- A parallel job scheduler based on work-stealing via Chase-Lev queues.
- All of the scheduling and execution of work is done via `Context` objects. Each worker thread owns a context, other threads may also have access to contexts in order to schedule work. There is a fixed number of contexts specified when initializing the thread pool and contexts can move between threads but can't be used by multiple threads at the same time (unless there is external synchronization). For the API this is a pretty big constraint, however this is also key to the performance of this system.
- To make a job available to the thread pool, a job must be inserted into a context queue.
- Contexts can participate in the work that they submit. This allows a submitter thread to complete work itself if the thread pool (or the CPU) is too busy to pick work up.
- Worker contexts can steal work, but non-worker contexts cannot (they only have access to their own queue). This is so that a job can be only processed by either a worker thread or the submitting thread instead of potentially any thread that owns a context. The reason we want this is for tasks like glyph rasterization in WebRender where we need to create a font context and register all fonts for each thread that may rasterize some glyphs so we want to keep this number under control. An argument can also be made that we don't want a thread to start processing a potentially heavy job from another thread's workload while waiting for something more important to complete, but that depends on how evenly distributed the cost of jobs is.
- Each context has two queues: high and low priority.
- Avoid blocking synchronization primitives using atomics to count remaining work and having thread pariticpate in the workloads they are waiting for as much as possible, to avoid the cost of the blocking primitive and the latency from putting the thread back to sleep and waking it back up.
- Avoid spinning in worker threads. If a worker can't find useful work to do it goes to sleep immediately. Depending on the application this may or may not be a good thing, for Gecko it is because the browser has many other threads with useful things to do.


## Understanding the code

At the core of this system, the most important parts are:
 - The main API entry point is `Context` objects.
 - All of the shared state is centralized into the `core::Shared` structure which is broken up into a few parts that each handle a specific piece of the logic (for example `core::Sleep`, `core::Activity`, `core::Stealers`, `core::Shutdown`, etc.).
 - The main tool for synchronization is `event::Event`. Use it to ensure control doesn't continue until a given number of dependencies are completed. Typically used when a thread will schedule the execution of N units of work and wants to wait until that work (the N dependencies) is done. From a synchronization standpoint `Event` is conceptually a condition variable around a dependency counter. However, `Event`'s raison d'être is to avoid blocking the current thread and instead use it to process jobs until dependencies are met. `Event` can also be used to immediately schedule followup jobs once all dependencies are met.

The pieces above are fairly "low level" but they are enough to efficiently schedule, execute and synchronize multi-threaded workloads. `Event` is typically not seen directly by users of the scheduler. Instead, higher-level cencepts are built on top of it, for example the parallel `for_each` which provides nice and safe APIs to execute a callback on a slice or vector of inputs (see `array.rs`). It could probably live in its own crate.

If we were to add, say, a parallel DAG processing abstraction, it would be implemented on top of the same building blocks as the parallel `for_each` implementation.

To summarize, at the lower level we have the main building blocks:
 - Most of the job scheduling logic living in `context.rs` and `core.rs`
 - The main synchronization logic in `event.rs`

And on top of the building blocks, at a higher level:
 - specific APIs geared towards processing workloads of certain types, like a parallel for-each over vectors or slice, which provide a safe API.

## Overhead of going to sleep and waking up

Interacting with condition variables, in particular waking threads up is not cheap. To mitigate that:

 - Instead of using a traditional Condvar/Mutex pair directly, each worker thread uses a Parker (from crossbeam_utils) which internally uses a condition variable, but also does various tricks with atomics to avoid interacting with the condvar, and also avoids holding the lock while notifying the thread so that they don't have to block as soon as they wake up (crossbeam_utils's source code explains this better than I can).
 - We track the sleepy thread using an atomic bitfield (see `core::Activity`, and avoid attempting to wake threads if they are not alseep. As a result, when all worker threads a already awake, calling Context::wake is fairly cheap.

Still, for short workloads waking threads up is pretty big cost (if the thread pool was idle), so the best way to avoid it is to keep the thread pool busy. Rayon handles this by spinning for a while in the hope that new work will come. It with helps reducing latency when new work does come soon enough, however it also takes a fair amount of CPU resources, which in some context (a web browser for example) is not very desirable. So for now, this crate doesn't do that and workers go to sleep as soon as they can't find useful work to do.

Another important topic is what to do with the thread that submits work (let's call it the submitter thread) when it is not one of the worker threads.
There are context objects, and only they can process submitted work. There are more contexts than workers, the number of contexts is fixed at the initialization of the thread pool. Any thread that owns a context can cheaply submit and participate in the work. So a "waiting" operation usually will try to process work until the submitted workload is done and only block if there is no more work to steal but the workload is still in progress. The best outcome is when the submitter thread is the last one to finish processing a job of its workload, so that it can continue on without having been put to sleep and woken up. In order to make this more likely, blocking primitives that schedule parallel work keep a bit more work for the submitter's context to process.

## How to get good performance

To have good performance with this system, it is best to either:
 - Ensure that arrays of items concurrently accessed are L1 cache-line padded (for example use crossbeam_utils::CachePadded which is reexported at the root of this crate for conveience).
 - have rather heavy workloads so that the help provided by worker threads is not eclipsed by the time we spent waking them up.
 - or with smaller workloads it is best to be able to overlap them before blocking.
 - for asynchronous workloads, postpone waiting on the result as much as possible to increase the probability of the workload being done by the time we need it (and therefore avoiding the expensive blocking primitives). 

Typically it is hard to beat single-threaded performance with a succession of short workloads where the submitter waits as soon as it submits a workload. Because waking the workers up is not cheap and there's additional latency coming from the gap between when a thread wakes another thread up and when the other thread actually starts running.

## The fixed number of contexts

The contexts are how work can be submitted with a low overhead and low chance of blocking. However their number is set when creating the thread pool and a Context can't be used by multiple threads concurrently. This works well with fixed number of dedicated threads like "The main thread, the gfx thread, etc.", however if we can't predict the number of threads that may want to use the thread pool, it is a bit annoying.

Right now:
- We can have one or several global contexts protected by a mutex.
- we can have some contexts that circulate between threads via pop_context/recycle_context (but there is no way to block on having a context available currently). Right now it's a simple `Mutex<Vec<Context>>` under the hood but it could be made into something fancier.

Creating a large number of contexts is not a great solution because it slows down work-stealing (loops over the context's queues).

Alternatives:
 - Right now making a global context is up to the user, should there always be one managed by this crate?
 - Add two per worker MPSC queues. The low priority one is checked only when the worker is about to sleep, the high priority one would be checked before work stealing. If the queue is too expensive to check, we can count the number of installed jobs in an atomic and only check when the atomic is not zero (but I would be surprised if a good MPSC queue doesn't do something like that internally already). That solves the problem of submitting work but not that of being able to contribute. Only contexts can process submitted jobs.
 - It might be possible to vary the number of contexts dynamically without adding too much overhead to work-stealing as long as the maximum number of contexts is known at initialization time (don't want to have to reallocate the storage while it can be concurrently accessed). Ultimately we have the same problem in that there is a limit, but that limit could be higher without penalizing performance as much.

## TODO

 - Some way to schedule a callback that is executed once per worker thread. WebRender currently needs this for some font related stuff where we don't trust the per-worker font context to be OK to access from outside the worker.
 - Some way to do dependency graphs that maps well with how WebRender's software backend works. (see webrender/src/compositor/sw_compositor.rs). work is divided into rectangles with a dependency graph between rectangles and each rectangle is divided into horizontal bands. The smallest unit of work that can be done in parallel is bands. The dependencies between rectangles is based on z-order and overlap. The graph is determined before execution. We need to separate the rectangles/bands aspects from the purely graph and scheduling aspects and provide a generic API for the latter that fits well with how swgl would use it.
 - See if we can have sync points block on the current worker's thread Parker instead of their own condvar. This way they could be woken up when new work comes in.


## What About Rayon?

Rayon and this crate use similar strategies like work-stealing, however there is a fairly important distinction between rayon and this crate in the execution model:

 - With rayon, workloads are sent to a thread pool (to wait for completion, the submitter thread blocks).
 - With this crate, a submitter thread typically starts processing a workload in the hope that the worker threads will chime in and help via work-stealing. If the thread pool is already very busy, what typically happens is that the new workload is entirely processed by the submitter thread.

The reason this is important (for Gecko) is that we can have very heavy low-priority workloads sent from some thread and high priority smaller ones that are sent from other threads. With a single rayon thread pool, what typically happens then is the high priority work is installed in the thread pool but tends to be picked up only when the bulk of the already-installed low priority work is done and the workers start picking new jobs up. As a result the high priority work can be delayed for a long time. In Gecko we currently work around this by having multiple thread pools but we would prefer to have a single one. This crate has a notion of high and low priority work, helping the workers steal high priority items before low priority ones, but more importantly, if for any reason the thread pool is too busy to pick up high priority work, then the submitter thread can process the entire workload without being blocked.

## Why use rayon over this crate?

Rayon is quite good, there are plenty of reasons to use it over this crate:

 - This crate is a work in progress (no panic handling, woops!). Rayon has been around for a while and works quite well.
 - Rayon is fine tuned for different types of workloads (typically very long ones, I suspect). In doubt, just try both crates and see which works better for your particular workloads.
 - Rayon reduces latency at the cost of spending more CPU cycles spinning. This crates avoids spinning which means worker threads go sleep as soon as they don't have anything to do. The extra latency comes from waking them back up.
 - Rayon has a very nice API with lots of functionality. This crate is designed around the set of particular workloads I am currently trying to optimize so it's API is not nearly as nice and rich for other use cases.
 - Rayon is a pretty cool name.

## Should this be an async executor?

I don't know. Maybe? What I'm trying to optimize at the moment doesn't use async code so I might look into that later.
