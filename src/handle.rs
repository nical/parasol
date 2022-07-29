//! Handles are references to ongoing work.
//!
//! They consist of:
//!  - A strong reference to a reference counted heap alocation to maintain the job's data alive,
//!  - A pointer to an Event to be able to wait until the job is done,
//!  - And optionally a pointer to where the output of the job will be written, if any.
//!
//! Typically, the heap allocation will contain the job's callback, then event, the output
//! and whatever else the job needs to do its work. The job will also hold a self-reference
//! that goes away at the end of the work. This way the job data isn't deleted until all
//! handles are gone and the job is done.

use crate::{Context, Event};
use crate::task::TaskDependency;
use crate::ref_counted::AnyRefPtr;
use std::cell::UnsafeCell;

/// An unsynchronized, internally mutable slot where data can be placed, for example
/// to store the output or input of a job.
pub struct DataSlot<T> {
    cell: UnsafeCell<Option<T>>,
}

impl<T> DataSlot<T> {
    /// Create a data slot.
    #[inline]
    pub fn new() -> Self {
        DataSlot {
            cell: UnsafeCell::new(None)
        }
    }

    #[inline]
    /// Create an already-set data slot.
    pub fn from(data: T) -> Self {
        DataSlot {
            cell: UnsafeCell::new(Some(data))
        }
    }

    /// Place data in the slot.
    /// 
    /// Safety:
    ///  - `set` must be called at most once.
    ///  - `set` must not be called if the slot was created with `from`.
    #[inline]
    pub unsafe fn set(&self, payload: T) {
        debug_assert!((*self.cell.get()).is_none());
        (*self.cell.get()) = Some(payload);
    }

    /// Move the data out of the slot.
    ///
    /// Safety:
    ///  - `take` must be called at most once, *after* the
    ///    slot is set (either after `set` was called or if the slot
    ///    was created with `from`).
    ///  - `take` and `get_ref` must not be called concurrently.
    #[inline]
    pub unsafe fn take(&self) -> T {
        (*self.cell.get()).take().unwrap()
    }

    /// Get a reference on the data.
    ///
    /// Safety:
    ///  - This must be called *after* the slot is set (either
    ///    after `set` was called or if the slot was created with
    ///    `from`).
    ///  - `take` and `get_ref` must not be called concurrently.
    pub unsafe fn get_ref(&self) -> &T {
        (*self.cell.get()).as_ref().unwrap()
    }
}

/// A non-clonable handle which owns the result.
pub struct OwnedHandle<Output> {
    // Maintains the task's data alive.
    job_data: AnyRefPtr,
    event: *const Event,
    output: *const DataSlot<Output>,
}

impl<Output> OwnedHandle<Output> {
    pub unsafe fn new(
        job_data: AnyRefPtr,
        event: *const Event,
        output: *const DataSlot<Output>,
    ) -> Self {
        OwnedHandle { job_data, event, output }
    }

    pub fn wait(&self, ctx: &mut Context) {
        unsafe {
            (*self.event).wait(ctx);
            debug_assert!((*self.event).is_signaled());
        }
    }

    pub fn resolve(self, ctx: &mut Context) -> Output {
        self.wait(ctx);
        unsafe {
            (*self.output).take()
        }
    }

    pub fn resolve_assuming_ready(self) -> Output {
        assert!(self.poll(), "Handle is not ready.");
        unsafe {
            (*self.output).take()
        }
    }

    pub fn poll(&self) -> bool {
        unsafe {
            (*self.event).is_signaled()
        }
    }

    pub fn shared(self) -> SharedHandle<Output> {
        SharedHandle { inner: self }
    }

    pub fn handle(&self) -> Handle {
        Handle {
            _job_data: self.job_data.clone(),
            event: self.event
        }
    }
}

// TODO: Need some way to implement TaskDependency<Output = &T> for SharedHandle<T>
// But the lifetime makes it hard to express.

/// A clonable handle that can only borrow the result.
pub struct SharedHandle<Output> {
    inner: OwnedHandle<Output>
}

impl<Output> SharedHandle<Output> {
    pub unsafe fn new(
        job_data: AnyRefPtr,
        event: *const Event,
        output: *mut DataSlot<Output>,
    ) -> Self {
        SharedHandle {
            inner: OwnedHandle::new(job_data, event, output)
        }
    }

    pub fn wait(&self, ctx: &mut Context) -> &Output {
        unsafe {
            (*self.inner.event).wait(ctx);
            (*self.inner.output).get_ref()
        }
    }

    pub fn poll(&self) -> bool {
        self.inner.poll()
    }

    pub fn handle(&self) -> Handle {
        self.inner.handle()
    }
}

impl<Output> Clone for SharedHandle<Output> {
    fn clone(&self) -> Self {
        SharedHandle {
            inner: OwnedHandle {
                job_data: self.inner.job_data.clone(),
                event: self.inner.event,
                output: self.inner.output,
            }
        }
    }
}

#[derive(Clone)]
/// A handle that doesn't know about the output of the task, but can
/// be used to wait or as a dependency.
pub struct Handle {
    _job_data: AnyRefPtr,
    event: *const Event,
}

impl Handle {
    pub unsafe fn new(
        job_data: AnyRefPtr,
        event: *const Event,
    ) -> Self {
        Handle { _job_data: job_data, event }
    }

    pub fn wait(&self, ctx: &mut Context) {
        unsafe {
            (*self.event).wait(ctx);
        }
    }

    pub fn poll(&self) -> bool {
        unsafe {
            (*self.event).is_signaled()
        }
    }
}

impl<T> TaskDependency for OwnedHandle<T> {
    type Output = T;
    fn get_output(&self) -> T {
        unsafe {
            debug_assert!((*self.event).is_signaled());
            (*self.output).take()
        }
    }

    fn get_event(&self) -> Option<&Event> {
        unsafe { Some(&*self.event) }
    }
}

impl TaskDependency for Handle {
    type Output = ();
    fn get_output(&self) -> () { () }
    fn get_event(&self) -> Option<&Event> {
        unsafe { Some(&*self.event) }
    }
}

impl TaskDependency for () {
    type Output = ();
    fn get_output(&self) -> () { () }
    fn get_event(&self) -> Option<&Event> { None }
}

impl<T> TaskDependency for DataSlot<T> {
    type Output = T;
    fn get_output(&self) -> T {
        unsafe { self.take() }
    }
    fn get_event(&self) -> Option<&Event> {
        None
    }
}
