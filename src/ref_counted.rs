//! Manually implemented reference counting.
//!
//! This module provides some helpers for intrusive reference counting. It is in almost every
//! way worse than the standard `Arc`, so as a general rule of thumb, do not use this when Arc
//! is flexible enough.
//!
//! The main thing that this provides is more flexibility to do unsafe things at your own risks.

use crate::sync::{AtomicI32, Ordering};

/// A rait to implement reference counting by hand.
///
/// We use it so that the job can release its own refcount, which is hard to do with `Arc`.
pub trait RefCounted {
    unsafe fn add_ref(&self);
    unsafe fn release_ref(&self);
}

pub struct InlineRefCounted<T> {
    ref_count: AtomicI32,
    payload: T,
}

impl<T> InlineRefCounted<T> {
    pub fn inner(&self) -> &T {
        &self.payload
    }
}

impl<T> RefCounted for InlineRefCounted<T> {
    unsafe fn add_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::Acquire);
    }

    unsafe fn release_ref(&self) {
        let ref_count = self.ref_count.fetch_sub(1, Ordering::Release) - 1;
        debug_assert!(ref_count >= 0);
        if ref_count == 0 {
            let this : *mut Self = std::mem::transmute(self);
            let _ = Box::from_raw(this);
        }
    }
}

pub struct RefPtr<T> {
    ptr: *mut InlineRefCounted<T>
}

impl<T: 'static> RefPtr<T> {
    pub fn new(payload: T) -> Self {
        let ptr = Box::new(InlineRefCounted {
            ref_count: AtomicI32::new(2),
            payload,
        });

        let ptr = Box::into_raw(ptr);

        RefPtr { ptr }
    }

    pub fn into_any(self) -> AnyRefPtr {
        AnyRefPtr::already_reffed(self.ptr as *mut dyn RefCounted)
    }

    pub unsafe fn mut_payload_unchecked(this: &Self) -> *mut T {
        &mut (*this.ptr).payload
    }

    pub fn as_raw(this: &Self) -> *const InlineRefCounted<T> {
        (*this).ptr
    }
}

impl<T> std::ops::Deref for InlineRefCounted<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.payload }
}

impl<T> std::ops::Deref for RefPtr<T> {
    type Target = InlineRefCounted<T>;
    fn deref(&self) -> &InlineRefCounted<T> { unsafe { &*self.ptr } }
}

/// A strong reference to any `RefCounted` object.
pub struct AnyRefPtr {
    ptr: *mut dyn RefCounted,
}

impl AnyRefPtr {
    pub fn already_reffed(ptr: *mut dyn RefCounted) -> Self {
        assert!(!ptr.is_null());
        AnyRefPtr { ptr }
    }
}

impl Clone for AnyRefPtr {
    fn clone(&self) -> Self {
        unsafe {
            (*self.ptr).add_ref();
        }

        AnyRefPtr::already_reffed(self.ptr)
    }
}

impl Drop for AnyRefPtr {
    fn drop(&mut self) {
        unsafe {
            (*self.ptr).release_ref()
        }
    }
}
