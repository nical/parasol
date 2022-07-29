//! Helpers to build high level parallel execution primitives on top of the core.
//!
//! 

use crate::array::{ForEachBuilder, new_for_each};
use crate::join::{JoinBuilder, new_join};
use crate::{Context, ContextId, Priority};
use crate::task::TaskParameters;

use std::ops::Deref;
use std::sync::Arc;

/// A builder for common execution parameters such as priority and context data.
pub struct Parameters<'c, 'cd, 'id, ContextData, ImmutableData> {
    pub(crate) ctx: &'c mut Context,
    pub(crate) context_data: &'cd mut [ContextData],
    pub(crate) immutable_data: &'id ImmutableData,
    pub(crate) priority: Priority,
}

impl<'c, 'cd, 'id, ContextData, ImmutableData> Parameters<'c, 'cd, 'id, ContextData, ImmutableData> {
    /// Specify some per-context data that can be mutably accessed by the run function.
    ///
    /// This can be useful to store and reuse some scratch buffers and avoid memory allocations in the
    /// run function.
    ///
    /// The length of the slice must be at least equal to the number of worker threads plus one.
    ///
    /// For best performance make sure the size of the data is a multiple of L1 cache line size (see `CachePadded`).
    #[inline]
    pub fn with_context_data<'cd2, CtxData: Send>(self, context_data: &'cd2 mut [CtxData]) -> Parameters<'c, 'cd2, 'id, CtxData, ImmutableData> {
        // Note: doing this check here is important for the safety of ContextDataRef::get
        assert!(
            context_data.len() >= self.ctx.num_worker_threads() as usize + 1,
            "Got {:?} context items, need at least {:?}",
            context_data.len(), self.ctx.num_worker_threads() + 1,
        );

        Parameters {
            context_data,
            immutable_data: self.immutable_data,
            priority: self.priority,
            ctx: self.ctx
        }
    }

    #[inline]
    pub fn with_immutable_data<'id2, Data: Sync>(self, immutable_data: &'id2 Data) -> Parameters<'c, 'cd, 'id2, ContextData, Data> {
        Parameters {
            context_data: self.context_data,
            immutable_data: immutable_data,
            priority: self.priority,
            ctx: self.ctx
        }
    }

    #[inline]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;

        self
    }

    #[inline]
    pub fn priority(&self) -> Priority {
        self.priority
    }

    #[inline]
    pub fn context(&self) -> &Context {
        &self.ctx
    }

    #[inline]
    pub fn context_mut(&mut self) -> &mut Context {
        &mut self.ctx
    }

    #[inline]
    pub fn join<F1, F2>(self, f1: F1, f2: F2) -> JoinBuilder<'c, 'cd, 'id, ContextData, ImmutableData, F1, F2>
    where
        F1: FnOnce(&mut Context, crate::join::Args<ContextData, ImmutableData>) + Send,
        F2: FnOnce(&mut Context, crate::join::Args<ContextData, ImmutableData>) + Send,
    {
        new_join(self, f1, f2)
    }

    #[inline]
    pub fn for_each<'i, Item>(self, items: &'i mut[Item]) -> ForEachBuilder<'i, 'cd, 'id, 'c, Item, ContextData, ImmutableData, ()> {
        new_for_each(self, items)
    }
}

/// An atomic reference counted container for data that can be mutably accessed by
/// contexts concurrently and safely.
///
/// Think of it as an `Arc<[T]>` with some extra constraints and gurarantees:
///  - The size of the array must be at least as large as the number of contexts.
///  - since contexts have their own index and can't be used by multiple threads
///  - concurrently, the the presence of a context is enough to guarantee exclusive
///    mutable access to the item at its index.
pub struct HeapContextData<T> {
    // Both options should only ever be None when the type is ().
    data: Option<Arc<[T]>>,
    ctx: Option<ContextId>,
}

impl<T> Clone for HeapContextData<T> {
    fn clone(&self) -> Self {
        HeapContextData { data: self.data.clone(), ctx: self.ctx.clone() }
    }
}

pub fn heap_context_data() -> HeapContextData<()> {
    HeapContextData { data: None, ctx: None, }
}


impl<C: Send> HeapContextData<C> {
    pub fn from_vec(data: Vec<C>, ctx: ContextId) -> Self {
        HeapContextData {
            data: Some(data.into()),
            ctx: Some(ctx),
        }
    }
}

impl<C> HeapContextData<C> {
    /// Returns a mutable slice only if this is the only remaining reference to
    /// the context data, None otherwise.
    pub fn as_mut_slice(&mut self) -> Option<&mut [C]> {
        if let Some(data) = &mut self.data {
            Arc::get_mut(data)
        } else {
            None
        }
    }

    pub unsafe fn get_ref(&self, ctx: &Context) -> ContextDataRef<C> {
        if !ctx.is_worker_thread() && self.ctx.is_some() {
            // There is a single slot in the context data array for non-worker
            // contexts, so we have to ensure that it isn't used by multiple
            // non-worker contexts by associating the context data to only one
            // of them at any given time.
            assert_eq!(self.ctx, Some(ctx.id()));
        }
        let ctx_data: *mut C = if let Some(data) = &self.data {
            // Note: This check is important for the safety of ContextDataRef::get.
            let min = ctx.num_worker_threads() as usize + 1;
            let count = data.len();
            assert!(count >= min, "Got {:?} context items, need at least {:?}", count, min);

            std::mem::transmute(data.as_ptr())
        } else {
            std::ptr::null_mut()
        };

        ContextDataRef { ptr: ctx_data }
    }

    // TODO:
    // This is actually not quite safe because a thread can create multiple unique references
    // to the same item via multiple context data refs.
    // pub fn get_item_mut(&mut self, ctx: &Context) -> &mut C {
    //     unsafe {
    //         let data_ref = self.get_ref(ctx);
    //         data_ref.get(ctx)
    //     }
    // }

    pub fn strong_count(&self) -> usize {
        self.data.as_ref()
            .map(|data| Arc::strong_count(&data))
            .unwrap_or(1)
    }
}

/// Erases the lifetime of references to the context data and immutable data.
///
/// This does not own/destroys the referenced data, it is on you to ensure that
/// the data outlives the ContextDataRef.
///
/// Used internally by various job implementations.
pub struct ContextDataRef<ContextData> {
    ptr: *mut ContextData,
}

impl<ContextData> ContextDataRef<ContextData> {
    #[inline]
    pub unsafe fn get<'l>(&self, ctx: &Context) -> &'l mut ContextData {
        let context_data_index = ctx.data_index() as isize;
        // SAFETY: Here we rely two very important things:
        // - If there is no context data, then it's type is `()`, which means reads and writes
        //   to the pointer are ALWAYS noop whatever the address of the pointer.
        // - If a context data array was provided, its size has been checked in `with_context_data`.
        //
        // As a result it is impossible to craft a pointer that will read or write out of bounds
        // here.
        //
        // TODO: unfortunately miri errors when producing &mut () from a null pointer.
        &mut *self.ptr.wrapping_offset(context_data_index)
    }

    /// Returns unsafe references to the context data and immutable data.
    ///
    /// The caller is responsible for ensuring that the context data and immutable data
    /// outlines the unsafe ref.
    #[inline]
    pub unsafe fn from_ref<'c, 'cd, 'id, ImmutableData>(parameters: &mut Parameters<'c, 'cd, 'id, ContextData, ImmutableData>) -> ContextDataRef<ContextData> {
        ContextDataRef {
            ptr: parameters.context_data.as_mut_ptr(),
        }
    }


    /// Returns unsafe references to the context data and immutable data.
    ///
    /// The caller is responsible for ensuring that the context data and immutable data
    /// outlines the unsafe ref.
    #[inline]
    pub unsafe fn from_owned<Input, ImmutableData>(parameters: &mut TaskParameters<Input, ContextData, ImmutableData>, ctx: &Context) -> ContextDataRef<ContextData> {
        parameters.context_data.get_ref(ctx)
    }
}

pub struct ImmutableDataRef<ImmutableData> {
    ptr: *const ImmutableData,
}

impl<ImmutableData> ImmutableDataRef<ImmutableData> {
    #[inline]
    pub unsafe fn from_ref<'c, 'cd, 'id, ContextData>(parameters: &mut Parameters<'c, 'cd, 'id, ContextData, ImmutableData>) -> ImmutableDataRef<ImmutableData> {
        ImmutableDataRef { ptr: parameters.immutable_data }
    }

    #[inline]
    pub unsafe fn from_owned<Input, ContextData>(parameters: &mut TaskParameters<Input, ContextData, ImmutableData>) -> ImmutableDataRef<ImmutableData> {
        // If immutable_data is None, then it s always the unit type (), in which case we don't care
        // about what the address points to since no interaction with the unit type translates to actual
        // reads or writes to memory. We could default to pass std::ptr::null(), however miri has checks
        // that fail when we dereference null pointers, even if they are the unit type.
        // So instead we use a dummy empty slice and take its pointer.
        let dummy: &[ImmutableData] = &[];
        let ptr = parameters.immutable_data
            .as_ref()
            .map_or(dummy.as_ptr(), |boxed| &*boxed.deref());

        ImmutableDataRef {
            ptr
        }
    }

    pub unsafe fn get(&self) -> &ImmutableData {
        &*self.ptr
    }
}

pub struct ConcurrentDataRef<ContextData, ImmutableData> {
    pub context_data: ContextDataRef<ContextData>,
    pub immutable_data: ImmutableDataRef<ImmutableData>,
}

impl<ContextData, ImmutableData> ConcurrentDataRef<ContextData, ImmutableData> {
    pub unsafe fn get(&self, ctx: &Context) -> (&mut ContextData, &ImmutableData) {
        (
            self.context_data.get(ctx),
            self.immutable_data.get(),
        )
    }

    /// Returns unsafe references to the context data and immutable data.
    ///
    /// The caller is responsible for ensuring that the context data and immutable data
    /// outlines the unsafe ref.
    #[inline]
    pub unsafe fn from_ref<'c, 'cd, 'id>(parameters: &mut Parameters<'c, 'cd, 'id, ContextData, ImmutableData>) -> ConcurrentDataRef<ContextData, ImmutableData> {
        ConcurrentDataRef {
            context_data: ContextDataRef::from_ref(parameters),
            immutable_data: ImmutableDataRef::from_ref(parameters),
        }
    }

    /// Returns unsafe references to the context data and immutable data.
    ///
    /// The caller is responsible for ensuring that the context data and immutable data
    /// outlines the unsafe ref.
    #[inline]
    pub unsafe fn from_owned<Input>(parameters: &mut TaskParameters<Input, ContextData, ImmutableData>, ctx: &Context) -> ConcurrentDataRef<ContextData, ImmutableData> {
        ConcurrentDataRef {
            context_data: ContextDataRef::from_owned(parameters, ctx),
            immutable_data: ImmutableDataRef::from_owned(parameters),
        }
    }
}
