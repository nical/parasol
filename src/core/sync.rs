pub use std::sync::atomic::Ordering;

#[cfg(not(loom))] pub use std::{
    sync::{
        Arc, Mutex, Condvar,
        atomic::{AtomicI32, AtomicU32, AtomicBool, AtomicPtr},
    },
    thread,
};


#[cfg(loom)] pub use loom::{
    sync::{
        Arc, Mutex, Condvar,
        atomic::{AtomicI32, AtomicU32, AtomicBool, AtomicPtr},
    },
    thread
};
