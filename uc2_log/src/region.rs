// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Raw memory region backing the log buffer: heap for unit tests (miri-clean)
//! and mmap'd files for real instances (Task 5 adds `from_mmap`).
//!
//! Safety model: the region hands out raw pointers only; no `&`/`&mut`
//! references to buffer bytes are ever held across threads. All cross-thread
//! ordering goes through the frame commit word and the position counters
//! (release/acquire). Concurrent writers/readers never touch the same bytes:
//! the appender's overrun gate (vs `durable`) and readers' bounds (vs
//! `append`) partition the address space by position.

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::ptr::NonNull;

enum Backing {
    Heap(Layout),
}

pub struct Region {
    ptr: NonNull<u8>,
    len: usize,
    backing: Backing,
}

// SAFETY: raw memory; synchronization is the caller's protocol (see module doc).
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// Heap-backed zeroed region (unit tests / miri).
    pub fn heap_zeroed(len: usize) -> Self {
        assert!(len > 0);
        let layout = Layout::from_size_align(len, 64).expect("region layout");
        // SAFETY: len > 0, valid layout.
        let raw = unsafe { alloc_zeroed(layout) };
        let Some(ptr) = NonNull::new(raw) else { handle_alloc_error(layout) };
        Self { ptr, len, backing: Backing::Heap(layout) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// # Safety
    /// `off < self.len()`; the caller upholds the module-level aliasing
    /// protocol (position-partitioned access, atomics for cross-thread order).
    #[inline]
    pub unsafe fn ptr_at(&self, off: usize) -> *mut u8 {
        debug_assert!(off < self.len);
        // SAFETY: off < len per contract.
        unsafe { self.ptr.as_ptr().add(off) }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        match &self.backing {
            // SAFETY: allocated with this exact layout in heap_zeroed.
            Backing::Heap(layout) => unsafe { dealloc(self.ptr.as_ptr(), *layout) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_region_is_zeroed_and_writable() {
        let r = Region::heap_zeroed(4096);
        assert_eq!(r.len(), 4096);
        unsafe {
            assert_eq!(*r.ptr_at(0), 0);
            assert_eq!(*r.ptr_at(4095), 0);
            *r.ptr_at(17) = 0xab;
            assert_eq!(*r.ptr_at(17), 0xab);
        }
    }
}
