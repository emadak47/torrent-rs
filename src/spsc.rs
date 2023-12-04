use std::mem;
use std::sync::Arc;
use std::vec::Vec;

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, PartialEq)]
pub enum QueueError {
    EmptyQueue,
}
/// The inner representation of a single-producer single-consumer queue.
struct Inner<T> {
    /// The head of the queue.
    ///
    /// This integer is in range `0 .. 2 * cap`.
    head: CachePadded<AtomicUsize>,

    /// The tail of the queue.
    ///
    /// This integer is in range `0 .. 2 * cap`.
    tail: CachePadded<AtomicUsize>,

    /// The buffer holding slots.
    buffer: *mut T,

    /// The queue capacity.
    cap: usize,
}

impl<T> Inner<T> {
    /// Returns a pointer to the slot at position `pos`.
    ///
    /// The position must be in range `0 .. 2 * cap`.
    #[inline]
    unsafe fn slot(&self, pos: usize) -> *mut T {
        if pos < self.cap {
            self.buffer.add(pos)
        } else {
            self.buffer.add(pos - self.cap)
        }
    }

    /// Increments a position by going one slot forward.
    ///
    /// The position must be in range `0 .. 2 * cap`.
    #[inline]
    fn increment(&self, pos: usize) -> usize {
        if pos < 2 * self.cap - 1 {
            pos + 1
        } else {
            0
        }
    }

    /// Returns the distance between two positions.
    ///
    /// Positions must be in range `0 .. 2 * cap`.
    #[inline]
    fn distance(&self, a: usize, b: usize) -> usize {
        if a <= b {
            b - a
        } else {
            2 * self.cap - a + b
        }
    }
}

/// The producer side of a bounded single-producer single-consumer queue.
pub struct Producer<T> {
    /// The inner representation of the queue.
    inner: Arc<Inner<T>>,

    /// A copy of `inner.head` for quick access.
    ///
    /// This value can be stale and sometimes needs to be resynchronized with `inner.head`.
    head: usize,

    /// A copy of `inner.tail` for quick access.
    ///
    /// This value is always in sync with `inner.tail`.
    tail: usize,
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T> Producer<T> {
    /// Attempts to push an element into the queue.
    ///
    /// If the queue is full, returns true or false depending on succesful of operation.
    pub fn try_push(&mut self, value: T) -> bool {
        let mut head = self.head;
        let mut tail = self.tail;

        // Check if the queue is *possibly* full.
        if self.inner.distance(head, tail) == self.inner.cap {
            // We need to refresh the head and check again if the queue is *really* full.
            head = self.inner.head.load(Ordering::Acquire);
            self.head = head;

            // Is the queue *really* full?
            if self.inner.distance(head, tail) == self.inner.cap {
                return false;
            }
        }

        // Write the value into the tail slot.
        unsafe {
            self.inner.slot(tail).write(value);
        }

        // Move the tail one slot forward.
        tail = self.inner.increment(tail);
        self.inner.tail.store(tail, Ordering::Release);
        self.tail = tail;

        return true;
    }

    // spinlock : spin until new space is available
    pub fn push(&mut self, value: T) {
        let mut head = self.head;
        let mut tail = self.tail;

        // spin until new space is left
        while self.inner.distance(head, tail) == self.inner.cap {
            head = self.inner.head.load(Ordering::Acquire);
            self.head = head;
        }
        // Write the value into the tail slot.
        unsafe {
            self.inner.slot(tail).write(value);
        }
        // Move the tail one slot forward.
        tail = self.inner.increment(tail);
        self.inner.tail.store(tail, Ordering::Release);
        self.tail = tail;
    }
}

/// The consumer side of a bounded single-producer single-consumer queue.
pub struct Consumer<T> {
    /// The inner representation of the queue.
    inner: Arc<Inner<T>>,

    /// A copy of `inner.head` for quick access.
    ///
    /// This value is always in sync with `inner.head`.
    head: usize,

    /// A copy of `inner.tail` for quick access.
    ///
    /// This value can be stale and sometimes needs to be resynchronized with `inner.tail`.
    tail: usize,
}
unsafe impl<T: Send> Send for Consumer<T> {}
impl<T> Consumer<T> {
    /// Attempts to pop an element from the queue.
    ///
    /// If the queue is empty, an error is returned.
    pub fn pop(&mut self) -> Result<T, QueueError> {
        let mut head = self.head;
        let mut tail = self.tail;

        // Check if the queue is *possibly* empty.
        if head == tail {
            // We need to refresh the tail and check again if the queue is *really* empty.
            tail = self.inner.tail.load(Ordering::Acquire);
            self.tail = tail;

            // Is the queue *really* empty?
            if head == tail {
                return Err(QueueError::EmptyQueue);
            }
        }

        // Read the value from the head slot.
        let value = unsafe { self.inner.slot(head).read() };

        // Move the head one slot forward.
        head = self.inner.increment(head);
        self.inner.head.store(head, Ordering::Release);
        self.head = head;
        Ok(value)
    }
}

pub struct SPSCQueue;

impl SPSCQueue {
    pub fn new<T>(cap: usize) -> (Producer<T>, Consumer<T>) {
        assert!(cap > 0, "capacity must be non-zero");

        // Allocate a buffer of length `cap`.
        let buffer = {
            let mut v = Vec::<T>::with_capacity(cap);
            let ptr = v.as_mut_ptr();
            mem::forget(v);
            ptr
        };

        let inner = Arc::new(Inner {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            buffer,
            cap,
        });

        let p = Producer {
            inner: inner.clone(),
            head: 0,
            tail: 0,
        };

        let c = Consumer {
            inner,
            head: 0,
            tail: 0,
        };

        (p, c)
    }
}
