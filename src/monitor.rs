use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::Cell;

#[derive(Clone)]
pub struct Monitor {
    pub queue: Arc<Mutex<VecDeque<String>>>,
    pub start: Arc<AtomicUsize>,
    pub stop: Arc<AtomicUsize>,
    pub cond: Arc<Condvar>
}

pub struct Listener<'a> {
    monitor: &'a Monitor,
    position: Cell<usize>
}

const MAX_QUEUE_LENGTH: usize = 100;

impl Monitor {
    pub fn new() -> Monitor {
        Monitor {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            start: Arc::new(AtomicUsize::new(0)),
            stop: Arc::new(AtomicUsize::new(0)),
            cond: Arc::new(Condvar::new())
        }
    }

    pub fn send(&self, payload: String) {
        let mut locked_queue = self.queue.lock().unwrap();

        locked_queue.push_back(payload);
        if locked_queue.len() > MAX_QUEUE_LENGTH {
            locked_queue.pop_front();
            self.start.fetch_add(1, Ordering::Release);
        }

        self.stop.fetch_add(1, Ordering::Release);

        self.cond.notify_all();
    }

    pub fn listen(&self) -> Listener {
        Listener {
            monitor: self,
            position: Cell::new(self.stop.load(Ordering::Acquire))
        }
    }
}

impl<'a> Listener<'a> {
    pub fn recv(&self) -> Option<String> {
        let mut locked_queue = self.monitor.queue.lock().unwrap();

        while self.position.get() == self.monitor.stop.load(Ordering::Acquire) {
            locked_queue = self.monitor.cond.wait(locked_queue).unwrap();
        }

        let start = self.monitor.start.load(Ordering::Acquire);

        if self.position.get() < start {
            // we missed some, notify caller?
            self.position.set(start);
        }

        let payload = locked_queue.get(self.position.get() - start);
        self.position.set(self.position.get() + 1);

        Some(payload.unwrap().clone())
    }
}
