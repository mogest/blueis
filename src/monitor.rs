use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::Cell;

#[derive(Clone)]
pub struct Monitor {
    pub queue: Arc<Mutex<VecDeque<String>>>,
    pub start: Arc<AtomicUsize>,
    pub stop: Arc<AtomicUsize>,
    pub cond: Arc<Condvar>,
    max_queue_size: usize
}

pub struct Listener<'a> {
    monitor: &'a Monitor,
    position: Cell<usize>
}

impl Monitor {
    pub fn new(max_queue_size: usize) -> Monitor {
        if max_queue_size < 1 { panic!("max_queue_size must be 1 or greater"); }

        Monitor {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            start: Arc::new(AtomicUsize::new(0)),
            stop: Arc::new(AtomicUsize::new(0)),
            cond: Arc::new(Condvar::new()),
            max_queue_size: max_queue_size
        }
    }

    pub fn send(&self, payload: String) {
        let mut locked_queue = self.queue.lock().unwrap();

        locked_queue.push_back(payload);
        if locked_queue.len() > self.max_queue_size {
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

#[cfg(test)]
mod tests {
    use super::Monitor;
    use std::{thread, time};

    #[test]
    fn monitor_acts_as_an_mpmc_queue() {
        let monitor = Monitor::new(100);

        monitor.send("will never be received".to_string());

        let listener_a = monitor.listen();
        monitor.send("A".to_string());

        let listener_b = monitor.listen();
        monitor.send("B".to_string());

        assert_eq!(listener_a.recv().unwrap(), "A".to_string());
        assert_eq!(listener_a.recv().unwrap(), "B".to_string());
        assert_eq!(listener_b.recv().unwrap(), "B".to_string());
    }

    #[test]
    fn monitor_can_be_shared_with_threads() {
        let monitor = Monitor::new(100);
        let mut receivers = vec![];

        for _ in 0..4 {
            let local_monitor = monitor.clone();

            receivers.push(thread::spawn(move || {
                let listener = local_monitor.listen();
                let mut payloads = (0..4).map(|_| listener.recv().unwrap()).collect::<Vec<String>>();
                payloads.sort();
                assert_eq!(payloads, vec!["0".to_string(), "1".to_string(), "2".to_string(), "3".to_string()]);
            }));
        }

        thread::sleep(time::Duration::from_millis(100));

        for index in 0..4 {
            let local_monitor = monitor.clone();

            thread::spawn(move || {
                local_monitor.send(index.to_string());
            });
        }

        for receiver in receivers {
            receiver.join().unwrap();
        }
    }

    #[test]
    fn nothing_bad_happens_when_you_send_with_no_listeners() {
        let monitor = Monitor::new(100);
        monitor.send("some payload".to_string());
    }

    #[test]
    fn queues_are_correctly_handled_when_they_overflow() {
        let monitor = Monitor::new(2);
        let listener = monitor.listen();

        for x in 0..5 {
            monitor.send(x.to_string());
            assert_eq!(listener.recv().unwrap(), x.to_string());
        }
    }

    #[test]
    fn listeners_miss_out_on_data_gracefully_if_queue_overflows() {
        let monitor = Monitor::new(2);
        let listener = monitor.listen();

        monitor.send("A".to_string());
        assert_eq!(listener.recv().unwrap(), "A".to_string());

        monitor.send("B".to_string());
        monitor.send("C".to_string());
        monitor.send("D".to_string());

        // the listener misses out on "B" because the queue size is only 2 long
        assert_eq!(listener.recv().unwrap(), "C".to_string());
        assert_eq!(listener.recv().unwrap(), "D".to_string());
    }
}
