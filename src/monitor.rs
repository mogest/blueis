extern crate bus;

use self::bus::Bus;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

pub fn start_monitor() -> (Arc<Mutex<Bus<String>>>, Sender<String>) {
    let (client_tx, monitor_rx) = mpsc::channel();

    let monitor = Arc::new(Mutex::new(Bus::new(100)));

    let local_monitor = monitor.clone();

    thread::spawn(move || {
        for message in monitor_rx.iter() {
            local_monitor.lock().unwrap().broadcast(message);
        }
    });

    (monitor, client_tx)
}
