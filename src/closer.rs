use std::sync::{Arc, Mutex};
use crossbeam_channel::{Receiver, Sender, unbounded};

pub struct Closer {
    send: Mutex<Option<Sender<()>>>,
    pub receive: Arc<Receiver<()>>
}

impl Closer {
    pub fn new() -> Self {
        let (sx, rx) = unbounded();
        
        Closer {
            send: Mutex::new(Some(sx)),
            receive: Arc::new(rx)
        }
    }
    
    pub fn close(&self) {
        self.send.lock().unwrap().take();
    }
}