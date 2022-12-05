//! 
//! YOU SHOULD NOT NEED TO CHANGE CODE IN THIS FILE.
//! 
extern crate serde;
extern crate serde_json;
extern crate bincode;
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;
use message;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;

#[derive(Debug)]
pub struct OpLog {
    seqno: i32,
    // log_arc: Arc<Mutex<HashMap<i32, message::ProtocolMessage>>>,
    path: String,
    lf: File,
}

impl OpLog {
    pub fn new(fpath: String) -> OpLog {
        // let l = HashMap::new();
        // let lck = Mutex::new(l);
        // let arc = Arc::new(lck);
        OpLog {
            seqno: 0,            
            // log_arc: arc,
            path: fpath.to_string(),
            lf: File::create(fpath).unwrap(),
        }
    }
    // pub fn from_file(fpath: String) -> OpLog {
    //     let seqno = 0;        
    //     let mut l = HashMap::new();
    //     let scopy = fpath.clone();
    //     let tlf = File::open(fpath).unwrap();
    //     let mut reader = BufReader::new(&tlf);
    //     let mut line = String::new();
    //     let mut len = reader.read_line(&mut line).unwrap();
    //     while len > 0 {
    //         let pm = message::ProtocolMessage::from_string(&line);
    //         l.insert(pm.uid, pm);
    //         line.clear();
    //         len = reader.read_line(&mut line).unwrap();
    //     }
    //     let lck = Mutex::new(l);
    //     let arc = Arc::new(lck);
    //     OpLog {
    //         seqno: seqno,            
    //         log_arc: arc,
    //         path: scopy,
    //         lf: tlf,
    //     }
    // }    
    // pub fn append(&mut self, t: message::MessageType, tid: i32, sender: String, op: i32) {
    //     let lck = Arc::clone(&self.log_arc);
    //     let mut log = lck.lock().unwrap();
    //     self.seqno += 1;
    //     let id = self.seqno;
    //     let pm = message::ProtocolMessage::generate(t, tid, sender, op);        
    //     serde_json::to_writer(&mut self.lf, &pm).unwrap();    
    //     writeln!(&mut self.lf).unwrap();
    //     self.lf.flush().unwrap();
    //     log.insert(id, pm);
    // }
    // pub fn read(&mut self, offset: &i32) -> message::ProtocolMessage {
    //     let lck = Arc::clone(&self.log_arc);
    //     let log = lck.lock().unwrap();
    //     let pm = log[&offset].clone();
    //     pm
    // }
    // pub fn arc(&self) -> Arc<Mutex<HashMap<i32, message::ProtocolMessage>>> {
    //     Arc::clone(&self.log_arc)
    // }
}