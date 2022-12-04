//! 
//! client.rs 
//! Implementation of 2PC client
//! 
extern crate log;
extern crate stderrlog;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::atomic::{AtomicI32, AtomicBool, Ordering};
use std::sync::{Arc};
use std::time::Duration;
use std::thread;
use std::collections::HashMap;
use message;
use message::MessageType;
use message::RequestStatus;


// static counter for getting unique TXID numbers
static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

// client state and 
// primitives for communicating with 
// the coordinator

#[derive(Debug)]
pub struct Client {    
    pub id: i32,
    // is: String,
    tx: Sender<message::ProtocolMessage>,
    rx: Receiver<message::ProtocolMessage>,
    logpath: String,
    r: Arc<AtomicBool>,
    // ...
}

///
/// client implementation
/// Required: 
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown 
/// 3. pub fn protocol(&mut self, n_requests: i32) -- implements client side protocol
///
impl Client {

    /// 
    /// new()
    /// 
    /// Return a new client, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    /// 
    pub fn new(i: i32,
               is: String,
               tx: Sender<message::ProtocolMessage>,
               rx: Receiver<message::ProtocolMessage>,
               logpath: String,
               r: Arc<AtomicBool>) -> Client {
        Client {
            id: i,
            tx,
            rx,
            logpath,
            r,
            // ...
        }   
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("Client_{} waiting for exit signal", self.id);

        while self.r.load(Ordering::SeqCst) {}

        trace!("Client_{} exiting", self.id);
    }

    /// 
    /// send_next_operation(&mut self)
    /// send the next operation to the coordinator
    /// 
    pub fn send_next_operation(&mut self, req: message::Request) {

        trace!("Client_{}::send_next_operation", self.id);

        // create a new request with a unique TXID.         
        let request_no: i32 = 0; // TODO--choose another number!
        let txid = TXID_COUNTER.fetch_add(1, Ordering::SeqCst);

        info!("Client {} request({})->txid:{} called", self.id, request_no, txid);

        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest(req), 
                                                    txid, 
                                                    format!("Client_{}", self.id), 
                                                    request_no);

        info!("client {} calling send...", self.id);

        self.tx.send(pm).unwrap();

        trace!("Client_{}::exit send_next_operation", self.id);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the 
    /// last issued request. Note that we assume the coordinator does 
    /// not fail in this simulation
    /// 
    pub fn recv_result(&mut self) {
        if !self.r.load(Ordering::SeqCst) {
            return
        }

        trace!("Client_{}::recv_result", self.id);

        match self.rx.recv() {
            Ok(v) => {println!("received {:?}", v)},
            Err(e) => {panic!("err {}", e)}
        }

        trace!("Client_{}::exit recv_result", self.id);
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this client before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // TODO: collect real stats!
        let successful_ops: usize = 0;
        let failed_ops: usize = 0; 
        let unknown_ops: usize = 0; 
        println!("Client_{}:\tC:{}\tA:{}\tU:{}", self.id, successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    /// 
    pub fn protocol(&mut self, requests: Vec<message::Request>) {
        // run the 2PC protocol for each of n_requests

        for req in requests {
            if !self.r.load(Ordering::SeqCst) {
                break
            }
            self.send_next_operation(req);
            self.recv_result();
        }

        // wait for signal to exit
        // and then report status
        self.wait_for_exit_signal();
        self.report_status();
    }
}
