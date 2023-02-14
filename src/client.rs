//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate crossbeam_channel;
extern crate shuttle;
extern crate log;
extern crate rand;
extern crate stderrlog;
use crate::constants;

// use std::sync::mpsc::{Sender, Receiver};
use shuttle::crossbeam_channel::{Receiver, Sender};
use client::rand::prelude::*;
use message;
use shuttle::sync::{Arc, atomic::{AtomicBool, AtomicI32, Ordering}};
use std::time::Duration;

// static counter for getting unique TXID numbers
static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

// client state and
// primitives for communicating with
// the coordinator

#[derive(Debug)]
pub struct Client {
    pub id: usize,
    // is: String,
    c_to_p_txs: Vec<Option<Sender<message::PtcMessage>>>,
    p_to_c_rxs: Vec<Option<Receiver<message::PtcMessage>>>,
    logpath: String,
    r: Arc<AtomicBool>,
    successful_ops: usize,
    failed_ops: usize,
    unknown_ops: usize,
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
    pub fn new(
        i: usize,
        is: String,
        c_to_p_txs: Vec<Option<Sender<message::PtcMessage>>>,
        p_to_c_rxs: Vec<Option<Receiver<message::PtcMessage>>>,
        logpath: String,
        r: Arc<AtomicBool>,
    ) -> Client {
        Client {
            id: i,
            c_to_p_txs,
            p_to_c_rxs,
            logpath,
            r,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
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

    fn select_dest_node(&self) -> usize {
        let rand = rand::thread_rng().gen_range(0..self.c_to_p_txs.len());
        return rand;
    }

    ///
    /// send_next_operation(&mut self)
    /// send the next operation to the coordinator
    ///
    pub fn send_next_operation(
        &mut self,
        req: message::PtcMessage,
        dest_node: usize,
    ) -> Option<Result<Option<u64>, String>> {
        trace!("Client_{}::send_next_operation", self.id);
        if !self.r.load(Ordering::SeqCst) {
            return None;
        }
        // create a new request with a unique TXID.
        let request_no: i32 = 0; // TODO--choose another number!
        let txid = TXID_COUNTER.fetch_add(1, Ordering::SeqCst);

        info!(
            "Client {} request({})->txid:{} called",
            self.id, request_no, txid
        );

        // info!("client {} calling send...", self.id);
        print!("client {} calling send... to node {}\n", self.id, dest_node);
        if let Err(_) = self.c_to_p_txs[dest_node]
            .clone()
            .unwrap()
            .send_timeout(req.clone(), Duration::from_millis(constants::CLIENT_OUTGOING_REQUEST_TIMEOUT_MS)) {
                println!("RETRYING CLIENT SEND");
            return self.send_next_operation(req, self.select_dest_node());
        }
        match self.p_to_c_rxs[dest_node].clone().unwrap().recv_timeout(Duration::from_millis(constants::CLIENT_INCOMING_RESPONSE_TIMEOUT_MS)) {
            Ok(message::PtcMessage::ParticipantResponse(
                message::ParticipantResponse::SUCCESS(o),
            )) => {
                self.successful_ops = self.successful_ops + 1;
                print!("SUCCESSFUL OPERATION\n");
                Some(Ok(o))
            }
            Ok(message::PtcMessage::ParticipantResponse(message::ParticipantResponse::LEADER(
                leader_id,
            ))) => {
                if leader_id < 0 {
                    // Some(Err("leader ID was -1".into()));
                    print!("resending request\n");
                    self.send_next_operation(req.clone(), dest_node as usize)
                } else {
                    print!(
                        "redirecting message\n"
                    );
                    self.send_next_operation(req.clone(), leader_id as usize)
                }
            }
            Ok(message::PtcMessage::ParticipantResponse(message::ParticipantResponse::ABORT)) => {
                self.failed_ops = self.failed_ops + 1;
                print!("received abort response, trying again.\n");
                self.send_next_operation(req.clone(), self.select_dest_node())
            }
            Ok(_) => {
                self.unknown_ops = self.unknown_ops + 1;
                Some(Err("received invalid message from participant".into()))
            }
            Err(e) => {
                print!("request has timed out, trying again.\n");
                self.send_next_operation(req.clone(), self.select_dest_node()) // select NEW destination node
                // Some(Err(e.to_string())) // TODO: not nice
            }
        }
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all
    /// transaction requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: collect real stats!

        println!(
            "Client_{}:\tC:{}\tA:{}\tU:{}",
            self.id, self.successful_ops, self.failed_ops, self.unknown_ops
        );
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, requests: Vec<message::PtcMessage>) {
        // run the 2PC protocol for each of n_requests

        for req in requests {
            let mut ret_val = None;
            if !self.r.load(Ordering::SeqCst) {
                break;
            }
            
            while let None = ret_val {
                // println!("next req {:?}", req);
                if !self.r.load(Ordering::SeqCst) {
                    break;
                }
                ret_val = self.send_next_operation(req.clone(), self.select_dest_node());
                if let Some(Ok(Some(v))) = ret_val {
                    println!("PARTICIPANT RETURN: {}", v);
                }
            }
        }

        // terminate program once all requests are done
        self.r.store(false, Ordering::SeqCst);

        // wait for signal to exit
        // and then report status
        self.wait_for_exit_signal();
        self.report_status();
    }
}
