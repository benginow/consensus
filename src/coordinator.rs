//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate rand;
extern crate stderrlog;
use coordinator::rand::prelude::*;
use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;
use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

/// CoordinatorState
/// States for 2PC state machine
///
/// TODO: add and/or delete!
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    // TODO...
}

/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    log: oplog::OpLog,
    msg_success_prob: f64,
    pub client_channels: Vec<(
        Sender<message::ProtocolMessage>,
        Receiver<message::ProtocolMessage>,
    )>,
    pub participant_channels: Vec<(
        Sender<message::ProtocolMessage>,
        Receiver<message::ProtocolMessage>,
    )>,
    r: Arc<AtomicBool>,
    // TODO: ...
}

///
/// Coordinator
/// implementation of coordinator functionality
/// Required:
/// 1. new -- ctor
/// 2. protocol -- implementation of coordinator side of protocol
/// 3. report_status -- report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- what to do when a participant joins
/// 5. client_join -- what to do when a client joins
///
impl Coordinator {
    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     logpath: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///     msg_success_prob --> probability sends succeed
    ///
    pub fn new(logpath: String, r: &Arc<AtomicBool>, msg_success_prob: f64) -> Coordinator {
        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(logpath),
            msg_success_prob: msg_success_prob,
            client_channels: Vec::new(),
            participant_channels: Vec::new(),
            r: r.clone(),
            // TODO...
        }
    }

    ///
    /// participant_join()
    /// handle the addition of a new participant
    /// HINT: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    ///
    pub fn participant_join(&mut self, name: &String) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
    }

    ///
    /// client_join()
    /// handle the addition of a new client
    /// HINTS: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    ///
    pub fn client_join(&mut self, name: &String) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
    }

    ///
    /// send()
    /// send a message, maybe drop it
    /// HINT: you'll need to do something to implement
    ///       the actual sending!
    ///
    pub fn send(&self, sender: &Sender<ProtocolMessage>, pm: ProtocolMessage) -> bool {
        let x: f64 = random();
        let mut result: bool = true;
        if x < self.msg_success_prob {
            sender.send(pm).unwrap();
        } else {
            // don't send anything!
            // (simulates failure)
            result = false;
        }
        result
    }

    ///
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    ///
    pub fn recv_request(&mut self) -> Option<ProtocolMessage> {
        let mut result = Option::None;
        assert!(self.state == CoordinatorState::Quiescent);
        trace!("coordinator::recv_request...");

        for (_, rx) in &self.client_channels {
            match rx.try_recv() {
                Ok(pm) => {
                    result = Some(pm);
                    break;
                }
                Err(e) => match e {
                    TryRecvError::Empty => (),
                    TryRecvError::Disconnected => panic!("channel disconnected!"),
                },
            }
        }

        trace!("leaving coordinator::recv_request");
        result
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all
    /// transaction requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        let successful_ops: usize = 0; // TODO!
        let failed_ops: usize = 0; // TODO!
        let unknown_ops: usize = 0; // TODO!
        println!(
            "coordinator:\tC:{}\tA:{}\tU:{}",
            successful_ops, failed_ops, unknown_ops
        );
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        while self.r.load(Ordering::SeqCst) {
            match self.recv_request() {
                Option::None => (),
                Option::Some(pm) => {
                    if let MessageType::ClientRequest = pm.mtype {
                        for (part_tx, _) in &self.participant_channels {
                            let participant_req_msg = message::ProtocolMessage::generate(
                                message::MessageType::CoordinatorPropose,
                                TXID_COUNTER.fetch_add(1, Ordering::SeqCst),
                                "Coordinator".into(),
                                0, // TODO: fix?
                            );
                            self.send(part_tx, participant_req_msg);
                        }
                    } else {
                        panic!("received non-client request from client!");
                    }
                }
            }
        }
        // TODO!

        self.report_status();
    }
}
