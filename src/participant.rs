//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate crossbeam_channel;
use std::time;
use participant::rand::prelude::*;
use std::sync::mpsc;
use std::time::Duration;
use std::sync::atomic::{AtomicI32};
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use message;
use std::collections::HashMap;
use std::thread;
use oplog;
use client;
use self::crossbeam_channel::{Sender, Receiver};
use rpc;


/// 
/// ParticipantState
/// enum for participant 2PC state machine
/// 
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {    
    Leader,
    Follower,
    Candidate,
    //mostly just used so that we can simulate a machine being down :)
    Down,
}


/// Node
/// structure for maintaining per-node state 
/// theoretically, we should go back and rename participant to node
/// for now, this works
#[derive(Debug)]
pub struct Participant {   
    //miscellania
    log: oplog::OpLog,
    //will tell u to stop if u gotta stop
    r: Arc<AtomicBool>,

    //machine information
    id: usize,
    op_success_prob: f64,
    msg_success_prob: f64,
    local_log: Vec<message::Request>,
    
    //participant<->participant communication 
    p_to_p_txs: Vec<Option<Sender<message::ProtocolMessage>>>, // for sending data to other participants
    p_to_p_rxs: Vec<Option<Receiver<message::ProtocolMessage>>>, // for receiving data from other participants

    //participant<->client communication 
    p_to_c_txs: Vec<Option<Sender<message::ProtocolMessage>>>, // for sending data to clients
    c_to_p_rxs: Vec<Option<Receiver<message::ProtocolMessage>>>, // for receiving data from clients
    
    //raft-protocol info
    current_term: u64,
    state: ParticipantState,
    leader_id: i64,

    //stats
    successful_ops: u64,
    unsuccessful_ops: u64,
    unknown_ops: u64,
}

///
/// Participant
/// implementation of per-participant 2PC protocol
/// Required:
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- implements participant side protocol
///
impl Participant {

    /// 
    /// new()
    /// 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course. 
    /// 
    pub fn new(
        i: usize, is: String, 
        p_to_p_txs: Vec<Option<Sender<message::ProtocolMessage>>>, 
        p_to_p_rxs: Vec<Option<Receiver<message::ProtocolMessage>>>, 
        p_to_c_txs: Vec<Option<Sender<message::ProtocolMessage>>>,
        c_to_p_rxs: Vec<Option<Receiver<message::ProtocolMessage>>>,
        logpath: String,
        r: &Arc<AtomicBool>,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64) -> Participant {

        Participant {
            id: i,
            log: oplog::OpLog::new(logpath),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
            state: ParticipantState::Follower,
            p_to_p_txs,
            p_to_p_rxs,
            p_to_c_txs,
            c_to_p_rxs, 
            r: r.clone(),
            local_log: Vec::new(),
            current_term: 0,
            leader_id: -1,
            successful_ops: 0,
            unsuccessful_ops: 0,
            unknown_ops: 0,
        }   
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator.
    /// This variant can be assumed to always succeed.
    /// You should make sure your solution works using this 
    /// variant before working with the send_unreliable variant.
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending.
    /// 
    pub fn send(&mut self, pm: message::ProtocolMessage) -> bool {
        let result: bool = true;

        // TODO

        result
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator, 
    /// with some probability of success thresholded by the 
    /// command line option success_probability [0.0..1.0].
    /// This variant can be assumed to always succeed
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending, but you can use the threshold 
    ///       logic in this implementation below. 
    /// 
    pub fn send_unreliable(&mut self, pm: message::ProtocolMessage) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            result = self.send(pm);
        } else {
            result = false;
        }
        result
    }   
    
    //figure out how communicating w/ client goes.. TODO
    // pub fn respond_to_client(msg: Message) {

    // }
    
    pub fn send_all_nodes_election(msg: rpc::RequestVote) -> bool {
        let yea_count = 0;
    }

    //maybe no need to send all response
    //return value is FAILED SEND ALL RESPONSES
    pub fn send_all_nodes_append(msg: Rpc::AppendEntries) -> Vec<SendAllResponse> {
        //we want to be able to deal with failure responses here
        //idk how to use channels yet hehe
        for chan in p_to_p_txs {
            send_timeout()
        }
        
    }

    pub fn send_all_nodes_unreliable(msg: Rpc::SendAllEntry) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            match msg {
                Rpc::Election(el) => {
                    result = self_all_nodes
                }
            }
            result = send_all.send(pm);
        } else {
            result = false;
        }
        result
    }

    /// 
    /// perform_operation
    /// perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the 
    /// command-line option success_probability. 
    /// 
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic. 
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than 
    ///       bool if it's more convenient for your design).
    /// 
    pub fn perform_operation(&mut self, request: &Option<message::ProtocolMessage>) -> bool {

        trace!("participant::perform_operation");
        match self.state {
            Leader => {
                //communicate with all to send request
                //append 
                let result = message::RequestStatus::Aborted;
            }
            Follower => {
                
                let result = message::RequestStatus::Aborted;

                // wait 
            }
            Candidate => {
                //we're in an election, so we want to abort the request until we have a leader
                let result = message::RequestStatus::Aborted;
            }
        }
        trace!("exit participant::perform_operation");
        result

        

        // let mut result: message::RequestStatus = message::RequestStatus::Unknown;

        // let x: f64 = random();
        // if x > self.op_success_prob {
            
        //     // TODO: fail the request
        //     result = message::RequestStatus::Aborted;
        // } else {

        //     // TODO: request succeeds!
        //     result = message::RequestStatus::Committed;
        // }

        
        // result == message::RequestStatus::Committed
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // TODO: maintain actual stats!
        // this is based off of INDIVIDUAL NODE STATUS
        println!("participant_{}:\tC:{}\tA:{}\tU:{}", self.id, successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("participant_{} waiting for exit signal", self.id);

        // TODO

        trace!("participant_{} exiting", self.id);
    }    

    

    fn follower_action(&mut self) -> ParticipantState {
        //wait on heartbeats from leader
        let mut rng = rand::thread_rng();
        //make this a range
        let rand_time: u64 = rng.gen();
        if self.leader_id == -1 {
            //instead of waiting on a leader, sleep for a random amount of time
            // let timer = Timeout::new(rx[0], rand_time);
            
            let rand_time = gen_range(0.0...10.0);
            let wait_duration = time::Duration::from_millis(rand_time); 
            //wait on a candidate response for this duration 


        }
        else {
            //wait on heartbeats from leader -> if no heartbeat, return and put yourself in the candidate state
            //additionally, listen for something from client. 
        }


        // let heartbeat = thread::spawn(move || {
        //     //listen on all channels for heartbeat? ugh yikes..
            

        // });
    }

    fn leader_action(&mut self) -> ParticipantState {
        ParticipantState::Leader
    }


    fn candidate_action(&mut self) -> ParticipantState {
        ParticipantState::Candidate
    }


    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {
        
        trace!("Participant_{}::protocol", self.id);

        //
        while (true) {
            match state {
                Leader => { 
                    //WILL DO THIS UNTIL IT GOES DOWN
                    //HOW ARE WE SIMULATING GOING DOWN?
                    //wait on client + send if client has something
                    let client_wait = thread::spawn(move || {
                        while true {
                            select! {}

                            //RANDOMLY GENERATE A NUMBER, IF 0 THEN GO DOWN (just exit from true and set state of follower)
                        }

                    });
                    //send heartbeats 
                    //
                    let heartbeat = thread::spawn(move || {

                    });

                }
                Follower => {
                    self.state = follower_action();
                }
                Candidate => {
                    
                }
                Down => {

                }
            }
        }
        

        // while self.r.load(Ordering::SeqCst) {   
        //     let res = self.rx.recv().unwrap();
            
        // }
        

        self.wait_for_exit_signal();
        self.report_status();
    }
}
