//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate crossbeam_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;
use crate::message::RPC;

use self::crossbeam_channel::{Receiver, Select, Sender};
use client;
use constants;
use message;
use oplog;
use participant::rand::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::Duration;

static DEBUG: bool = false;
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
    //tuple of message and term
    local_log: Vec<(message::ClientRequest, usize)>,
    current_value: u64,

    //participant<->participant communication
    p_to_p_txs: Vec<Option<Sender<message::RPC>>>, // for sending data to other participants
    p_to_p_rxs: Vec<Option<Receiver<message::RPC>>>, // for receiving data from other participants

    //participant<->client communication
    p_to_c_txs: Vec<Option<Sender<message::PtcMessage>>>, // for sending data to clients
    c_to_p_rxs: Vec<Option<Receiver<message::PtcMessage>>>, // for receiving data from clients

    //raft-protocol info
    current_term: usize,
    state: ParticipantState,
    leader_id: i64,
    voted_for: i64,

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
impl<'a> Participant {
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
        i: usize,
        is: String,
        p_to_p_txs: Vec<Option<Sender<message::RPC>>>,
        p_to_p_rxs: Vec<Option<Receiver<message::RPC>>>,
        p_to_c_txs: Vec<Option<Sender<message::PtcMessage>>>,
        c_to_p_rxs: Vec<Option<Receiver<message::PtcMessage>>>,
        logpath: String,
        r: &Arc<AtomicBool>,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64,
    ) -> Participant {
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
            current_value: 0,
            unknown_ops: 0,
            voted_for: -1,
        }
    }

    pub fn get_p_to_p_wait_all(
        p_to_p_rxs: &'a Vec<Option<Receiver<message::RPC>>>,
    ) -> (Select<'a>, Vec<&Receiver<RPC>>) {
        let mut selector = Select::new();
        let mut refs = Vec::new();
        for rx in p_to_p_rxs.iter() {
            match rx {
                Some(rx) => {
                    selector.recv(rx);
                    refs.push(rx);
                }
                None => continue,
            }
        }
        (selector, refs)
    }

    pub fn get_c_to_p_wait_all(
        c_to_p_rxs: &'a Vec<Option<Receiver<message::PtcMessage>>>,
    ) -> (Select<'a>, Vec<&Receiver<message::PtcMessage>>) {
        let mut selector = Select::new();
        let mut refs = Vec::new();
        for rx in c_to_p_rxs.iter() {
            match rx {
                Some(rx) => {
                    selector.recv(rx);
                    refs.push(rx);
                }
                None => continue,
            }
        }
        (selector, refs)
    }

    pub fn get_prev_log_term(&self) -> Option<message::ClientRequest> {
        if self.local_log.len() > 0 {
            Some(self.local_log[self.local_log.len() - 1].0)
        } else {
            None
        }
    }

    pub fn get_last_log_index(&self) -> i64 {
        return (self.local_log.len() as i64) - 1;
    }

    //send response to client
    pub fn send_client(&mut self, msg: message::PtcMessage, id: usize) -> bool {
        // send message to client saying whether we have aborted or not
        self.p_to_c_txs[id].clone().unwrap().send_timeout(msg, Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS)).unwrap();
        true
    }

    pub fn send_client_unreliable(&mut self, pm: message::PtcMessage, id: usize) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            result = self.send_client(pm, id);
        } else {
            result = false;
        }
        result
    }

    // message to all participants
    pub fn send_all_nodes(&mut self, msg: message::RPC) -> Result<bool, &str> {
        match msg {
            message::RPC::Election(r) => {
                for i in self.p_to_p_txs.iter() {
                    match i {
                        Some(tx) => {
                            if let Err(_) = tx.send_timeout(msg.clone(), Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS)) {
                                return Ok(false); // TODO: return count of nodes it actually went to? so we know how mnay requests to wait for?
                            }
                        }
                        None => continue,
                    }
                }
                //TODO: possibly handle timeout!
                return Ok(true);
            }
            message::RPC::Request(a) => {
                for i in self.p_to_p_txs.iter() {
                    match i {
                        Some(tx) => {
                            // TODO: help me finny :D
                            if let Err(_) = tx.send_timeout(msg.clone(), Duration::from_millis(constants::LEADER_APPEND_ENTRIES_TIMEOUT_MS)) {
                                return Ok(false);
                            }
                        }
                        None => continue,
                    }
                }
                //TODO: possibly handle timeout!
                return Ok(true);
            }
            message::RPC::ElectionResp(_) => {
                Err("not supposed to send election response to all nodes")
            }
            message::RPC::RequestResp(_) => {
                Err("not supposed to send request response to all nodes")
            }
        }
    }

    pub fn send_all_nodes_unreliable(&mut self, msg: message::RPC) -> Result<bool, &str> {
        let x: f64 = random();
        if x < self.msg_success_prob {
            // TODO: this unreliability should take place for each node, not for all nodes at once
            let result_result = self.send_all_nodes(msg);
            match result_result {
                Err(e) => Err(e),
                Ok(_) => Ok(true),
            }
        } else {
            Err("failed to send message")
        }
    }

    pub fn get_value_log(&mut self) -> u64 {
        let mut ret_val: u64 = 0;
        for (req, _) in self.local_log.iter() {
            match req {
                message::ClientRequest::SET(v) => {
                    ret_val = v.clone();
                }
                message::ClientRequest::ADD(v) => {
                    ret_val += v.clone();
                }
                _ => (),
            }
        }
        ret_val
    }

    pub fn get_value(&mut self) -> u64 {
        self.current_value
    }

    ///
    /// perform_operation
    /// perform the operation sent by client
    /// this is where we will do the
    /// respond to the client here
    pub fn perform_operation(
        &mut self,
        request: &Option<message::PtcMessage>,
        client_id: usize,
    ) -> bool {
        trace!("participant::perform_operation");

        let mut operation_completed = false;
        let mut result =
            message::PtcMessage::ParticipantResponse(message::ParticipantResponse::ABORT);

        match request {
            Some(message::PtcMessage::ClientRequest(c)) => {
                
                match self.state {
                    ParticipantState::Leader => {
                        // print!("leader has received a request\n");
                        //communicate with all to send request
                        match request {
                            Some(message::PtcMessage::ClientRequest(
                                message::ClientRequest::ADD(n),
                            )) => {
                                self.current_value += n;
                            }
                            Some(message::PtcMessage::ClientRequest(
                                message::ClientRequest::SET(n),
                            )) => {
                                self.current_value = *n;
                            }
                            _ => (),
                        }

                        for i in self.p_to_p_txs.clone() {
                            let append_entry = message::AppendEntries {
                                term: self.current_term,
                                leader_id: self.leader_id,
                                prev_log_index: self.get_last_log_index(),
                                prev_log_term: self.get_prev_log_term(),
                                heartbeat: false,
                                leader_commit: self.local_log.len(),
                                entries: Some(*c),
                                current_leader_val: self.current_value,
                            };

                            match i {
                                Some(channel) => {
                                    // I think this is the right constant in this case, but correct me if I am wrong!
                                    if let Err(_) = channel.send_timeout(message::RPC::Request(append_entry), Duration::from_millis(constants::LEADER_APPEND_ENTRIES_TIMEOUT_MS)) {
                                        // do nothing.
                                        // println!("unable to send request for majority agreement on value");
                                        ()
                                    }
                                }
                                None => continue,
                            }
                        }

                        //now, we need to wait on responses for a majority
                        let majority = self.p_to_p_rxs.len() / 2 + 1;
                        let mut num = 1;
                        while num < majority {
                            if !self.r.load(Ordering::SeqCst) {
                                // println!("in here");
                                return false;
                            }
                            //MAKE SURE THIS TIMEOUT IS A GOOD DURATION
                            let (mut selector, refs) = Self::get_p_to_p_wait_all(&self.p_to_p_rxs);
                            let msg = selector.select_timeout(Duration::from_millis(
                                constants::LEADER_MAJORITY_TIMEOUT_MS,
                            ));
                            // print!("communicating with followers\n");
                            match msg {
                                Ok(so) => {
                                    
                                    let sel_index = so.index();
                                    let resp = so.recv(refs[sel_index]);
                                    match resp {
                                        Ok(rpc) => {
                                            match rpc {
                                                message::RPC::RequestResp(aer) => {
                                                    // println!("node {} (current term: {}) received vote: {:?}", self.id, self.current_term, aer);
                                                    if aer.success {
                                                        num += 1;
                                                    } else {
                                                        // ignore
                                                        // in the end, should fix log
                                                        continue;
                                                    }
                                                }
                                                //prob should worry about throwing things out but... oh well
                                                //only thing we could possibly receive is an election response, which would be weird
                                                r => {
                                                    // print!(
                                                    //     "waiting on follower messages but the message is not an append entries response {:?}\n",
                                                    //     r,
                                                    // );
                                                    continue;
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            // if we are in here, we have seriously messed up again
                                            continue;
                                        }
                                    }
                                }
                                Err(err) => {
                                    // println!("num is {}; majorti vote not received", num);
                                    // we couldn't get a majority, so we return false --
                                    // there may be inconsistent state
                                    result = message::PtcMessage::ParticipantResponse(
                                        message::ParticipantResponse::ABORT,
                                    );
                                    //make sure unwrap doesn't panic
                                    if let Err(_) = self.p_to_c_txs[client_id].clone().unwrap().send_timeout(result, Duration::from_millis(constants::RESPOND_TO_CLIENTS_TIMEOUT)) {
                                        // do nothing again? what do we even do if a client is down... idk
                                        if DEBUG { print!("communication with clients has failed\n"); }
                                        ()
                                    }
                                    return false;
                                }
                            }
                        }
                        operation_completed = true;
                        let append_to_success = match c {
                            message::ClientRequest::GET => Some(self.get_value_log()),
                            _ => None,
                        };
                        self.local_log.push((*c, self.current_term));

                        result = message::PtcMessage::ParticipantResponse(
                            message::ParticipantResponse::SUCCESS(append_to_success),
                        );
                        //append
                        // let result = message::ParticipantResponse::ABORT;
                    }
                    ParticipantState::Follower => {
                        // print!("follower has received a request\n");
                        // need to reroute to leader
                        // if leader is -1, then aborted
                        if self.leader_id == -1 {
                            result = message::PtcMessage::ParticipantResponse(
                                message::ParticipantResponse::ABORT,
                            );
                        } else {
                            result = message::PtcMessage::ParticipantResponse(
                                message::ParticipantResponse::LEADER(self.leader_id),
                            );
                            // self.p_to_p_txs[self.leader_id].send(client_slide_rpc);
                        }
                        // wait
                    }
                    _ => (),
                }
                trace!("exit participant::perform_operation");
                self.send_client_unreliable(result, client_id);
                return operation_completed;
            }
            _ => false,
        }
    }

    fn candidate_action(&mut self) -> ParticipantState {
        // print!(
        //     "node {} is a candidate now\n", self.id
        // );
        let vote_me = message::RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.get_last_log_index(),
            last_log_term: self.get_prev_log_term(),
        };
        for i in self.p_to_p_txs.clone() {
            match i {
                Some(tx) => {
                    // create RequestVote
                    match tx.send_timeout(message::RPC::Election(vote_me.clone()), Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS)){
                        _ => (),
                    }
                }
                None => {
                    continue;
                }
            }
        }
        let majority = self.p_to_p_rxs.len() / 2 + 1;
        let mut num = 1;
        while num < majority {
            if !self.r.load(Ordering::SeqCst) {
                // println!("in here!!!");
                return self.state;
            }
            let (mut selector, refs) = Self::get_p_to_p_wait_all(&self.p_to_p_rxs);
            let msg = selector
                .select_timeout(Duration::from_millis(constants::LEADER_MAJORITY_TIMEOUT_MS));
            match msg {
                Ok(index) => {
                    let indix = index.index();
                    let resp = index.recv(refs[indix]);
                    match resp {
                        Ok(message::RPC::ElectionResp(er)) => {
                            if er.vote_granted {
                                num += 1;
                            } else {
                                continue;
                            }
                        }
                        // JUST CHANGED THIS MAKE SURE CORRECT
                        //TODO: respond with no vote
                        Ok(message::RPC::Election(e)) => (),
                        Ok(_) => {
                            self.voted_for = -1;
                            return ParticipantState::Follower;
                                 }, // TODO: double check if this should be unit?
                        Err(_) => (),
                        _ => (),
                    }
                }
                Err(_) => {
                    // println!("NO MESSAGE");
                    self.voted_for = -1;
                    return ParticipantState::Follower;
                }
            }
        }
        // TODO: update current_term?
        self.leader_id = self.id as i64;
        self.current_term = self.current_term + 1;
        return ParticipantState::Leader;
    }

    // this is what a follower is up to
    fn follower_action(&mut self) -> ParticipantState {
        
        //listens for messages from 1. leader 2. clients rxs (reroute packets to leader :))
        while self.r.load(Ordering::SeqCst) {
            // print!(
            //     "node {} is a follower now\n", self.id
            // );
            //LISTEN FOR HEARTBEAT
            let mut received_heartbeat = false;
            // OPTIMIZATION: at the beginning of every loop, wait for either leader heartbeat OR client request (so that client requests don't
            // get held up)
            let wait_val = if self.leader_id == -1 {
                let mut rng = rand::thread_rng();
                rng.gen_range(0..1000)
            } else {
                constants::FOLLOWER_TIMEOUT_MS
            };

            let p_to_p_rxs_cloned = self.p_to_p_rxs.clone();
            let (mut selector, refs) = Self::get_p_to_p_wait_all(&p_to_p_rxs_cloned);
            let msg = selector.select_timeout(Duration::from_millis(wait_val));
            match msg {
                Ok(so) => {
                    let idx = so.index();
                    let data = so.recv(refs[idx]);
                    match data {
                        Ok(message::RPC::Request(ae)) => {
                            // println!("HEartbeat received.");
                            received_heartbeat = true;
                            if ae.term > self.current_term {
                                self.current_term = ae.term;
                            }
                            self.leader_id = ae.leader_id;
                            
                            // TODO: we must update state
                            if !ae.heartbeat {
                                // must be a request for a vote on a client request if not a heartbeat
                                let success = ae.leader_id == self.leader_id &&
                                    // ae.prev_log_index == (self.local_log.len() as i64) - 1 &&
                                    // ae.prev_log_term == self.get_prev_log_term() &&
                                    ae.term >= self.current_term;
                                self.current_value = ae.current_leader_val;
                                self.p_to_p_txs[self.leader_id as usize]
                                    .clone()
                                    .unwrap()
                                    .send_timeout(message::RPC::RequestResp(message::AppendEntriesResponse {
                                        term: self.current_term,
                                        success,
                                    }), Duration::from_millis(constants::FOLLOWER_TO_LEADER_COMM))
                                    .unwrap();
                            }
                        }
                        Ok(message::RPC::Election(e)) => {
                            // println!("reguest vote response\n");

                            let vote_g = e.term <= self.current_term && (self.voted_for == -1 || self.voted_for == e.candidate_id as i64);
                            if vote_g {
                                self.voted_for = e.candidate_id as i64;
                            }
                            
                            let resp = message::RequestVoteResponse {
                                term: self.current_term,
                                vote_granted: vote_g,
                            };
                            let send_resp = message::RPC::ElectionResp(resp);
                            let mut chan_index = idx;
                            if chan_index >= self.id {
                                chan_index += 1;
                            }
                            
                            self.p_to_p_txs[chan_index]
                                .clone()
                                .unwrap()
                                .send_timeout(send_resp, Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS))
                                .unwrap();
                        },
                        Ok(message::RPC::ElectionResp(_)) => {
                            // don't panic: election response may be from when this was previously a candidat
                        },
                        Ok(req) => {
                            panic!("follower received invalid request {:?}", req);
                        }
                        Err(_) => {
                            
                        }
                    }
                }, 
                Err(_) => { }
            }

            if !received_heartbeat {
                //listen for x (random) seconds for a election request
                //if no election request, then propose yourself :)
                //break out and set state to candidate

                //nop timeout because that would be unfair and add an extra layer of complexity
                let (mut selector, refs) = Self::get_p_to_p_wait_all(&self.p_to_p_rxs);
                let resp = selector.try_select();
                self.leader_id = -1;
                // println!("HEART BEAT NOT RECEIVED, AHHHHH GO CRAZY!");
                
                self.voted_for = -1;
                match resp {
                    Ok(so) => {
                        let sel_index = so.index();
                        let mut chan_index = so.index();
                        if chan_index >= self.id {
                            chan_index = chan_index + 1;
                        }

                        let msg = so.recv(refs[sel_index]);
                        match msg {
                            Ok(message::RPC::Election(e)) => {
                                let resp = message::RequestVoteResponse {
                                    term: self.current_term,
                                    vote_granted: e.term < self.current_term,
                                };
                                let send_resp = message::RPC::ElectionResp(resp);
                                self.p_to_p_txs[chan_index]
                                    .clone()
                                    .unwrap()
                                    .send_timeout(send_resp, Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS))
                                    .unwrap();
                                //dont want to handle client stuff until leader is established...?
                                continue;
                            }
                            Ok(message::RPC::Request(r)) => {
                                //freak out
                                // ignore AppendEntries query, proceed to election (alternative: could treat this as a heartbeat and respond to leader despite it being late)
                                return ParticipantState::Candidate;
                            }
                            Err(_) => {
                                return ParticipantState::Candidate;
                            }
                            _ => {
                                return ParticipantState::Candidate;
                            }
                        }
                    }
                    Err(_) => {
                        // no response
                        // we will break and enter the candidate state
                        break;
                    }
                }
            }

            // LISTEN ON CLIENTS, IF A CLIENT MISDIRECTS A MESSAGE, SEND LEADER ID
            let copied_channels = self.c_to_p_rxs.clone();
            let (mut listen_to_me, refs) = Self::get_c_to_p_wait_all(&copied_channels);
            let msg =
                listen_to_me.select_timeout(Duration::from_millis(constants::FOLLOWER_TIMEOUT_MS));
            match msg {
                Ok(so) => {
                    let idx = so.index();
                    let msg = so.recv(&refs[idx]);
                    match msg {
                        Ok(m) => {
                            // print!("calling perform operation\n");
                            self.perform_operation(&Some(m), idx);
                        }
                        Err(_) => {
                            panic!("unable to receive data from selector");
                        }
                    }
                }
                Err(_) => {
                    // print!("tooth");
                }
            }
        }

        return ParticipantState::Candidate;
    }

    fn leader_action(&mut self) -> ParticipantState {
        //TODO: if receive an election request, no longer follower
        while self.r.load(Ordering::SeqCst) {
            // print!(
            //     "node {} is a leader now\n", self.id
            // );
            // wait to receive from any of the clients (selector), with a timeout
            let c_to_p_rxs_clone = self.c_to_p_rxs.clone();
            let (mut selector, refs) = Self::get_c_to_p_wait_all(&c_to_p_rxs_clone);
            let select_res = selector.select_timeout(Duration::from_millis(
                constants::LEADER_CLIENT_REQ_TIMEOUT_MS,
            ));
            let should_send_heartbeat = match select_res {
                Ok(op) => {
                    // received client request, so action it
                    let idx = op.index();
                    match op.recv(refs[idx]) {
                        Ok(req) => {
                            // println!("got client request {:?}", req);
                            self.perform_operation(&Some(req), idx);
                            false
                        }
                        Err(e) => true,
                    }
                }
                Err(e) => {
                    // timed out, so send heartbeat to followers
                    true
                }
            };

            if should_send_heartbeat {
                match self.send_all_nodes_unreliable(message::RPC::Request(
                    message::AppendEntries {
                        term: self.current_term,
                        prev_log_index: self.get_last_log_index(),
                        leader_id: self.leader_id,
                        prev_log_term: self.get_prev_log_term(),
                        entries: None,
                        heartbeat: true,
                        leader_commit: self.local_log.len(),
                        current_leader_val: self.current_value,
                    },
                )) {
                    _ => continue,
                }
            }
            //here, listen for election requests
            // let () = get_p_to_p_wait_all
        }
        ParticipantState::Leader
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("Participant_{}::protocol", self.id);

        while self.r.load(Ordering::SeqCst) {
            match self.state {
                ParticipantState::Leader => {
                    self.state = self.leader_action();
                }
                ParticipantState::Follower => {
                    self.state = self.follower_action();
                }
                ParticipantState::Candidate => {
                    self.state = self.candidate_action();
                }
                ParticipantState::Down => {
                    //TODO
                    continue;
                }
            }
        }

        // while self.r.load(Ordering::SeqCst) {
        //     let res = self.rx.recv().unwrap();

        // }

        self.wait_for_exit_signal();
        self.report_status();
    }

    pub fn report_status(&self) {
        println!(
            "participant_{}:\tC:{}\tA:{}\tU:{}\tLEADER:{}\tSTATE:{:?}",
            self.id, self.successful_ops, self.unsuccessful_ops, self.unknown_ops, self.leader_id, self.state,
        );
        
    }

    pub fn wait_for_exit_signal(&mut self) {
        trace!("participant_{} waiting for exit signal", self.id);
        // TODO
        trace!("participant_{} exiting", self.id);
    }
}
