//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate crossbeam_channel;
extern crate shuttle;
extern crate log;
extern crate rand;
extern crate stderrlog;
use crate::{message::{RPC, AppendEntriesResponse, ParticipantResponse, PtcMessage}, participant};

use shuttle::crossbeam_channel::{unbounded, Receiver, Select, Sender, SelectedOperation};
use client;
use constants;
use message;
use oplog;
use participant::rand::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use shuttle::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time;
use std::time::Duration;

static DEBUG: bool = false;
pub static leader_ct_per_term: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(vec![]);
// each index of outer vector represents a given term; HashMap maps from voter to list of votees in that given term

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

pub enum UnreliableReceiveError {
    TermOutOfDate(usize),
    RecvError,
}

pub enum PerformOperationResult {

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

    // Generates some random permutation of the numbers from 0..(ct-1), inclusive.
    pub fn gen_permutation(ct: usize) -> Vec<usize>{
        let mut rng = rand::thread_rng();
        let mut ret_perm = Vec::new();
        while ret_perm.len() < ct {
            let next_val = rng.gen_range(0..ct);
            if !ret_perm.contains(&next_val) {
                ret_perm.push(next_val);
            }
        }
        ret_perm
    }

    pub fn set_current_term(&mut self, new_term: usize) {
        assert!(self.current_term <= new_term); // monotonic invariant

        if self.current_term != new_term {
            self.voted_for = -1;
        }
        self.current_term = new_term;
    }
    
    /// Returns a selector to wait on either client or participant messages.
    /// The selector stores the client and participant channels in a randomly permuted order.
    /// 
    /// The returned client and participant receivers, encode the order of storage. If 
    /// the stored value at a given index in either of the vectors is not None, then that
    /// index stores a tuple given as (original index, receiver) representing the original index 
    /// in either the client or participant receivers from which the receiver came alongside
    /// the actual receiver.
    pub fn get_all_to_p_wait_all(
        c_to_p_rxs: &'a Vec<Option<Receiver<message::PtcMessage>>>,
        p_to_p_rxs: &'a Vec<Option<Receiver<message::RPC>>>,
    ) -> (Select<'a>, Vec<Option<(usize, &'a Receiver<message::PtcMessage>)>>, Vec<Option<(usize, &'a Receiver<message::RPC>)>>) {
        let selectables_ct = c_to_p_rxs.len() + p_to_p_rxs.len();
        let permutation = Self::gen_permutation(selectables_ct);

        let mut selector = Select::new();
        let mut c_to_p_refs = vec![None; selectables_ct];
        let mut p_to_p_refs = vec![None; selectables_ct];

        for (i, val) in permutation.iter().enumerate() {
            if *val < c_to_p_rxs.len() { // work with client
                let rx = &c_to_p_rxs[*val];
                match rx {
                    Some(rx) => {
                        selector.recv(&rx);
                        c_to_p_refs[i] = Some((*val, rx));
                    }
                    None => continue,
                }
            } else {  // work with participant
                let rx = &p_to_p_rxs[*val - c_to_p_rxs.len()];
                match rx {
                    Some(rx) => {
                        selector.recv(&rx);
                        p_to_p_refs[i] = Some((*val, rx));
                    }
                    None => continue,
                }
            }
        }
        (selector, c_to_p_refs, p_to_p_refs)
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

    // Performs an RPC for an arbitrary RPC request whilst handling the case where the given term is out of date.
    pub fn recv_unreliable_rpc(&self, so: SelectedOperation, r: &Receiver<message::RPC>) -> Result<message::RPC, UnreliableReceiveError> 
    {
        let msg = so.recv(r);
        let term = match msg {
            Ok(message::RPC::Election(rv)) => {
                Ok(rv.term)
            }, 
            Ok(message::RPC::Request(ae)) => {
                Ok(ae.term)
            }
            Ok(message::RPC::ElectionResp(rvr)) => {
                Ok(rvr.term)
            },
            Ok(message::RPC::RequestResp(aer)) => {
                Ok(aer.term)
            },
            Err(_) => {
                Err(UnreliableReceiveError::RecvError)
            }
        };

        match term {
            Ok(t) => {
                if t > self.current_term {
                    Err(UnreliableReceiveError::TermOutOfDate(t))
                } else {
                    Ok(msg.unwrap())
                }
            },
            Err(e) => Err(e),
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

    // Messages all participants who have NOT responded yet (i.e. those who have value false in the responded_participants vec).
    pub fn message_all_participants(&mut self, append_entries: message::AppendEntries, responded_participants: Vec<bool>) {
        for (idx, chan)  in self.p_to_p_txs.clone().iter().enumerate() {
            if responded_participants[idx] {
                continue;
            }

            match chan {
                Some(channel) => {
                    // I think this is the right constant in this case, but correct me if I am wrong!
                    if let Err(_) = channel.send_timeout(message::RPC::Request(append_entries), Duration::from_millis(constants::LEADER_APPEND_ENTRIES_TIMEOUT_MS)) {
                        // do nothing.
                        // println!("unable to send request for majority agreement on value");
                        ()
                    }
                }
                None => continue,
            }
        }
    }

    pub fn service_client_request_leader(&mut self, 
        request: message::ClientRequest, 
        client_id: usize,
    ) -> Result<Option<message::ParticipantResponse>, UnreliableReceiveError> {
        let result = {
            // print!("leader has received a request\n");
            //communicate with all to send request
            match request {
                message::ClientRequest::ADD(n) => {
                    self.current_value += n;
                }
                message::ClientRequest::SET(n) => {
                    self.current_value = n;
                }
                _ => (),
            }

            let mut responded_participants = vec![false; self.p_to_p_rxs.len()];

            let append_entries = message::AppendEntries {
                term: self.current_term,
                leader_id: self.leader_id,
                heartbeat: false,
                entries: Some(request),
                current_leader_val: self.current_value,
            };

            self.message_all_participants(append_entries, responded_participants.clone());

            //now, we need to wait on responses for a majority
            let majority = self.p_to_p_rxs.len() / 2 + 1;
            let mut num = 1;
            while num < majority { 
                if !self.r.load(Ordering::SeqCst) {
                    return Ok(None);
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
                        let resp = self.recv_unreliable_rpc(so, refs[sel_index]);
                        match resp {
                            Ok(rpc) => {
                                match rpc {
                                    message::RPC::RequestResp(aer) => {
                                        // println!("node {} (current term: {}) received vote: {:?}", self.id, self.current_term, aer);
                                        if aer.success {
                                            responded_participants[sel_index] = true;
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
                            },
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    Err(err) => {
                        // println!("num is {}; majorti vote not received", num);
                        // we couldn't get a majority, so we return false --
                        // there may be inconsistent state
                        //make sure unwrap doesn't panic
                        // result_temp = message::PtcMessage::ParticipantResponse(
                        //     message::ParticipantResponse::ABORT,
                        // );
                        if let Err(_) = self.p_to_c_txs[client_id].clone().unwrap().send_timeout(message::PtcMessage::ParticipantResponse(
                                message::ParticipantResponse::ABORT,
                            ), Duration::from_millis(constants::RESPOND_TO_CLIENTS_TIMEOUT)) {
                            // do nothing again? what do we even do if a client is down... idk
                            if DEBUG { print!("communication with clients has failed\n"); }
                            ()
                        }
                        return Ok(None);
                    }
                }

                // retry message for all participants which have NOT responded yet
                self.message_all_participants(append_entries, responded_participants.clone());
            };
            let append_to_success = match request {
                message::ClientRequest::GET => Some(self.get_value_log()),
                _ => None,
            };

            //append
            Some(
                message::ParticipantResponse::SUCCESS(append_to_success)
            )
        };
        Ok(result)
    }
    
    ///
    /// perform_operation
    /// perform the operation sent by client
    /// this is where we will do the
    /// respond to the client here
    pub fn service_client_request_follower(&self) -> Result<Option<message::ParticipantResponse>, UnreliableReceiveError> {
        trace!("participant::perform_operation");

        if self.leader_id == -1 {
            Ok(Some(message::ParticipantResponse::ABORT))
        } else {
            Ok(Some(message::ParticipantResponse::LEADER(self.leader_id)))
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
        let mut voting_yes_nodes = Vec::new();
        voting_yes_nodes.push(self.id);
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
                    let resp = self.recv_unreliable_rpc(index, refs[indix]);
                    match resp {
                        Ok(message::RPC::ElectionResp(er)) => {
                            assert!(self.id == er.intended_part_temp);
                            if er.vote_granted && er.term == self.current_term {
                                num += 1;
                                assert!(!voting_yes_nodes.contains(&er.sender_id));
                                // assert!(er.term <= self.current_term);
                                voting_yes_nodes.push(er.sender_id);
                            } else {
                                continue;
                            }
                        }
                        // JUST CHANGED THIS MAKE SURE CORRECT
                        //TODO: respond with no vote
                        Ok(message::RPC::Election(e)) => {
                            assert!(self.voted_for != e.candidate_id as i64);

                            let resp = message::RPC::ElectionResp(message::RequestVoteResponse {
                                term: self.current_term,
                                vote_granted: false,
                                sender_id: self.id,
                                intended_part_temp: e.candidate_id,
                            });
                            let mut chan_idx = indix;
                            if chan_idx >= self.id {
                                chan_idx += 1;
                            }
                            self.p_to_p_txs[chan_idx].clone().unwrap().send_timeout(resp, Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS)).unwrap();
                        },
                        Ok(message::RPC::Request(ae)) => {
                            if ae.term >= self.current_term {
                                // TODO: why is it possible for a new leader to emerge with smaller term than an existing candidate?
                                self.leader_id = ae.leader_id;
                                return ParticipantState::Follower;
                            }
                        }, // TODO: double check if this should be unit?
                        Err(UnreliableReceiveError::TermOutOfDate(t)) => {
                            self.set_current_term(t);
                            return ParticipantState::Follower; // early return: follower
                        }
                        Err(_) => {
                            return ParticipantState::Candidate; // election timeout exceeded, so start new election
                        },
                        _ => (),
                    }
                }
                Err(_) => {
                    // println!("NO MESSAGE");
                    return ParticipantState::Follower;
                }
            }
        }
        // TODO: update current_term?
        assert!(num >= majority);
        assert!(voting_yes_nodes.len() >= majority);
        self.leader_id = self.id as i64;
        ParticipantState::Leader
    }

    // this is what a follower is up to
    fn follower_action(&mut self) -> ParticipantState {
        //listens for messages from 1. leader 2. clients rxs (reroute packets to leader :))
        while self.r.load(Ordering::SeqCst) {
            // print!(
            //     "node {} is a follower now\n", self.id
            // );
            //LISTEN FOR HEARTBEAT
            // OPTIMIZATION: at the beginning of every loop, wait for either leader heartbeat OR client request (so that client requests don't
            // get held up)
            let wait_val = if self.leader_id == -1 {
                let mut rng = rand::thread_rng();
                rng.gen_range(0..1000)
            } else {
                constants::FOLLOWER_TIMEOUT_MS
            };

            let cloned_c_to_p = &self.c_to_p_rxs.clone();
            let cloned_p_to_p = &self.p_to_p_rxs.clone();
            let (mut selector, p_refs) = Self::get_p_to_p_wait_all(cloned_p_to_p);
            let msg = selector.select_timeout(Duration::from_millis(wait_val));
            match msg {
                Ok(so) => {
                    let idx = so.index();
                    self.follower_participant_action(idx, so, p_refs[idx]);
                },
                Err(e) => { 
                    return ParticipantState::Candidate;
                }
            }
        }
        ParticipantState::Follower
    }

    fn follower_participant_action(&mut self, idx: usize, so: SelectedOperation, rx: &Receiver<RPC>) -> ParticipantState {
        let mut received_heartbeat = false;
        let data = self.recv_unreliable_rpc(so, rx);
        match data {
            Ok(message::RPC::Request(ae)) => {
                if ae.term < self.current_term {
                    return ParticipantState::Follower; // discard, retry
                }

                received_heartbeat = true;
                self.leader_id = ae.leader_id;
                
                // TODO: we must update state
                if !ae.heartbeat {
                    // must be a request for a vote on a client request if not a heartbeat
                    let success = ae.leader_id == self.leader_id &&
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
                ParticipantState::Follower
            }
            Ok(message::RPC::Election(e)) => {
                // println!("reguest vote response\n");
                let vote_g = e.term >= self.current_term && (self.voted_for == -1 || self.voted_for == e.candidate_id as i64);
                if vote_g {
                    // println!("node {:?} voting for {:?} in term {:?} {:?}", self.id, e.candidate_id, self.current_term, e.term);
                    if self.voted_for == -1 {
                        self.voted_for = e.candidate_id as i64;
                    }
                }
                assert!(!vote_g || e.term == self.current_term);
                
                let resp = message::RPC::ElectionResp(message::RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: vote_g,
                    sender_id: self.id,
                    intended_part_temp: e.candidate_id,
                });
                let mut chan_index = idx;
                if chan_index >= self.id {
                    chan_index += 1;
                }
                
                self.p_to_p_txs[chan_index]
                    .clone()
                    .unwrap()
                    .send_timeout(resp, Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS))
                    .unwrap();
                
                ParticipantState::Follower
            },
            Ok(message::RPC::ElectionResp(_)) => {
                // don't panic: election response may be from when this was previously a candidat
                ParticipantState::Follower
            },
            Ok(req) => {
                println!("follower received invalid request {:?}", req);
                ParticipantState::Follower
            }
            Err(UnreliableReceiveError::TermOutOfDate(new_term)) => {
                self.set_current_term(new_term);
                ParticipantState::Follower // early return: follower
            },
            Err(_) => {
                ParticipantState::Candidate
            }
        }
    }

    fn follower_client_action(&mut self, idx: usize, so: SelectedOperation, rx: &Receiver<PtcMessage>) -> ParticipantState {
        // LISTEN ON CLIENTS, IF A CLIENT MISDIRECTS A MESSAGE, SEND LEADER ID
        let msg = so.recv(rx);
        match msg {
            Ok(m) => {
                // print!("calling perform operation\n");
                match self.service_client_request_follower() {
                    Err(UnreliableReceiveError::TermOutOfDate(t)) => {
                        self.set_current_term(t);
                        return ParticipantState::Follower;
                    }, 
                    Ok(Some(pr))=> {
                        self.send_client_unreliable(message::PtcMessage::ParticipantResponse(pr), idx);
                    },
                    _ => {

                    }
                }
            }
            Err(_) => {
                // panic!("unable to receive data from selector");
            }
        }
        return ParticipantState::Follower;
    }

    fn leader_action(&mut self) -> ParticipantState {
        //TODO: if receive an election request, no longer follower
        while self.r.load(Ordering::SeqCst) {
            // print!(
            //     "node {} is a leader now\n", self.id
            // );
            // wait to receive from any of the clients (selector), with a timeout
            let c_to_p_rxs_clone = self.c_to_p_rxs.clone();
            let p_to_p_rxs_clone = self.p_to_p_rxs.clone();

            let (mut selector, c_to_p_refs, p_to_p_refs) = Self::get_all_to_p_wait_all(&c_to_p_rxs_clone, &p_to_p_rxs_clone);
            let select_res = selector.select_timeout(Duration::from_millis(
                constants::LEADER_CLIENT_REQ_TIMEOUT_MS,
            ));
            let should_send_heartbeat = true; // for now: always send heartbeats
            match select_res {
                Ok(op) => {
                    if let Some((idx, rx)) = c_to_p_refs[op.index()] {   // received client request
                        match op.recv(rx) {
                            Ok(req) => {
                                println!("leader got client request {:?}", req);
                                match req {
                                    message::PtcMessage::ClientRequest(cr) => {
                                        match self.service_client_request_leader(cr, idx) {
                                            Err(UnreliableReceiveError::TermOutOfDate(t)) => {
                                                // update term. revert to follower.
                                                self.set_current_term(t);
                                                return ParticipantState::Follower;
                                            }, 
                                            Ok(Some(o)) => { // TODO: when would this be None?
                                                // respond to client.
                                                self.send_client(message::PtcMessage::ParticipantResponse(o), idx);
                                            },
                                            _ => {
                                                // TODO: RecvError?
                                            }
                                        }
                                        false
                                    },
                                    _ => {
                                        panic!("received participant response from client..");
                                    }
                                }
                            }
                            Err(e) => true,
                        }
                    } else if let Some((idx, rx)) = p_to_p_refs[op.index()] { // received participant request
                        // idx represents an index in the all_to_p selector, which starts with all c_to_p channels and is followed by p_to_p channels
                        match self.recv_unreliable_rpc(op, rx) {
                            Ok(req) => {
                                println!("leader got participant request {:?}", req);
                                match req {
                                    message::RPC::Election(rv) => {
                                        if self.leader_received_election_req_action(rv) {
                                            println!("leader voted for another node. downgrade to follower.");
                                            return ParticipantState::Follower;
                                        }
                                    },
                                    _ => {
                                        println!("leader should not receive append entries request.")
                                    },
                                }
                                false
                            },
                            Err(UnreliableReceiveError::TermOutOfDate(t)) => {
                                self.set_current_term(t);
                                return ParticipantState::Follower;
                            },
                            Err(_) => true,
                        }
                    } else {
                        // panic!("returned selector index should either be a participant or client {:?} {:?} {:?}", op.index(), c_to_p_refs, p_to_p_refs)
                        true
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
                        leader_id: self.leader_id,
                        entries: None,
                        heartbeat: true,
                        current_leader_val: self.current_value,
                    },
                )) {
                    _ => continue,
                }
            }
        }
        ParticipantState::Leader
    }

    /// If a leader receives an election request, performs the necessary actions and then returns
    /// whether or not the action took place.
    fn leader_received_election_req_action(&mut self, rv: message::RequestVote) -> bool {
        let should_vote_yes = false; //rv.term >= self.current_term && 
            //(self.voted_for == -1 || self.voted_for == rv.candidate_id as i64);
        let vote = message::RequestVoteResponse {
            term: self.current_term,
            vote_granted: should_vote_yes,
            sender_id: self.id,
            intended_part_temp: rv.candidate_id,
        };

        self.p_to_p_txs[rv.candidate_id].clone().unwrap().send_timeout(
            RPC::ElectionResp(vote), 
            Duration::from_millis(constants::LEADER_ELECTION_REQ_TIMEOUT_MS)
        ).unwrap();

        if should_vote_yes {
            self.voted_for = rv.candidate_id as i64;
        }

        should_vote_yes
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
            let init_state = self.state;
            // panic!("beginning {:?} action", self.state);
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
                    // continue;
                }
            }

            if init_state != ParticipantState::Candidate && self.state == ParticipantState::Candidate {
                self.set_current_term(self.current_term + 1);
                self.voted_for = self.id as i64;
            }

            // Use std primitives rather than Shuttle primitives to ensure Shuttle scheduling not affected
            if init_state != ParticipantState::Leader && self.state == ParticipantState::Leader {
                let mut leaders_vec = leader_ct_per_term.lock().unwrap();
                if self.current_term < leaders_vec.len() {
                    leaders_vec[self.current_term] += 1;
                } else {
                    for _ in leaders_vec.len()..self.current_term { // pad all previous terms with 0
                        leaders_vec.push(0);
                    }
                    leaders_vec.push(1);
                }
                // println!("node {:?} became leader in term {:?}", self.id, self.current_term);
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
