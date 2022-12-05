//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate crossbeam_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;
use self::crossbeam_channel::select;
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
            unknown_ops: 0,
        }
    }

    pub fn get_p_to_p_wait_all(p_to_p_rxs: &'a Vec<Option<Receiver<message::RPC>>>) -> Select<'a> {
        let mut selector = Select::new();
        for rx in p_to_p_rxs.iter() {
            match rx {
                Some(rx) => {
                    selector.recv(rx);
                }
                None => continue,
            }
        }
        selector
    }

    pub fn get_c_to_p_wait_all(c_to_p_rxs: &'a Vec<Option<Receiver<message::PtcMessage>>>) -> Select<'a> {
        let mut selector = Select::new();
        for rx in c_to_p_rxs.iter() {
            match rx {
                Some(rx) => {
                    selector.recv(rx);
                }
                None => continue,
            }
        }
        selector
    }

    //send response to client
    pub fn send_client(&mut self, msg: message::PtcMessage, id: usize) -> bool {
        // send message to client saying whether we have aborted or not
        self.p_to_c_txs[id].clone().unwrap().send(msg);
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
                            tx.send(msg.clone());
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
                            tx.send(msg.clone());
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

    pub fn get_value(&mut self) -> u64 {
        let mut ret_val: u64 = 0;
        for (req, _) in self.local_log.iter() {
            match req {
                message::ClientRequest::SET(v) => { ret_val = v.clone(); },
                message::ClientRequest::ADD(v) => { ret_val += v.clone(); },
                _ => (),
            }
        }
        ret_val
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
        let mut result = message::PtcMessage::ParticipantResponse(message::ParticipantResponse::ABORT);

        match request {
            Some(message::PtcMessage::ClientRequest(c)) => {
                match self.state {
                    ParticipantState::Leader => {
                        //communicate with all to send request
                        for i in self.p_to_p_txs.clone() {
                            let append_entry = message::AppendEntries {
                                term: self.current_term,
                                leader_id: self.leader_id,
                                prev_log_index: self.local_log.len() - 1,
                                prev_log_term: self.local_log[self.local_log.len() - 1].0,
                                heartbeat: false,
                                leader_commit: self.local_log.len(),
                                entries: Some(*c),
                            };

                            match i {
                                Some(channel) => {
                                    channel.send(message::RPC::Request(append_entry));
                                }
                                None => continue,
                            }
                        }

                        //now, we need to wait on responses for a majority
                        let majority = self.p_to_p_rxs.len() / 2 + 1;
                        let mut num = 1;
                        while num < majority {
                            //MAKE SURE THIS TIMEOUT IS A GOOD DURATION
                            let msg = Self::get_p_to_p_wait_all(&self.p_to_p_rxs).select_timeout(
                                Duration::from_millis(constants::LEADER_MAJORITY_TIMEOUT_MS),
                            );
                            match msg {
                                Ok(index) => {
                                    let mut indix = index.index();
                                    if indix >= self.id {
                                        indix += 1;
                                    }
                                    let resp = (self.p_to_p_rxs)[indix].clone().unwrap().recv();
                                    match resp {
                                        Ok(rpc) => {
                                            match rpc {
                                                message::RPC::RequestResp(aer) => {
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
                                                _ => {
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
                                    // we couldn't get a majority, so we return false --
                                    // there may be inconsistent state
                                    result = message::PtcMessage::ParticipantResponse(
                                        message::ParticipantResponse::ABORT,
                                    );
                                    //make sure unwrap doesn't panic
                                    self.p_to_c_txs[client_id].clone().unwrap().send(result);
                                    return false;
                                }
                            }
                        }
                        operation_completed = true;
                        let append_to_success = match c {
                            message::ClientRequest::GET => Some(self.get_value()),
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
                        // need to reroute to leader
                        // if leader is -1, then aborted
                        if self.leader_id == -1 {
                            result = message::PtcMessage::ParticipantResponse(
                                message::ParticipantResponse::ABORT
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
                self.p_to_c_txs[client_id].clone().unwrap().send(result);
                return operation_completed;
            }
            _ => false,
        }
    }

    fn candidate_action(&mut self) -> ParticipantState {
        let vote_me = message::RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.local_log.len() - 1,
            last_log_term: self.local_log[self.local_log.len() - 1].0,
        };
        for i in self.p_to_p_txs.clone() {
            match i {
                Some(tx) => {
                    // create RequestVote
                    tx.send(message::RPC::Election(vote_me.clone()));
                }
                None => {
                    continue;
                }
            }
        }
        let majority = self.p_to_p_rxs.len() / 2 + 1;
        let mut num = 1;
        while num < majority {
            let msg = Self::get_p_to_p_wait_all(&self.p_to_p_rxs).select_timeout(
                Duration::from_millis(constants::LEADER_MAJORITY_TIMEOUT_MS),
            );
            match msg {
                Ok(index) => {
                    let mut indix = index.index();
                    if indix >= self.id {
                        indix += 1;
                    }
                    let resp = (self.p_to_p_rxs)[indix].clone().unwrap().recv();
                    match resp {
                        Ok(message::RPC::ElectionResp(er)) => {
                            if er.vote_granted {
                                num += 1;
                            }
                            else {
                                continue;
                            }
                        },
                        Ok(_) => (), // TODO: double check if this should be unit?
                        Err(_) => (),
                        _ => (),
                    }
                }
                Err(_) => {
                    return ParticipantState::Follower;
                }
            }

        }
        // TODO: update current_term?
        return ParticipantState::Leader;
    }

    // this is what a follower is up to
    fn follower_action(&mut self) -> ParticipantState {
        // todo!("todo");


        let mut rng = rand::thread_rng();
        //listens for messages from 1. leader 2. clients rxs (reroute packets to leader :))
        while self.r.load(Ordering::SeqCst) {
            //LISTEN FOR HEARTBEAT
            let mut received_heartbeat = false;
            if self.leader_id == -1 {
                //should be a timer to make self candidate
                let rand_val = rng.gen_range(0..500);
                thread::sleep(Duration::from_millis(rand_val));
            }
            else {
                // OPTIMIZATION: at the beginning of every loop, wait for either leader heartbeat OR client request (so that client requests don't
                // get held up)
                let resp = self.p_to_p_rxs[self.leader_id as usize].clone().unwrap().recv_timeout(Duration::from_millis(constants::FOLLOWER_TIMEOUT_MS));
                match resp {
                    Ok(message::RPC::Request(ae)) => {
                        received_heartbeat = true;
                        // TODO: we must update state 
                        if !ae.heartbeat { // must be a request for a vote on a client request if not a heartbeat
                            let success = ae.leader_id == self.leader_id &&
                                ae.prev_log_index == self.local_log.len() - 1 &&
                                ae.prev_log_term == self.local_log[self.local_log.len() - 1].0 &&
                                ae.term == self.current_term;

                            self.p_to_p_txs[self.leader_id as usize].clone().unwrap().send(
                                message::RPC::RequestResp(message::AppendEntriesResponse{term: self.current_term, success})
                            );
                        }
                    },
                    Ok(message::RPC::Election(e)) => {
                        panic!("leader requested to be elected!!!");
                    },
                     _ => ()
                }
            }

            if !received_heartbeat {
                //listen for x (random) seconds for a election request
                //if no election request, then propose yourself :)
                //break out and set state to candidate


                //nop timeout because that would be unfair and add an extra layer of complexity
                let resp = Self::get_p_to_p_wait_all(&self.p_to_p_rxs).try_select();
                self.leader_id = -1;
                self.current_term = self.current_term + 1;
                match resp {
                    Ok(index) => {
                        let mut indix = index.index();
                        if indix > self.id {
                            indix = indix + 1;
                        }
                        //scary unwrap
                        let msg = self.p_to_p_rxs[indix].clone().unwrap().recv();
                        match msg {
                            Ok(message::RPC::Election(e)) => {
                                
                                let resp = message::RequestVoteResponse{term: self.current_term,
                                                                vote_granted: e.term < self.current_term};
                                let send_resp = message::RPC::ElectionResp(resp);
                                self.p_to_p_txs[indix].clone().unwrap().send(send_resp);
                                //dont want to handle client stuff until leader is established...?
                                continue;
                            }
                            Ok(message::RPC::Request(r)) => {
                                //freak out
                                return ParticipantState::Candidate;
                            }
                            Err(_) => { return ParticipantState::Candidate; }
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
            let mut listen_to_me = Self::get_c_to_p_wait_all(&self.c_to_p_rxs);
            let msg = listen_to_me.select_timeout(Duration::from_millis(constants::FOLLOWER_TIMEOUT_MS));
            match msg {
                Ok(index) => {
                    // let msg = self.c_to_p_rxs[index].clone().recv();
                }
                Err(_) => {
                    print!("tooth");
                }
            }

            //RESPOND TO A CLIENT IF NEEDED

            
        }

        return ParticipantState::Candidate;
        
        // need to listen for
        // let listen = thread::spawn(move || { 
        //     select! (
        //         recv(p_to_c_txs, msg, from) => {
        //             return Leader;
        //         }
        //     )
        // });

        // let wait_on_heartbeat = thread::spawn(move || {

        // });

        //wait on heartbeats from leader
        
        //make this a range

        // if self.leader_id == -1 {
        //     //instead of waiting on a leader, sleep for a random amount of time
        //     // let timer = Timeout::new(rx[0], rand_time);

        //     let rand_time: u64 = rng.gen_range(0..10);
        //     let wait_duration = time::Duration::from_millis(rand_time);
        //     //wait on a candidate response for this duration
        //     select! {
        //         recv(p_to_p_rxs, msg) => {
        //             handle_message_follower();
        //         },
        //         default(wait_duration) => {
        //             //we have timed out, so we send an election response to all?
        //             let elect_me = RequestVote::new(self.term, self.id, self.local_log.len() - 1, self.local_log[local_log.len() - 1].1);
        //             send_all_nodes()
        //         }
        //     }
        // }
        // else {
        //     //wait on heartbeats from leader -> if no heartbeat, return and put yourself in the candidate state
        //     //additionally, listen for something from client.
        //     let rand_time: u64 = rng.gen_range(0..10);
        //     let wait_duration = time::Duration::from_millis(rand_time);
        //     let msg = recv_timeout(p_to_p_rxs[self.leader_id], wait_duration);
        //     match msg {
        //         Ok(m) => {
        //             let handled_message = handle_message(m)
        //             //now, send the message back to the
        //         }

        //         ,
        //         Err =>
        //     }

        // }

        // let heartbeat = thread::spawn(move || {
        //     //listen on all channels for heartbeat? ugh yikes..

        // });
    }

    fn leader_action(&mut self) -> ParticipantState {
        while self.r.load(Ordering::SeqCst) {
            // wait to receive from any of the clients (selector), with a timeout
            let c_to_p_rxs_clone = self.c_to_p_rxs.clone();
            let select_res = Self::
                get_c_to_p_wait_all(&c_to_p_rxs_clone)
                .select_timeout(Duration::from_millis(
                    constants::LEADER_CLIENT_REQ_TIMEOUT_MS,
                ));
            let should_send_heartbeat = match select_res {
                Ok(op) => {
                    // received client request, so action it
                    match self.c_to_p_rxs[op.index()].clone().unwrap().recv() {
                        Ok(req) => {
                            self.perform_operation(&Some(req), op.index());
                            false
                        },
                        Err(e) => {
                            true
                        }
                    }
                }
                Err(e) => {
                    // timed out, so send heartbeat to followers
                    true
                }
            };
            if should_send_heartbeat {
                self.send_all_nodes_unreliable(message::RPC::Request(message::AppendEntries{
                    term: self.current_term, 
                    prev_log_index: self.local_log.len() - 1,
                    leader_id: self.leader_id,
                    prev_log_term: self.local_log[self.local_log.len() - 1].0,
                    entries: None,
                    heartbeat: true,
                    leader_commit: self.local_log.len(),
                }));
            }
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
                Leader => {
                    self.state = self.leader_action();
                }
                Follower => {
                    self.state = self.follower_action();
                }
                Candidate => {
                    self.state = self.candidate_action();
                }
                Down => {
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

    //shit that kind of does not matter that much
    pub fn report_status(&self) {
        println!(
            "participant_{}:\tC:{}\tA:{}\tU:{}",
            self.id, self.successful_ops, self.unsuccessful_ops, self.unknown_ops
        );
    }

    pub fn wait_for_exit_signal(&mut self) {
        trace!("participant_{} waiting for exit signal", self.id);
        // TODO
        trace!("participant_{} exiting", self.id);
    }
}
