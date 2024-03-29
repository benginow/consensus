extern crate serde;
extern crate serde_json;
use crate::client::Client;
use std::sync::atomic::{AtomicI32, Ordering};

use self::serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
// ptc = participant to coordiator
pub enum PtcMessage {
    ClientRequest(ClientRequest),
    ParticipantResponse(ParticipantResponse),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientRequest {
    SET(u64),
    ADD(u64),
    GET,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ParticipantResponse {
    // (Option<Vec<ClientRequest>>) -> i had this, not sure it's necessary
    SUCCESS(Option<u64>),
    LEADER(i64),
    ABORT,
    ELECTION,
    // will never be sent back, but whatever
    UNKNOWN,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RPC {
    Election(RequestVote),
    Request(AppendEntries),
    ElectionResp(RequestVoteResponse),
    RequestResp(AppendEntriesResponse),
}

// REMEMBER: APPEND ENTRIES IS ALSO USED FOR HEARTBEAT!
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub struct AppendEntries {
    pub term: usize,
    pub leader_id: i64,
    pub entries: Option<ClientRequest>,
    // not fully necessary, but could be convenient just to interface w/
    pub heartbeat: bool,
    //leader's commit index..
    //says which nth item this should be in the log
    pub current_leader_val: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: usize,
    // follower can return non-success in the case that
    pub success: bool,
}

// i added this, it isn't in the paper
// if append entries response returns success=false, then
// we want to amend the log.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AmendLog {
    //index where log should be fixed @
    n: u64,
    entries: Vec<ClientRequest>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVote {
    pub term: usize,
    pub candidate_id: usize,
    pub last_log_index: i64,
    pub last_log_term: Option<ClientRequest>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: usize,
    pub vote_granted: bool,
    pub sender_id: usize,
    pub intended_part_temp: usize,
}
