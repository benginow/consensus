//! 
//! message.rs
//! Message type/primitives
//! 
//! 
//! YOU SHOULD NOT NEED TO CHANGE CODE IN THIS FILE.
//! 
extern crate serde;
extern crate serde_json;
use std::sync::atomic::{AtomicI32, Ordering};
use self::serde_json::Value;

///
/// MessageType
/// Message type codes that various 2PC parties may want to send 
/// or receive. 
/// HINT: You may want to add to this list, but you don't need to.
/// 
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {    
    ClientRequest,          // Request a transaction from the coordinator
    CoordinatorPropose,     // Coordinator sends propose work to clients
    ParticipantVoteCommit,  // Participant votes to commit in phase 1
    ParticipantVoteAbort,   // Participant votes to abort in phase 1
    CoordinatorAbort,       // Coordinator aborts in phase 2
    CoordinatorCommit,      // Coordinator commits phase 2
    ClientResultCommit,     // result (success/fail) communicated to client
    ClientResultAbort,      // result (success/fail) communicated to client
    CoordinatorExit,        // Coordinator telling client/participant about shut down
}

///
/// RequestStatus
/// status of request from client. 
/// HINT: you can probably leave this one alone.
/// 
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStatus {    
    Committed,              // Request succeeded
    Aborted,                // Request explicitly aborted
    Unknown,                // Request status unknown (typically timed out)
}

/// generator for unique ids of messages
static COUNTER: AtomicI32 = AtomicI32::new(1);

///
/// ProtocolMessage
/// message struct to be send as part of 2PC protocol
/// HINT: you probably don't need to change this one. 
///       you can certainly add if it helps, though.
/// 
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ProtocolMessage {
    pub mtype: MessageType,
    pub uid: i32,
    pub txid: i32,
    pub senderid: String, 
    pub opid: i32,
}

///
/// ProtocolMessage implementation
/// 
impl ProtocolMessage {
    pub fn generate(t: MessageType, tid: i32, sid: String, oid: i32) -> ProtocolMessage {
        ProtocolMessage {
            mtype: t,
            uid: COUNTER.fetch_add(1, Ordering::SeqCst),
            txid: tid,
            senderid: sid,
            opid: oid,
        }
    }
    pub fn instantiate(t: MessageType, u: i32, tid: i32, sid: String, oid: i32) -> ProtocolMessage {
        ProtocolMessage {
            mtype: t,
            uid: u,
            txid: tid,
            senderid: sid,
            opid: oid,
        }
    }
    pub fn from_string(line: &String) -> ProtocolMessage {
        let data: Value = serde_json::from_str(&line.to_string()).unwrap();
        let pm: ProtocolMessage = serde_json::from_value(data).unwrap();
        pm
    }

}
