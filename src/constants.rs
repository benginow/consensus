pub const LEADER_CLIENT_REQ_TIMEOUT_MS: u64 = 100;
pub const LEADER_MAJORITY_TIMEOUT_MS: u64 = 500; // time after which leader gives up on getting a majority
pub const FOLLOWER_TIMEOUT_MS: u64 = 1000;
pub const LEADER_ELECTION_REQ_TIMEOUT_MS: u64 = 500; // time after which leader gives up on sending an election request to a given node
pub const LEADER_APPEND_ENTRIES_TIMEOUT_MS: u64 = 500;
pub const RESPOND_TO_CLIENTS_TIMEOUT: u64 = 500;
pub const FOLLOWER_TO_LEADER_COMM: u64 = 250;
// time after which leader gives up on sending append entries request to a given node

pub const CLIENT_OUTGOING_REQUEST_TIMEOUT_MS: u64 = 500; // time after which client gives up on sending request 
pub const CLIENT_INCOMING_RESPONSE_TIMEOUT_MS: u64 = 1000; // time after which client gives up on receiving response