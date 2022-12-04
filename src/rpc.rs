use message;


// message.rs should be an interface that allows for easy communicatiojn
// between client and nodes

// on the other hand, rpc.rs is an interface that allows nodes/participants
// to communicate effectively with each other.

pub enum SendAllEntry {
    Election(RequestVote),
    Request(AppendEntries)
}

pub enum SendAllResponse {
    ElectionResp(RequestVoteResponse),
    RequestResp(AppendEntriesResponse),
}

// REMEMBER: APPEND ENTRIES IS ALSO USED FOR HEARTBEAT!
pub struct AppendEntries {
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: Message::Request,
    entries: Vec<Message::Request>,
    // not fully necessary, but could be convenient just to interface w/
    heartbeat: bool,
    //leader's commit index..
    //says which nth item this should be in the log
    leader_commit: u64,
}

pub struct AppendEntriesResponse {
    term: u64,
    // follower can return non-success in the case that 
    success: bool

}

// i added this, it isn't in the paper
// if append entries response returns success=false, then 
// we want to amend the log.
pub struct AmendLog {
    //index where log should be fixed @
    n: u64,
    entries: Vec<Message::Request>
}

pub struct RequestVote {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64, 
}

pub struct RequestVoteResponse {
    term: u64,
    vote_granted: bool,
}