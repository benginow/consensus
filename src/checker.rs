//! 
//! checker
//! Tools for checking output logs produced by the _T_wo _P_hase _C_ommit
//! project in run mode. Exports a single public function called check_last_run
//! that accepts a directory where client, participant, and coordinator log files
//! are found, and the number of clients, participants. Loads and analyses 
//! log files to check a handful of correctness invariants. 
//! 
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
use std::collections::HashMap;
use oplog::OpLog;
use message::ProtocolMessage;
use message::MessageType;
use message;

///
/// check_participant()
/// 
/// Given a participant name and HashMaps that represents the log files
/// for the participant and coordinator (already filtered for commit records),
/// check that the committed and aborted transactions are agreed upon by the two. 
/// 
/// <params>
///     participant: name of participant (label)
///     ncommit: number of committed transactions from coordinator
///     nabort: number of aborted transactions from coordinator
///     ccommitted: map of committed transactions from coordinator
///     plog: map of participant operations
///
fn check_participant(
    participant: &String,
    ncommit: usize,
    nabort: usize,
    ccommitted: &HashMap<i32, ProtocolMessage>,
    plog: &HashMap<i32, ProtocolMessage>
    ) -> bool {
    
    let mut result = true;
    let pcommitted = plog.iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorCommit)
        .map(|(k,v)| (k.clone(), v.clone()));
    let plcommitted = plog.iter()
        .filter(|e| (*e.1).mtype == MessageType::ParticipantVoteCommit)
        .map(|(k,v)| (k.clone(), v.clone()));
    let paborted = plog.iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorAbort)
        .map(|(k,v)| (k.clone(), v.clone()));

    let mcommit: HashMap<i32, message::ProtocolMessage> = pcommitted.collect();
    let mlcommit: HashMap<i32, message::ProtocolMessage> = plcommitted.collect();
    let mabort: HashMap<i32, message::ProtocolMessage> = paborted.collect();
    let npcommit = mcommit.len();
    let nlcommit = mlcommit.len();
    let npabort = mabort.len();
    result &= (npcommit <= ncommit) && (nlcommit >= ncommit);
    result &= npabort <= nabort;
    assert!(ncommit <= nlcommit);
    assert!(npcommit <= ncommit);
    assert!(nabort >= npabort);

    for (_k, v) in ccommitted.iter() {
        let txid = v.txid;
        let mut _foundtxid = 0;
        let mut foundlocaltxid = 0;
        for (_k2, v2) in mcommit.iter() {
            if v2.txid == txid {
                _foundtxid += 1;
            }
        }
        for (_k3, v3) in mlcommit.iter() {
			// handle the case where the participant simply doesn't get 
			// the global commit message from the coordinator. If the 
			// coordinator committed the transaction, the participant 
			// has to have voted in favor. 
            if v3.txid == txid {
                foundlocaltxid += 1;
            }
        }		
        result &= foundlocaltxid == 1;
        assert!(foundlocaltxid == 1); // exactly one commit of txid per participant
    }
    println!("{} OK: C:{} == {}(C-global), A:{} <= {}(A-global)",
             participant.clone(),
             npcommit,
             ncommit,
             npabort,
             nabort);
    result
}

///
/// check_last_run()
/// 
/// accepts a directory where client, participant, and coordinator log files
/// are found, and the number of clients, participants. Loads and analyses 
/// log files to check a handful of correctness invariants. 
///
/// <params>
///     n_clients: number of clients
///     n_requests: number of requests per client
///     n_participants: number of participants
///     logpathbase: directory for client, participant, and coordinator logs 
///
pub fn check_last_run(
    n_clients: i32,
    n_requests: i32, 
    n_participants: usize, 
    logpathbase: &String) {

        info!("Checking 2PC run:  {} requests * {} clients, {} participants", 
              n_requests, 
              n_clients,
              n_participants);

        let mut logs = HashMap::new();
        for pid in 0..n_participants {
             let pid_str = format!("participant_{}", pid);
             let plogpath = format!("{}//{}.log", logpathbase, pid_str);
             let plog = OpLog::from_file(plogpath);
             logs.insert(pid_str, plog);
        }
        let clogpath = format!("{}{}", logpathbase, "coordinator.log");
        let clog = OpLog::from_file(clogpath);        

        let lck = clog.arc();
        let cmap = lck.lock().unwrap();
        let committed: HashMap<i32, message::ProtocolMessage> = 
            cmap.iter().filter(|e| (*e.1).mtype == MessageType::CoordinatorCommit)
                       .map(|(k,v)| (k.clone(), v.clone()))
                       .collect();
        let aborted: HashMap<i32, message::ProtocolMessage> = 
            cmap.iter().filter(|e| (*e.1).mtype == MessageType::CoordinatorAbort)
                       .map(|(k,v)| (k.clone(), v.clone()))
                       .collect();
        let ncommit = committed.len();
        let nabort = aborted.len();

        for(p, v) in logs.iter() {
            let plck = v.arc();
            let plog = plck.lock().unwrap();
            check_participant(p, ncommit, nabort, &committed, &plog);
        }
    }


