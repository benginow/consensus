#[macro_use]
extern crate log;
extern crate clap;
extern crate crossbeam_channel;
extern crate ctrlc;
extern crate stderrlog;
extern crate shuttle;

use std::convert::TryInto;
use shuttle::thread::{self, JoinHandle};
pub mod checker;
pub mod client;
pub mod constants;
pub mod message;
pub mod oplog;
pub mod participant;
pub mod testdata;
pub mod tpcoptions;
use shuttle::crossbeam_channel::{unbounded, Receiver, Sender};
use client::Client;
use participant::Participant;
use shuttle::sync::{Arc, atomic::{AtomicBool, Ordering}};

fn vec_from_closure<F, T>(f: F, size: usize) -> Vec<T>
where
    F: Fn() -> T,
{
    let mut ret_val = Vec::with_capacity(size);
    for i in 0..size {
        ret_val.push(f());
    }
    ret_val
}

fn vec_from_idx_closure<F, T>(f: F, size: usize) -> Vec<T>
where
    F: Fn(usize) -> T,
{
    let mut ret_val = Vec::with_capacity(size);
    for i in 0..size {
        ret_val.push(f(i));
    }
    ret_val
}

///
/// register_clients()
///
/// The coordinator needs to know about all clients.
/// This function should create clients and use some communication
/// primitive to ensure the coordinator and clients are aware of
/// each other and able to exchange messages. Starting threads to run the
/// client protocol should be deferred until after all the communication
/// structures are created.
///
/// HINT: you probably want to look at rust's mpsc::channel or crossbeam
///       channels to set up communication. Communication in 2PC
///       is duplex!
///
/// HINT: read the logpathbase documentation carefully.
///
/// <params>
///     coordinator: the coordinator!
///     n_clients: number of clients to create and register
///     logpathbase: each participant, client, and the coordinator
///         needs to maintain its own operation and commit log.
///         The project checker assumes a specific directory structure
///         for files backing these logs. Concretely, participant log files
///         will be expected to be produced in:
///            logpathbase/client_<num>.log
///     running: atomic bool indicating whether the simulation is still running
///

struct PartPartCommunicator {
    txs: Vec<Vec<Option<Sender<message::RPC>>>>,
    rxs: Vec<Vec<Option<Receiver<message::RPC>>>>,
}

fn create_part_part_channels(n_participants: usize) -> PartPartCommunicator {
    let mut part_part_txs =
        vec_from_closure(|| vec_from_closure(|| None, n_participants), n_participants);
    let mut part_part_rxs =
        vec_from_closure(|| vec_from_closure(|| None, n_participants), n_participants);

    for src_part in 0..n_participants {
        for dest_part in 0..n_participants {
            if src_part == dest_part {
                part_part_txs[src_part][dest_part] = None;
                part_part_rxs[dest_part][src_part] = None;
            } else {
                let (outgoing_tx, outgoing_rx) = unbounded();
                part_part_txs[src_part][dest_part] = Some(outgoing_tx);
                part_part_rxs[dest_part][src_part] = Some(outgoing_rx);
            }
        }
    }

    PartPartCommunicator {
        txs: part_part_txs,
        rxs: part_part_rxs,
    }
}

struct ClientPartCommunicator {
    p_to_c_txs: Vec<Vec<Option<Sender<message::PtcMessage>>>>, // for the participants to send data to the clients
    p_to_c_rxs: Vec<Vec<Option<Receiver<message::PtcMessage>>>>, // for the clients to receive data from the participants
    c_to_p_txs: Vec<Vec<Option<Sender<message::PtcMessage>>>>, // for the clients to send data to the participants
    c_to_p_rxs: Vec<Vec<Option<Receiver<message::PtcMessage>>>>, // for the participants to receive data from the clients
}

fn create_client_part_channels(n_participants: usize, n_clients: usize) -> ClientPartCommunicator {
    let mut p_to_c_txs = vec_from_closure(|| vec_from_closure(|| None, n_clients), n_participants);
    let mut p_to_c_rxs = vec_from_closure(|| vec_from_closure(|| None, n_participants), n_clients);
    let mut c_to_p_txs = vec_from_closure(|| vec_from_closure(|| None, n_participants), n_clients);
    let mut c_to_p_rxs = vec_from_closure(|| vec_from_closure(|| None, n_clients), n_participants);

    for part in 0..n_participants {
        for client in 0..n_clients {
            let (part_to_client_tx, part_to_client_rx) = unbounded();
            let (client_to_part_tx, client_to_part_rx) = unbounded();

            p_to_c_txs[part][client] = Some(part_to_client_tx);
            p_to_c_rxs[client][part] = Some(part_to_client_rx);
            c_to_p_txs[client][part] = Some(client_to_part_tx);
            c_to_p_rxs[part][client] = Some(client_to_part_rx);
        }
    }

    ClientPartCommunicator {
        p_to_c_txs,
        p_to_c_rxs,
        c_to_p_txs,
        c_to_p_rxs,
    }
}

///
/// register_participants()
///
/// The coordinator needs to know about all participants.
/// This function should create participants and use some communication
/// primitive to ensure the coordinator and participants are aware of
/// each other and able to exchange messages. Starting threads to run the
/// participant protocol should be deferred until after all the communication
/// structures are created.
///
/// HINT: you probably want to look at rust's mpsc::channel or crossbeam
///       channels to set up communication. Note that communication in 2PC
///       is duplex!
///
/// HINT: read the logpathbase documentation carefully.
///
/// <params>
///     coordinator: the coordinator!
///     n_participants: number of participants to create an register
///     logpathbase: each participant, client, and the coordinator
///         needs to maintain its own operation and commit log.
///         The project checker assumes a specific directory structure
///         for files backing these logs. Concretely, participant log files
///         will be expected to be produced in:
///            logpathbase/participant_<num>.log
///     running: atomic bool indicating whether the simulation is still running
///     success_prob_op: [0.0..1.0] probability that operations succeed.
///     success_prob_msg: [0.0..1.0] probability that sends succeed.
///
fn register_participants_and_clients<'a>(
    n_participants: usize,
    n_clients: usize,
    logpathbase: &String,
    running: &'a Arc<AtomicBool>,
    success_prob_op: f64,
    success_prob_msg: f64,
) -> (Vec<Participant>, Vec<Client>) {
    let mut participants = Vec::with_capacity(n_participants);
    let mut clients = Vec::with_capacity(n_clients);

    // register participants with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.

    // initialize participant-participant communication
    let mut p_p_comm = create_part_part_channels(n_participants);
    // initialize client<->participant communication
    let mut p_c_comm = create_client_part_channels(n_participants, n_clients);

    for i in (0..n_participants).rev() {
        // remove backwards
        let log_path = format!("{}/participant_{}.log", logpathbase, i);
        let new_participant = Participant::new(
            i,
            "TODO".into(),
            p_p_comm.txs.remove(i),
            p_p_comm.rxs.remove(i),
            p_c_comm.p_to_c_txs.remove(i),
            p_c_comm.c_to_p_rxs.remove(i),
            log_path,
            running,
            success_prob_op,
            success_prob_msg,
        );
        participants.push(new_participant);
    }

    for i in (0..n_clients).rev() {
        let log_path = format!("{}/client_{}.log", logpathbase, i);
        let new_client = Client::new(
            i,
            "TODO".into(),
            p_c_comm.c_to_p_txs.remove(i),
            p_c_comm.p_to_c_rxs.remove(i),
            log_path,
            running.clone(),
        );
        clients.push(new_client);
    }

    (participants, clients)
}

///
/// launch_clients()
///
/// create a thread per client to run the client
/// part of the 2PC protocol. Somewhere in each of the threads created
/// here, there should be a call to Client::protocol(...). Telling the client
/// how many requests to send is probably a good idea. :-)
///
/// <params>
/// participants: a vector of Participant structs
/// handles: (optional depending on design) -- a mutable vector
///    to return wait handles to the caller
///
fn launch_clients(
    clients: Vec<Client>,
    requests: Vec<Vec<message::PtcMessage>>,
    handles: &mut Vec<JoinHandle<()>>,
) {
    for (i, mut client) in clients.into_iter().enumerate() {
        let data = requests.get(i).unwrap().to_vec();
        handles.push(thread::spawn(move || {
            client.protocol(data);
        }));
    }
}

///
/// launch_participants()
///
/// create a thread per participant to run the participant
/// part of the 2PC protocol. Somewhere in each of the threads created
/// here, there should be a call to Participant::participate(...).
///
/// <params>
/// participants: a vector of Participant structs
/// handles: (optional depending on design) -- a mutable vector
///    to return wait handles to the caller
///
fn launch_participants(participants: Vec<Participant>, handles: &mut Vec<JoinHandle<()>>) {
    // do something to create threads for participant 'processes'
    // the mutable handles parameter allows you to return
    // more than one wait handle to the caller to join on.
    for mut participant in participants {
        handles.push(thread::spawn(move || {
            participant.protocol();
        }));
    }
}

///
/// run()
/// opts: an options structure describing mode and parameters
///
/// 0. install a signal handler that manages a global atomic boolean flag
/// 1. creates a new coordinator
/// 2. creates new clients and registers them with the coordinator
/// 3. creates new participants and registers them with coordinator
/// 4. launches participants in their own threads
/// 5. launches clients in their own threads
/// 6. creates a thread to run the coordinator protocol
///
fn run(opts: &tpcoptions::TPCOptions) {
    // vector for wait handles, allowing us to
    // wait for client, participant, and coordinator
    // threads to join.
    let mut handles: Vec<JoinHandle<()>> = vec![];

    // create an atomic bool object and a signal handler
    // that sets it. this allows us to inform clients and
    // participants that we are exiting the simulation
    // by pressing "control-C", which will set the running
    // flag to false.
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("CTRL-C!");
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting signal handler!");

    // create a coordinator, create and register clients and participants
    // launch threads for all, and wait on handles.
    let cpath = format!("{}{}", opts.logpath, "coordinator.log");

    let (participants, clients) = register_participants_and_clients(
        opts.num_participants,
        opts.num_clients.try_into().unwrap(),
        &opts.logpath,
        &running,
        opts.success_probability_ops,
        opts.success_probability_msg,
    );

    let test_data = testdata::get_test_data(&opts.test_name);
    if let Err(e) = test_data {
        panic!("{}", e);
    }
    launch_participants(participants, &mut handles);
    launch_clients(clients, test_data.unwrap().clone(), &mut handles);

    // wait for clients, participants, and coordinator here...
    for handle in handles {
        handle.join().unwrap();
    }
}

///
/// main()
///
fn main() {
    shuttle::check_random(move || {
        let opts = tpcoptions::TPCOptions::new();
        stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();

        match opts.mode.as_ref() {
            "run" => run(&opts),
            "check" => checker::check_last_run(
                opts.num_clients,
                testdata::get_test_data(&opts.test_name)
                    .unwrap()
                    .len()
                    .try_into()
                    .unwrap(),
                opts.num_participants,
                &opts.logpath.to_string(),
            ),
            _ => panic!("unknown mode"),
        }
    }, 1);
}
