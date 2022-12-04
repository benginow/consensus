#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
use std::sync::mpsc::channel;
use std::thread;
use std::thread::JoinHandle;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use coordinator::Coordinator;
use participant::Participant;
use client::Client;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};

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
fn register_clients(
    coordinator: &mut Coordinator,
    n_clients: i32,
    logpathbase: String,
    running: &Arc<AtomicBool>) -> Vec<Client> {
    let mut clients = vec![];
    // register clients with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.
    for i in 0..n_clients {
        let (client_to_coord_tx, client_to_coord_rx) = channel();
        let (coord_to_client_tx, coord_to_client_rx) = channel();

        let log_path = format!("{}/client_{}.log", logpathbase, i);
        let new_client = Client::new(i, "TODO".into(), client_to_coord_tx, coord_to_client_rx, log_path, running.clone());
        clients.push(new_client);
        coordinator.client_channels.push((coord_to_client_tx, client_to_coord_rx));
    }
    clients
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
fn register_participants(
    coordinator: &mut Coordinator,
    n_participants: i32,
    logpathbase: &String,
    running: &Arc<AtomicBool>, 
    success_prob_op: f64,
    success_prob_msg: f64) -> Vec<Participant> {

    let mut participants = vec![];
    // register participants with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.
    for i in 0..n_participants {
        let (part_to_coord_tx, part_to_coord_rx) = channel();
        let (coord_to_part_tx, coord_to_part_rx) = channel();

        let log_path = format!("{}/participant_{}.log", logpathbase, i);
        let new_participants = Participant::new(i, "TODO".into(), part_to_coord_tx, coord_to_part_rx, log_path, running, success_prob_op, success_prob_msg);
        participants.push(new_participants);
        coordinator.participant_channels.push((coord_to_part_tx, part_to_coord_rx));
    }
    participants
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
    n_requests: i32,
    handles: &mut Vec<JoinHandle<()>>) {
    for mut client in clients {
        handles.push(thread::spawn(move || {
            client.protocol(n_requests);
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
fn launch_participants(
    participants: Vec<Participant>,
    handles: &mut Vec<JoinHandle<()>>) {

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
fn run(opts: & tpcoptions::TPCOptions) {

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
    }).expect("Error setting signal handler!");

    // create a coordinator, create and register clients and participants
    // launch threads for all, and wait on handles. 
    let cpath = format!("{}{}", opts.logpath, "coordinator.log");
    let mut coordinator = Coordinator::new(cpath, &running, opts.success_probability_msg);

    let clients = register_clients(&mut coordinator, opts.num_clients, opts.logpath.clone(), &running);
    let participants = register_participants(&mut coordinator, opts.num_participants, &opts.logpath, &running, opts.success_probability_ops, opts.success_probability_msg);

    launch_clients(clients, opts.num_requests, &mut handles);
    launch_participants(participants, &mut handles);

    handles.push(thread::spawn(move || {
        coordinator.protocol();
    }));

    // wait for clients, participants, and coordinator here...
    for handle in handles {
        handle.join().unwrap();
    }
}

///
/// main()
/// 
fn main() {
    
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
        "check" => checker::check_last_run(opts.num_clients, 
                                        opts.num_requests, 
                                        opts.num_participants, 
                                        &opts.logpath.to_string()),
        _ => panic!("unknown mode"),
    }
}
