//!
//! TPCOptions
//! A simple tool for managing command line options and
//! trace/log/debug instrumentation for the _T_wo _P_hase _C_ommit
//! project. Exports a constructor for a struct that represents
//! command line options for the project, uses the clap crate
//! to collect command line options, and use the log and stderrlog
//! crates to initialize the application to use trace!(), debug!(),
//! info!() etc macros from the log crate.
//!
//! YOU SHOULD NEED TO MAKE ONLY ONE CHANGE IN THIS FILE,
//! IF ANY: THAT CHANGE Is limited to choosing a different
//! default log path from that specified on line 51 (which
//! currently assumes your log files will go in ~/tmp on
//! your home directory. If that works for you, no change
//! should be necessary at all.)
//!
extern crate clap;
extern crate ctrlc;
extern crate log;
extern crate shellexpand;
extern crate stderrlog;
use clap::{App, Arg};

#[derive(Clone, Debug)]
pub struct TPCOptions {
    pub success_probability_ops: f64, // probability that an operation succeeds
    pub success_probability_msg: f64, // probability that message send succeeds
    pub num_clients: i32,             // number of concurrent clients issuing requests
    pub test_name: String,            // the number of the test to execute
    pub num_participants: usize, // number of participants in 2PC protocol (not including coordinator)
    pub verbosity: usize, // integer verbosity level. experiment with 0 (default) to 5 (fire-hose of output)
    pub mode: String,     // "run" or "check"
    pub logpath: String,  // directory for client, participant, and coordinator logs
}

impl TPCOptions {
    ///
    /// new()
    /// return a new options structure representing
    /// command line options or defaults. initialize
    /// trace/log tools as well.
    ///
    pub fn new() -> TPCOptions {
        let default_n_participants = "3";
        let default_n_clients = "3";
        let default_test_name = "0";
        let default_verbosity = "0";
        let default_mode = "run";
        let default_success_prob_ops = "1.0";
        let default_success_prob_msg = "1.0";

        let default_logpath = shellexpand::tilde("~/tmp/").clone();

        let matches = App::new("concurrency-2pc")
            .version("0.1.0")
            .author("Learner Q. Student <anon@cs.utexas.edu>")
            .about("2pc exercise written in Rust")
            .arg(
                Arg::with_name("success_probability_ops")
                    .short("s")
                    .required(false)
                    .takes_value(true)
                    .help("probability participants successfully execute requests"),
            )
            .arg(
                Arg::with_name("success_probability_msg")
                    .short("S")
                    .required(false)
                    .takes_value(true)
                    .help("probability participants successfully send messages"),
            )
            .arg(
                Arg::with_name("num_clients")
                    .short("c")
                    .required(false)
                    .takes_value(true)
                    .help("number of clients making requests"),
            )
            .arg(
                Arg::with_name("test_name")
                    .short("t")
                    .required(false)
                    .takes_value(true)
                    .help("the test to run"),
            )
            .arg(
                Arg::with_name("num_participants")
                    .short("p")
                    .required(false)
                    .takes_value(true)
                    .help("number of participants in protocol"),
            )
            .arg(
                Arg::with_name("verbose")
                    .short("v")
                    .required(false)
                    .takes_value(true)
                    .help("produce verbose output: 0->none, 5->*most* verbose"),
            )
            .arg(
                Arg::with_name("logpath")
                    .short("l")
                    .required(false)
                    .takes_value(true)
                    .help("specifies path to directory where logs are produced"),
            )
            .arg(
                Arg::with_name("mode")
                    .short("m")
                    .required(false)
                    .takes_value(true)
                    .help("mode--\"run\" runs 2pc, \"check\" checks logs produced by previous run"),
            )
            .get_matches();

        let _mode = matches.value_of("mode").unwrap_or(default_mode);
        let f_success_prob_ops = matches
            .value_of("success_probability_ops")
            .unwrap_or(default_success_prob_ops)
            .parse::<f64>()
            .unwrap();
        let f_success_prob_msg = matches
            .value_of("success_probability_msg")
            .unwrap_or(default_success_prob_msg)
            .parse::<f64>()
            .unwrap();
        let n_participants = matches
            .value_of("num_participants")
            .unwrap_or(default_n_participants)
            .parse::<usize>()
            .unwrap();
        let n_clients = matches
            .value_of("num_clients")
            .unwrap_or(default_n_clients)
            .parse::<i32>()
            .unwrap();
        let test_name = matches
            .value_of("test_name")
            .unwrap_or(default_test_name)
            .parse::<String>()
            .unwrap();
        let _verbosity = matches
            .value_of("verbose")
            .unwrap_or(default_verbosity)
            .parse::<usize>()
            .unwrap();
        let _logpath = matches.value_of("logpath").unwrap_or(&default_logpath);

        match _mode.as_ref() {
            "run" => {}
            "check" => {}
            _ => panic!("unknown execution mode requested!"),
        }

        TPCOptions {
            success_probability_ops: f_success_prob_ops,
            success_probability_msg: f_success_prob_msg,
            num_clients: n_clients,
            test_name: test_name,
            num_participants: n_participants,
            verbosity: _verbosity,
            mode: _mode.to_string(),
            logpath: _logpath.to_string(),
        }
    }
}
