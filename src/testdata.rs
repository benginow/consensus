use message::Request;
use std::collections::{HashMap};

macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {{
        core::convert::From::from([$(($k, $v),)*])
    }};
    // set-like
    ($($v:expr),* $(,)?) => {{
        core::convert::From::from([$($v,)*])
    }};
}

static map: HashMap<u64, Vec<Vec<Request>>> = collection! { 0 => collection![collection![Request::GET, Request::ADD(15)]],
                                                            1 => collection![collection![Request::SET(1), Request::ADD(2), Request::GET]],
                                                            2 => collection![collection![Request::GETLOG, Request::ADD(15), Request::GETLOG]]}; 
// HashMap::from();