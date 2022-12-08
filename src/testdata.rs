use message::ClientRequest::{ADD, GET, SET};
use message::PtcMessage;

extern crate phf;

pub fn get_test_data(test_name: &str) -> Result<Vec<Vec<PtcMessage>>, &'static str> {
    match test_name {
        "0" => Ok(vec![vec![
            PtcMessage::ClientRequest(ADD(15)),
            PtcMessage::ClientRequest(GET),
            PtcMessage::ClientRequest(ADD(15)),
            PtcMessage::ClientRequest(GET),
            PtcMessage::ClientRequest(SET(8)),
            PtcMessage::ClientRequest(GET),
        ],
        ]),
        "1" => Ok(vec![vec![
            PtcMessage::ClientRequest(SET(1)),
            PtcMessage::ClientRequest(ADD(2)),
            PtcMessage::ClientRequest(GET),
        ]]),
        "2" => Ok(vec![vec![
            PtcMessage::ClientRequest(GET),
            PtcMessage::ClientRequest(ADD(15)),
            PtcMessage::ClientRequest(GET),
        ]]),
        _ => Err("invalid test name"),
    }
}
