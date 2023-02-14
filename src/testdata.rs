use message::ClientRequest::{ADD, GET, SET};
use message::PtcMessage;

extern crate phf;

//a bunch of GETs
//

fn get_multi_client_test(n_clients: usize) -> Vec<Vec<PtcMessage>> {
    let mut return_vec = Vec::new();
    let message = vec![
        PtcMessage::ClientRequest(ADD(15)),
        PtcMessage::ClientRequest(GET),
        PtcMessage::ClientRequest(ADD(15)),
        PtcMessage::ClientRequest(GET),
        PtcMessage::ClientRequest(SET(8)),
        PtcMessage::ClientRequest(GET),
    ];
    for i in 0..n_clients {
        return_vec.push(message.clone());
    }
    return_vec
}

pub fn get_test_data(test_name: &str) -> Result<Vec<Vec<PtcMessage>>, &'static str> {
    match test_name {
        "0" => { 
            let mut messages = Vec::new();
            for _ in 0..50 {
                messages.push(PtcMessage::ClientRequest(GET));
            }
            Ok(vec![messages])
        }
        "1" => {
            let mut messages = Vec::new();
            for _ in 0..50 {
                messages.push(PtcMessage::ClientRequest(ADD(2)));
            }
            messages.push(PtcMessage::ClientRequest(GET));
            Ok(vec![messages])
        }
        "2" => Ok(get_multi_client_test(1)),
        "3" => Ok(get_multi_client_test(2)),
        "4" => Ok(get_multi_client_test(3)),
        "5" => Ok(get_multi_client_test(4)),
        _ => Err("invalid test name"),
    }
}
