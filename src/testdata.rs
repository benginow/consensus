use message::Request;
extern crate phf;

pub fn get_test_data(test_name: &str) -> Result<Vec<Vec<Request>>, &'static str> {
    match test_name {
        "0" => Ok(vec![vec![Request::GET, Request::ADD(15)]]),
        "1" => Ok(vec![vec![Request::SET(1), Request::ADD(2), Request::GET]]),
        "2" => Ok(vec![vec![Request::GETLOG, Request::ADD(15), Request::GETLOG]]),
        _ => Err("invalid test name"),
    }
}