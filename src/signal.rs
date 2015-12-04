use zmq;
use simple_signal::{Signals, Signal};
use dupl_server_proto::{Trans, Req, Rep};
use dupl_server_proto::bin::{ToBin, FromBin};
use super::ZmqError;

pub fn term_on_signal(external_zmq_addr: &str) {
    let external_zmq_addr_owned = external_zmq_addr.to_owned();
    Signals::set_handler(&[Signal::Hup, Signal::Int, Signal::Quit, Signal::Abrt, Signal::Term], move |_signals| {
        let mut zmq_ctx = zmq::Context::new();
        let mut sock = zmq_ctx.socket(zmq::REQ).map_err(|e| ZmqError::Socket(e)).unwrap();
        sock.connect(&external_zmq_addr_owned).map_err(|e| ZmqError::Connect(external_zmq_addr_owned.clone(), e)).unwrap();
        println!(" ;; SIGINT received, terminating server...");
        loop {
            let packet: Trans<String> = Trans::Sync(Req::Terminate);
            let required = packet.encode_len();
            let mut load_msg = zmq::Message::with_capacity(required).map_err(|e| ZmqError::Message(e)).unwrap();
            packet.encode(&mut load_msg);
            sock.send_msg(load_msg, 0).map_err(|e| ZmqError::Send(e)).unwrap();
            let reply_msg = sock.recv_msg(0).map_err(|e| ZmqError::Recv(e)).unwrap();
            match Rep::<String>::decode(&reply_msg).unwrap() {
                (Rep::TerminateAck, _) => break,
                (Rep::TooBusy, _) => {
                    println!("  ;;; too busy, retrying in 250 ms...");
                    ::std::thread::sleep(::std::time::Duration::from_millis(250));
                },
                (other, _) => panic!("unexpected reply for terminate: {:?}", other),
            }
        }
    });
}
