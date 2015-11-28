extern crate zmq;
extern crate getopts;
extern crate yauid;
extern crate hash_dupl;
extern crate dupl_server_proto;

use std::{io, env, process};
use std::io::Write;
use std::thread::{Builder, sleep, JoinHandle};
use std::num::{ParseFloatError, ParseIntError};
use std::sync::mpsc::{channel, sync_channel, Sender, Receiver, TryRecvError};
use getopts::Options;
use yauid::Yauid;
use hash_dupl::{HashDupl, Config, Shingles};
use hash_dupl::shingler::tokens::Tokens;
use hash_dupl::backend::in_memory::InMemory;
use dupl_server_proto as proto;
use dupl_server_proto::{
    Req, Workload, LookupTask, LookupType, PostAction, InsertCond, ClusterAssign,
    Rep, LookupResult, Match,
};
use dupl_server_proto::bin::{ToBin, FromBin};

const INTERNAL_SOCKET_ADDR: &'static str = "inproc://internal";

#[derive(Debug)]
pub enum ZmqError {
    Socket(zmq::Error),
    Connect(String, zmq::Error),
    Bind(String, zmq::Error),
    Send(zmq::Error),
    Recv(zmq::Error),
    Poll(zmq::Error),
    GetSockOpt(zmq::Error),
    Message(zmq::Error),
}

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    InvalidSignatureLength(ParseIntError),
    InvalidSimilarityThreshold(ParseFloatError),
    InvalidShingleLength(ParseIntError),
    InvalidBandMinProbability(ParseFloatError),
    Zmq(ZmqError),
    Proto(proto::bin::Error),
    Yauid(String),
    MasterIsDown,
    SlaveIsDown,
}

pub struct App {
    zmq_ctx: zmq::Context,
    master_thread: JoinHandle<()>,
    master_watchdog_rx: Receiver<()>,
    slave_thread: JoinHandle<()>,
    slave_watchdog_rx: Receiver<()>,
}

fn bootstrap(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));
    let external_zmq_addr = matches.opt_str("zmq-addr").unwrap_or("ipc://./dupl_server.ipc".to_owned());
    let key_file = matches.opt_str("key-file").unwrap_or("/tmp/hbase.key".to_owned());
    let node_id = matches.opt_str("node-id");
    let signature_length = try!(matches.opt_str("signature-length").map(|s| Ok(Some(try!(s.parse())))).unwrap_or(Ok(None))
                                .map_err(|e| Error::InvalidSignatureLength(e)));
    let shingle_length = try!(matches.opt_str("shingle-length").map(|s| Ok(Some(try!(s.parse())))).unwrap_or(Ok(None))
                              .map_err(|e| Error::InvalidShingleLength(e)));
    let similarity_threshold = try!(matches.opt_str("similarity-threshold").map(|s| Ok(Some(try!(s.parse())))).unwrap_or(Ok(None))
                                    .map_err(|e| Error::InvalidSimilarityThreshold(e)));
    let band_min_probability = try!(matches.opt_str("band-min-probability").map(|s| Ok(Some(try!(s.parse())))).unwrap_or(Ok(None))
                                    .map_err(|e| Error::InvalidBandMinProbability(e)));

    let app = try!(entrypoint(external_zmq_addr,
                              key_file,
                              node_id,
                              signature_length,
                              shingle_length,
                              similarity_threshold,
                              band_min_probability));
    shutdown_app(app)
}

pub fn entrypoint(external_zmq_addr: String,
                  key_file: String,
                  node_id: Option<String>,
                  signature_length: Option<usize>,
                  shingle_length: Option<usize>,
                  similarity_threshold: Option<f64>,
                  band_min_probability: Option<f64>) -> Result<App, Error>
{
    let mut zmq_ctx = zmq::Context::new();
    let mut ext_sock = try!(zmq_ctx.socket(zmq::ROUTER).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut int_sock_master = try!(zmq_ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut int_sock_slave = try!(zmq_ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    try!(ext_sock.bind(&external_zmq_addr).map_err(|e| Error::Zmq(ZmqError::Bind(external_zmq_addr, e))));
    try!(int_sock_master.bind(INTERNAL_SOCKET_ADDR).map_err(|e| Error::Zmq(ZmqError::Bind(INTERNAL_SOCKET_ADDR.to_string(), e))));
    try!(int_sock_slave.connect(INTERNAL_SOCKET_ADDR).map_err(|e| Error::Zmq(ZmqError::Connect(INTERNAL_SOCKET_ADDR.to_string(), e))));

    let (master_tx, slave_rx) = channel();
    let (slave_tx, master_rx) = channel();
    let (master_watchdog_tx, master_watchdog_rx) = sync_channel(0);
    let (slave_watchdog_tx, slave_watchdog_rx) = sync_channel(0);

    let master_thread = Builder::new().name("master thread".to_owned()).spawn(move || {
        master_loop(ext_sock, int_sock_master, master_tx, master_rx).unwrap();
        master_watchdog_tx.send(()).unwrap();
    }).unwrap();

    let slave_thread = Builder::new().name("slave thread".to_owned()).spawn(move || {
        slave_loop(int_sock_slave,
                   slave_tx,
                   slave_rx,
                   key_file,
                   node_id,
                   signature_length,
                   shingle_length,
                   similarity_threshold,
                   band_min_probability).unwrap();
        slave_watchdog_tx.send(()).unwrap();
    }).unwrap();

    Ok(App {
        zmq_ctx: zmq_ctx,
        master_thread: master_thread,
        master_watchdog_rx: master_watchdog_rx,
        slave_thread: slave_thread,
        slave_watchdog_rx: slave_watchdog_rx,
    })
}

pub fn shutdown_app(app: App) -> Result<(), Error> {
    let (mut master_finished, mut slave_finished) = (false, false);
    while !master_finished && !slave_finished {
        if !master_finished {
            match app.master_watchdog_rx.try_recv() {
                Ok(()) => master_finished = true,
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => return Err(Error::MasterIsDown),
            }
        }
        if !slave_finished {
            match app.slave_watchdog_rx.try_recv() {
                Ok(()) => slave_finished = true,
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => return Err(Error::SlaveIsDown),
            }
        }

        sleep(std::time::Duration::from_millis(250));
    }

    app.slave_thread.join().unwrap();
    app.master_thread.join().unwrap();

    Ok(())
}

pub type Headers = Vec<zmq::Message>;
pub struct Message<R> {
    pub headers: Option<Headers>,
    pub load: R,
}

fn rx_sock(sock: &mut zmq::Socket) -> Result<(Option<Headers>, Req<String>), Error> {
    let mut frames = Vec::new();
    loop {
        frames.push(try!(sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))));
        if !try!(sock.get_rcvmore().map_err(|e| Error::Zmq(ZmqError::GetSockOpt(e)))) {
            break
        }
    }

    let load_msg = frames.pop().unwrap();
    Ok((Some(frames), try!(Req::decode(&load_msg).map_err(|e| Error::Proto(e))).0))
}

fn tx_sock(packet: Rep<String>, maybe_headers: Option<Headers>, sock: &mut zmq::Socket) -> Result<(), Error> {
    let required = packet.encode_len();
    let mut load_msg = try!(zmq::Message::with_capacity(required).map_err(|e| Error::Zmq(ZmqError::Message(e))));
    packet.encode(&mut load_msg);

    if let Some(headers) = maybe_headers {
        for header in headers {
            try!(sock.send_msg(header, zmq::SNDMORE).map_err(|e| Error::Zmq(ZmqError::Send(e))));
        }
    }
    sock.send_msg(load_msg, 0).map_err(|e| Error::Zmq(ZmqError::Send(e)))
}

fn master_loop(mut ext_sock: zmq::Socket,
               mut int_sock: zmq::Socket,
               tx: Sender<Message<Req<String>>>,
               rx: Receiver<Message<Rep<String>>>) -> Result<(), Error>
{
    enum SlaveState { Online, Busy, Finished, }
    let mut slave_state = SlaveState::Online;
    loop {
        let (ext_sock_online, int_sock_online) = {
            let mut pollitems = [ext_sock.as_poll_item(zmq::POLLIN), int_sock.as_poll_item(zmq::POLLIN)];
            try!(zmq::poll(&mut pollitems, -1).map_err(|e| Error::Zmq(ZmqError::Poll(e))));
            (pollitems[0].get_revents() == zmq::POLLIN, pollitems[1].get_revents() == zmq::POLLIN)
        };

        if int_sock_online {
            let _ = try!(int_sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e))));
        }

        loop {
            if let SlaveState::Finished = slave_state {
                break
            }

            match rx.try_recv() {
                Ok(message) => {
                    slave_state = SlaveState::Online;
                    match message.load {
                        rep @ Rep::TerminateAck => {
                            slave_state = SlaveState::Finished;
                            try!(tx_sock(rep, message.headers, &mut ext_sock));
                        },
                        rep =>
                            try!(tx_sock(rep, message.headers, &mut ext_sock)),
                    }
                },
                Err(TryRecvError::Empty) =>
                    break,
                Err(TryRecvError::Disconnected) =>
                    panic!("slave worker thread is down"),
            }
        }

        if let SlaveState::Finished = slave_state {
            return Ok(())
        }

        if ext_sock_online {
            match (try!(rx_sock(&mut ext_sock)), &slave_state) {
                ((headers, req), &SlaveState::Online) => {
                    tx.send(Message { headers: headers, load: req, }).unwrap();
                    slave_state = SlaveState::Busy;
                },
                ((headers, _), &SlaveState::Busy) =>
                    try!(tx_sock(Rep::TooBusy, headers, &mut ext_sock)),
                (_, &SlaveState::Finished) =>
                    unreachable!(),
            }
        }
    }
}

fn notify_sock(sock: &mut zmq::Socket) -> Result<(), Error> {
    sock.send_msg(try!(zmq::Message::new().map_err(|e| Error::Zmq(ZmqError::Message(e)))), 0)
        .map_err(|e| Error::Zmq(ZmqError::Send(e)))
}

fn rep_notify(rep: Rep<String>, headers: Option<Headers>, tx: &Sender<Message<Rep<String>>>, sock: &mut zmq::Socket) -> Result<(), Error> {
    tx.send(Message { headers: headers, load: rep, }).unwrap();
    notify_sock(sock)
}

fn slave_loop(mut int_sock: zmq::Socket,
              tx: Sender<Message<Rep<String>>>,
              rx: Receiver<Message<Req<String>>>,
              key_file: String,
              node_id: Option<String>,
              signature_length: Option<usize>,
              shingle_length: Option<usize>,
              similarity_threshold: Option<f64>,
              band_min_probability: Option<f64>) -> Result<(), Error>
{
    let yauid = try!(if let Some(ref node_file) = node_id {
        Yauid::with_node_id(&key_file, node_file)
    } else {
        Yauid::new(&key_file, 1)
    }.map_err(|e| Error::Yauid(e)));

    let mut config = Config::default();
    signature_length.map(|v| config.signature_length = v);
    shingle_length.map(|v| config.shingle_length = v);
    similarity_threshold.map(|v| config.similarity_threshold = v);
    band_min_probability.map(|v| config.band_min_probability = v);

    let mut hd: HashDupl<_, InMemory<String>> = HashDupl::new(Tokens::new(), InMemory::new(), config).unwrap();
    let mut shingles = Shingles::new();

    loop {
        match rx.recv().unwrap() {
            Message { headers: hdrs, load: Req::Init, } =>
                try!(rep_notify(Rep::InitAck, hdrs, &tx, &mut int_sock)),
            Message { headers: hdrs, load: Req::Terminate, } => {
                try!(rep_notify(Rep::TerminateAck, hdrs, &tx, &mut int_sock));
                break;
            },
            Message { headers: hdrs, load: Req::Lookup(Workload::Single(task)), } => {
                let rep = LookupResult::Error("todo single".to_owned());
                try!(rep_notify(Rep::Result(Workload::Single(rep)), hdrs, &tx, &mut int_sock));
            },
            Message { headers: hdrs, load: Req::Lookup(Workload::Many(tasks)), } => {
                let reps: Vec<_> = tasks
                    .into_iter()
                    .map(|task| LookupResult::Error("todo many".to_owned()))
                    .collect();
                try!(rep_notify(Rep::Result(Workload::Many(reps)), hdrs, &tx, &mut int_sock));
            },
        }
    }

    Ok(())
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.optopt("z", "zmq-addr", "server zeromq listen address (optional, default: ipc://./dupl_server.ipc)", "");
    opts.optopt("k", "key-file", "yauid key file to use (optional, default: /tmp/hbase.key)", "");
    opts.optopt("n", "node-id", " yauid node id file to use (optional, default: node id = 1)", "");
    opts.optopt("d", "database", "database path for a backend (optional, default: no database path)", "");
    opts.optopt("", "min-tree-height", "minimum tree height for mset backend (optional, default: 3)", "");
    opts.optopt("", "lookup-index-mode", "one of index mode for mset_lookup backend: mmap or memory (optional, default: mmap)", "");
    opts.optopt("", "lookup-docs-mode", "one of docs file read mode for mset_lookup backend: mmap or memory (optional, default: mmap)", "");
    opts.optopt("", "lookup-slaves", "lookup workers count for mset_lookup, mset_rw and stream backends (optional, default: 2)", "");
    opts.optopt("w", "windows-count", "windows count for stream backend (optional, default: 4)", "");
    opts.optopt("r", "rotate-count", "windows rotate for stream backend for each 'count' documents inserted (optional, default: 128)", "");
    opts.optopt("", "signature-length", "signature length hd param to use (optional)", "");
    opts.optopt("", "shingle-length", "shingle length hd param to use (optional)", "");
    opts.optopt("", "similarity-threshold", "similarity threshold hd param to use (optional)", "");
    opts.optopt("", "band-min-probability", "band minimum probability hd param to use (optional)", "");

    match bootstrap(opts.parse(args)) {
        Ok(()) =>
            (),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod test {
    use zmq;
    use dupl_server_proto::{Req, Rep};
    use dupl_server_proto::bin::{ToBin, FromBin};
    use super::{entrypoint, shutdown_app};

    fn tx_sock(packet: Req<String>, sock: &mut zmq::Socket) {
        let required = packet.encode_len();
        let mut msg = zmq::Message::with_capacity(required).unwrap();
        packet.encode(&mut msg);
        sock.send_msg(msg, 0).unwrap();
    }

    fn rx_sock(sock: &mut zmq::Socket) -> Rep<String> {
        let msg = sock.recv_msg(0).unwrap();
        assert!(!sock.get_rcvmore().unwrap());
        let (rep, _) = Rep::decode(&msg).unwrap();
        rep
    }

    #[test]
    fn start_stop() {
        let mut app = entrypoint("ipc:///tmp/dupl_server_a".to_owned(),
                                 "/tmp/dupl_server_a.key".to_owned(),
                                 None, None, None, None, None).unwrap();
        {
            let mut sock = app.zmq_ctx.socket(zmq::REQ).unwrap();
            sock.connect("ipc:///tmp/dupl_server_a").unwrap();
            tx_sock(Req::Terminate, &mut sock);
            match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        }
        shutdown_app(app).unwrap();
    }
}
