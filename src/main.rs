extern crate zmq;
extern crate serde;
extern crate getopts;
extern crate yauid;
extern crate hash_dupl;
extern crate bin_merge_pile;
extern crate dupl_server_proto;
#[cfg(test)] extern crate rand;

use std::{io, env, process};
use std::io::Write;
use std::sync::Arc;
use std::convert::From;
use std::thread::{Builder, sleep, JoinHandle};
use std::num::{ParseFloatError, ParseIntError};
use std::sync::mpsc::{channel, sync_channel, Sender, Receiver, TryRecvError};
use getopts::Options;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use yauid::Yauid;
use bin_merge_pile::merge::ParallelConfig;
use hash_dupl::{HashDupl, Config, Shingles, Backend};
use hash_dupl::shingler::tokens::Tokens;
use hash_dupl::backend::stream::{Params, Stream};
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
    InvalidRotateCount(ParseIntError),
    InvalidMinTreeHeight(ParseIntError),
    InvalidMaxBlockSize(ParseIntError),
    InvalidMemLimitPower(ParseIntError),
    InvalidMergePileThreads(ParseIntError),
    InvalidWindowsCount(ParseIntError),
    Zmq(ZmqError),
    Proto(proto::bin::Error),
    HashDupl(hash_dupl::Error<(), hash_dupl::backend::stream::Error>),
    Yauid(yauid::Error),
    MasterIsDown,
    SlaveIsDown,
}

impl From<proto::bin::Error> for Error {
    fn from(err: proto::bin::Error) -> Error {
        Error::Proto(err)
    }
}

impl From<hash_dupl::Error<(), hash_dupl::backend::stream::Error>> for Error {
    fn from(err: hash_dupl::Error<(), hash_dupl::backend::stream::Error>) -> Error {
        Error::HashDupl(err)
    }
}

impl From<yauid::Error> for Error {
    fn from(err: yauid::Error) -> Error {
        Error::Yauid(err)
    }
}

pub struct App {
    master_watchdog_rx: Receiver<()>,
    slave_watchdog_rx: Receiver<()>,
    master_thread: Option<JoinHandle<()>>,
    slave_thread: Option<JoinHandle<()>>,
    _zmq_ctx: zmq::Context,
}

impl App {
    fn join(&mut self) {
        while self.slave_thread.is_some() || self.master_thread.is_some() {
            if self.slave_thread.is_some() {
                match self.slave_watchdog_rx.try_recv() {
                    Ok(()) => { self.slave_thread.take().map(|slave_thread| { let _ = slave_thread.join(); }); },
                    Err(TryRecvError::Empty) => (),
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            if self.master_thread.is_some() {
                match self.master_watchdog_rx.try_recv() {
                    Ok(()) => { self.master_thread.take().map(|master_thread| { let _ = master_thread.join(); }); },
                    Err(TryRecvError::Empty) => (),
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            sleep(std::time::Duration::from_millis(250));
        }
    }
}

macro_rules! try_param_parse {
    ($matches:ident, $name:expr, $err:ident) => ({
        try!($matches.opt_str($name).map(|s| Ok(Some(try!(s.parse())))).unwrap_or(Ok(None)).map_err(|e| Error::$err(e)))
    })
}

fn bootstrap(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));
    let external_zmq_addr = matches.opt_str("zmq-addr").unwrap_or("ipc://./dupl_server.ipc".to_owned());
    let database_dir = matches.opt_str("database").unwrap_or("windows".to_owned());
    let key_file = matches.opt_str("key-file").unwrap_or("/tmp/hbase.key".to_owned());
    let node_id = matches.opt_str("node-id");
    let min_tree_height = try_param_parse!(matches, "min-tree-height", InvalidMinTreeHeight);
    let max_block_size = try_param_parse!(matches, "max-block-size", InvalidMaxBlockSize);
    let mem_limit_power = try_param_parse!(matches, "mem-limit-power", InvalidMemLimitPower);
    let merge_pile_threads = try_param_parse!(matches, "merge-pile-threads", InvalidMergePileThreads);
    let windows_count = try_param_parse!(matches, "windows-count", InvalidWindowsCount);
    let rotate_count = try_param_parse!(matches, "rotate-count", InvalidRotateCount);
    let signature_length = try_param_parse!(matches, "signature-length", InvalidSignatureLength);
    let shingle_length = try_param_parse!(matches, "shingle-length", InvalidShingleLength);
    let similarity_threshold = try_param_parse!(matches, "similarity-threshold", InvalidSimilarityThreshold);
    let band_min_probability = try_param_parse!(matches, "band-min-probability", InvalidBandMinProbability);

    try!(entrypoint(external_zmq_addr,
                    database_dir,
                    key_file,
                    node_id,
                    min_tree_height,
                    max_block_size,
                    mem_limit_power,
                    merge_pile_threads,
                    windows_count,
                    rotate_count,
                    signature_length,
                    shingle_length,
                    similarity_threshold,
                    band_min_probability)).join();
    Ok(())
}

pub fn entrypoint(external_zmq_addr: String,
                  database_dir: String,
                  key_file: String,
                  node_id: Option<String>,
                  min_tree_height: Option<usize>,
                  max_block_size: Option<usize>,
                  mem_limit_power: Option<usize>,
                  merge_pile_threads: Option<usize>,
                  windows_count: Option<usize>,
                  rotate_count: Option<usize>,
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
        slave_loop(database_dir,
                   int_sock_slave,
                   slave_tx,
                   slave_rx,
                   key_file,
                   node_id,
                   min_tree_height,
                   max_block_size,
                   mem_limit_power,
                   merge_pile_threads,
                   windows_count,
                   rotate_count,
                   signature_length,
                   shingle_length,
                   similarity_threshold,
                   band_min_probability).unwrap();
        slave_watchdog_tx.send(()).unwrap();
    }).unwrap();

    Ok(App {
        master_watchdog_rx: master_watchdog_rx,
        slave_watchdog_rx: slave_watchdog_rx,
        master_thread: Some(master_thread),
        slave_thread: Some(slave_thread),
        _zmq_ctx: zmq_ctx,
    })
}

pub type Headers = Vec<zmq::Message>;
pub struct Message<R> {
    pub headers: Option<Headers>,
    pub load: R,
}

fn rx_sock(sock: &mut zmq::Socket) -> Result<(Option<Headers>, Req<Arc<String>>), Error> {
    let mut frames = Vec::new();
    loop {
        frames.push(try!(sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))));
        if !try!(sock.get_rcvmore().map_err(|e| Error::Zmq(ZmqError::GetSockOpt(e)))) {
            break
        }
    }

    let load_msg = frames.pop().unwrap();
    Ok((Some(frames), try!(Req::decode(&load_msg)).0))
}

fn tx_sock(packet: Rep<Arc<String>>, maybe_headers: Option<Headers>, sock: &mut zmq::Socket) -> Result<(), Error> {
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
               tx: Sender<Message<Req<Arc<String>>>>,
               rx: Receiver<Message<Rep<Arc<String>>>>) -> Result<(), Error>
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

fn rep_notify(rep: Rep<Arc<String>>, headers: Option<Headers>, tx: &Sender<Message<Rep<Arc<String>>>>, sock: &mut zmq::Socket) ->
    Result<(), Error>
{
    tx.send(Message { headers: headers, load: rep, }).unwrap();
    notify_sock(sock)
}

struct HashDuplEntry {
    cluster_id: u64,
    user_data: Arc<String>,
}

impl Serialize for HashDuplEntry {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        try!(self.cluster_id.serialize(serializer));
        try!(self.user_data.serialize(serializer));
        Ok(())
    }
}

impl Deserialize for HashDuplEntry {
    fn deserialize<D>(deserializer: &mut D) -> Result<HashDuplEntry, D::Error> where D: Deserializer {
        Ok(HashDuplEntry {
            cluster_id: try!(Deserialize::deserialize(deserializer)),
            user_data: try!(Deserialize::deserialize(deserializer)),
        })
    }
}

struct Processor {
    hd: HashDupl<Tokens, Stream<HashDuplEntry>>,
    shingles: Shingles,
    yauid: Yauid,
    inserts_count: usize,
    rotate_count: usize,
}

impl Processor {
    fn new(config: Config,
           params: Params,
           database_dir: String,
           rotate_count: Option<usize>,
           key_file: String,
           node_id: Option<String>) -> Result<Processor, Error> {
        let yauid = try!(if let Some(ref node_file) = node_id {
            Yauid::with_node_id(&key_file, node_file)
        } else {
            Yauid::new(&key_file, 1)
        });

        let backend = try!(Stream::new(database_dir, params).map_err(|e| hash_dupl::Error::Backend(e)));
        let hd = HashDupl::new(Tokens::new(), backend, config).unwrap();
        let shingles = Shingles::new();

        Ok(Processor {
            hd: hd,
            shingles: shingles,
            yauid: yauid,
            inserts_count: 0,
            rotate_count: rotate_count.unwrap_or(131072),
        })
    }

    fn handle(&mut self, LookupTask { text: doc_text, result: lookup_type, post_action: action, }: LookupTask<Arc<String>>) ->
        Result<LookupResult<Arc<String>>, Error>
    {
        try!(self.hd.shinglify(doc_text, &mut self.shingles));
        let signature = try!(self.hd.sign(&self.shingles));
        let (rep, best_similarity) = match lookup_type {
            LookupType::All => {
                let mut best_similarity = None;
                let mut matches: Vec<_> = try!(self.hd.lookup_all(signature.clone()))
                    .into_iter()
                    .map(|neighbour| Match {
                        cluster_id: neighbour.document.cluster_id,
                        similarity: neighbour.similarity,
                        user_data: neighbour.document.user_data.clone(),
                    })
                    .inspect(|&Match { similarity: sim, .. }| if best_similarity.map(|best_sim| best_sim > sim).unwrap_or(true) {
                        best_similarity = Some(sim)
                    })
                    .collect();

                match matches.len() {
                    0 => (LookupResult::EmptySet, None),
                    1 => (LookupResult::Neighbours(Workload::Single(matches.pop().unwrap())), best_similarity),
                    _ => (LookupResult::Neighbours(Workload::Many(matches)), best_similarity),
                }
            },
            LookupType::Best | LookupType::BestOrMine =>
                match try!(self.hd.lookup_best(signature.clone())) {
                    None =>
                        (LookupResult::EmptySet, None),
                    Some(neighbour) =>
                        (LookupResult::Best(Match {
                            cluster_id: neighbour.document.cluster_id,
                            similarity: neighbour.similarity,
                            user_data: neighbour.document.user_data.clone(),
                        }), Some(neighbour.similarity)),
                },
        };

        if let Some(PostAction::InsertNew { assign: action_assign, user_data: action_data, .. }) =
            match (best_similarity, &action) {
                (_, &PostAction::None) =>
                    None,
                (None, &PostAction::InsertNew { .. }) =>
                    Some(action),
                (Some(..), &PostAction::InsertNew { cond: InsertCond::Always, .. }) =>
                    Some(action),
                (Some(best_sim), &PostAction::InsertNew { cond: InsertCond::BestSimLessThan(sim), .. }) if best_sim < sim =>
                    Some(action),
                (Some(..), &PostAction::InsertNew { .. }) =>
                    None,
            } {
                let assigned_cluster_id = match action_assign {
                    ClusterAssign::ServerChoice => try!(self.yauid.get_key()),
                    ClusterAssign::ClientChoice(cluster_id) => cluster_id,
                };
                try!(self.hd.insert(signature, Arc::new(HashDuplEntry { cluster_id: assigned_cluster_id, user_data: action_data.clone(), })));

                self.inserts_count += 1;
                if self.inserts_count >= self.rotate_count {
                    try!(self.hd.backend_mut().rotate().map_err(|e| Error::HashDupl(hash_dupl::Error::Backend(e))));
                    self.inserts_count = 0;
                }

                match lookup_type {
                    LookupType::BestOrMine =>
                        Ok(LookupResult::Best(Match {
                            cluster_id: assigned_cluster_id,
                            similarity: 1.0,
                            user_data: action_data,
                        })),
                    _ => Ok(rep),
                }
            } else {
                Ok(rep)
            }
    }
}

fn slave_loop(database_dir: String,
              mut int_sock: zmq::Socket,
              tx: Sender<Message<Rep<Arc<String>>>>,
              rx: Receiver<Message<Req<Arc<String>>>>,
              key_file: String,
              node_id: Option<String>,
              min_tree_height: Option<usize>,
              max_block_size: Option<usize>,
              mem_limit_power: Option<usize>,
              merge_pile_threads: Option<usize>,
              windows_count: Option<usize>,
              rotate_count: Option<usize>,
              signature_length: Option<usize>,
              shingle_length: Option<usize>,
              similarity_threshold: Option<f64>,
              band_min_probability: Option<f64>) -> Result<(), Error>
{
    let mut config: Config = Default::default();
    signature_length.map(|v| config.signature_length = v);
    shingle_length.map(|v| config.shingle_length = v);
    similarity_threshold.map(|v| config.similarity_threshold = v);
    band_min_probability.map(|v| config.band_min_probability = v);

    let mut params: Params = Default::default();
    min_tree_height.map(|v| params.compile_params.min_tree_height = v);
    max_block_size.map(|v| params.compile_params.max_block_size = v);
    mem_limit_power.map(|v| params.compile_params.memory_limit_power = v);
    merge_pile_threads.map(|v| if v == 1 {
        params.compile_params.parallel_config = ParallelConfig::SingleThread
    } else if v > 1 {
        params.compile_params.parallel_config = ParallelConfig::MultiThread {
            max_slaves: v,
            spawn_trigger_power: params.compile_params.memory_limit_power + 2,
        }
    });
    windows_count.map(|v| params.windows_count = v);

    let mut processor = try!(Processor::new(config, params, database_dir, rotate_count, key_file, node_id));
    loop {
        match rx.recv().unwrap() {
            Message { headers: hdrs, load: Req::Init, } =>
                try!(rep_notify(Rep::InitAck, hdrs, &tx, &mut int_sock)),
            Message { headers: hdrs, load: Req::Terminate, } => {
                try!(rep_notify(Rep::TerminateAck, hdrs, &tx, &mut int_sock));
                break;
            },
            Message { headers: hdrs, load: Req::Lookup(Workload::Single(task)), } => {
                let rep = try!(processor.handle(task));
                try!(rep_notify(Rep::Result(Workload::Single(rep)), hdrs, &tx, &mut int_sock));
            },
            Message { headers: hdrs, load: Req::Lookup(Workload::Many(tasks)), } => {
                let maybe_reps: Result<Vec<_>, _> = tasks
                    .into_iter()
                    .map(|task| processor.handle(task))
                    .collect();
                try!(rep_notify(Rep::Result(Workload::Many(try!(maybe_reps))), hdrs, &tx, &mut int_sock));
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
    opts.optopt("d", "database", "database path for a backend (optional, default: ./windows)", "");
    opts.optopt("", "min-tree-height", "minimum tree height for NTree index (optional, default: 3)", "");
    opts.optopt("", "max-block-size", "maximum block size for NTree index (optional, default: 64)", "");
    opts.optopt("", "mem-limit-power", "power limit for memory part of bin-merge-pile index compiler (optional, default: 16)", "");
    opts.optopt("", "merge-pile-threads", "number of threads for bin-merge-pile index compiler (optional, default: 1)", "");
    opts.optopt("w", "windows-count", "windows count for stream backend (optional, default: 32)", "");
    opts.optopt("r", "rotate-count", "windows rotate for stream backend for each 'count' documents inserted (optional, default: 32768)", "");
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
    use std::fs;
    use zmq;
    use rand::{thread_rng, Rng};
    use dupl_server_proto::{Req, Rep, Workload, LookupTask, LookupType, PostAction, InsertCond, ClusterAssign, LookupResult, Match};
    use dupl_server_proto::bin::{ToBin, FromBin};
    use super::entrypoint;

    fn tx_sock(packet: Req<String>, sock: &mut zmq::Socket) {
        let required = packet.encode_len();
        let mut msg = zmq::Message::with_capacity(required).unwrap();
        packet.encode(&mut msg);
        sock.send_msg(msg, 0).unwrap();
    }

    fn rx_sock(sock: &mut zmq::Socket) -> Rep<String> {
        if zmq::poll(&mut [sock.as_poll_item(zmq::POLLIN)], 2500).unwrap() == 0 {
            panic!("rx_sock timed out")
        }
        let msg = sock.recv_msg(0).unwrap();
        assert!(!sock.get_rcvmore().unwrap());
        let (rep, _) = Rep::decode(&msg).unwrap();
        rep
    }

    #[test]
    fn start_stop() {
        let _ = fs::remove_dir_all("/tmp/windows_dupl_server_a");
        let mut app = entrypoint("ipc:///tmp/dupl_server_a".to_owned(),
                                 "/tmp/windows_dupl_server_a".to_owned(),
                                 "/tmp/dupl_server_a.key".to_owned(),
                                 None, None, None, None, None, None, None, None, None, None, None).unwrap();
        let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
        sock.connect("ipc:///tmp/dupl_server_a").unwrap();
        tx_sock(Req::Init, &mut sock);
        match rx_sock(&mut sock) { Rep::InitAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        tx_sock(Req::Terminate, &mut sock);
        match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        app.join();
    }

    #[test]
    fn insert_lookup() {
        let _ = fs::remove_dir_all("/tmp/windows_dupl_server_b");
        let mut app = entrypoint("ipc:///tmp/dupl_server_b".to_owned(),
                                 "/tmp/windows_dupl_server_b".to_owned(),
                                 "/tmp/dupl_server_b.key".to_owned(),
                                 None, None, None, None, None, None, None, None, None, None, None).unwrap();
        let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
        sock.connect("ipc:///tmp/dupl_server_b").unwrap();
        // initially empty: expect EmptySet
        tx_sock(Req::Lookup(Workload::Single(LookupTask {
            text: "arbitrary text".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None,
        })), &mut sock);
        match rx_sock(&mut sock) { Rep::Result(Workload::Single(LookupResult::EmptySet)) => (), rep => panic!("unexpected rep: {:?}", rep), }
        // try to add something
        tx_sock(Req::Lookup(Workload::Many(vec![LookupTask {
            text: "cat dog mouse bird wolf bear fox hare".to_owned(),
            result: LookupType::BestOrMine,
            post_action: PostAction::InsertNew {
                cond: InsertCond::Always,
                assign: ClusterAssign::ClientChoice(17),
                user_data: "1177".to_owned(),
            },
        }, LookupTask {
            text: "elephant tiger lion jackal crocodile cat dog".to_owned(),
            result: LookupType::Best,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(1.0),
                assign: ClusterAssign::ClientChoice(28),
                user_data: "2288".to_owned(),
            },
        }, LookupTask {
            text: "coyote cat dog mouse bird wolf bear fox".to_owned(),
            result: LookupType::All,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(0.95),
                assign: ClusterAssign::ClientChoice(39),
                user_data: "3399".to_owned(),
            },
        }, LookupTask {
            text: "tiger lion jackal crocodile cat dog".to_owned(),
            result: LookupType::All,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(0.5),
                assign: ClusterAssign::ClientChoice(40),
                user_data: "4400".to_owned(),
            },
        }])), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Many(ref workloads)) => {
                assert_eq!(workloads.len(), 4);
                match workloads.get(0) {
                    Some(&LookupResult::Best(Match { cluster_id: 17, similarity: 1.0, user_data: ref data, })) if data == "1177" => (),
                    other => panic!("unexpected rep workload 0: {:?}", other),
                }
                match workloads.get(1) {
                    Some(&LookupResult::EmptySet) => (),
                    other => panic!("unexpected rep workload 1: {:?}", other),
                }
                match workloads.get(2) {
                    Some(&LookupResult::Neighbours(Workload::Single(Match { cluster_id: 17, user_data: ref data, .. }))) if data == "1177" => (),
                    other => panic!("unexpected rep workload 2: {:?}", other),
                }
                match workloads.get(3) {
                    Some(&LookupResult::Neighbours(Workload::Single(Match { cluster_id: 28, user_data: ref data, .. }))) if data == "2288" => (),
                    other => panic!("unexpected rep workload 3: {:?}", other),
                }
            },
            rep => panic!("unexpected rep: {:?}", rep),
        }
        // try to search common stuff, both should be found
        tx_sock(Req::Lookup(Workload::Single(LookupTask {
            text: "cat dog mouse bird wolf bear fox hare".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None, })), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Neighbours(Workload::Many(mut workloads)))) => {
                assert_eq!(workloads.len(), 2);
                workloads.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap());
                match workloads.get(0) {
                    Some(&Match { cluster_id: 17, similarity: sim, user_data: ref data, .. }) if sim >= 0.4 && data == "1177" => (),
                    other => panic!("unexpected rep neighbour 0: {:?}", other),
                }
                match workloads.get(1) {
                    Some(&Match { cluster_id: 39, similarity: sim, user_data: ref data, .. }) if sim >= 0.4 && data == "3399" => (),
                    other => panic!("unexpected rep neighbour 1: {:?}", other),
                }
            },
            rep => panic!("unexpected rep: {:?}", rep),
        }
        // try to search next common stuff, only one should be found
        tx_sock(Req::Lookup(Workload::Single(LookupTask {
            text: "tiger lion jackal crocodile cat".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None, })), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Neighbours(Workload::Single(Match {
                cluster_id: 28,
                similarity: sim,
                user_data: ref data, ..
            })))) if sim >= 0.4 && data == "2288" => (),
            rep => panic!("unexpected rep: {:?}", rep),
        }
        // terminate
        tx_sock(Req::Terminate, &mut sock);
        match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        app.join();
    }

    fn gen_text() -> String {
        let mut rng = thread_rng();
        let total = 50 + (rng.gen::<usize>() % 50);
        (0 .. total).map(|_| format!("{} ", rng.gen::<u8>())).collect()
    }

    #[test]
    fn stress() {
        let texts: Vec<_> = (0 .. 1000).map(|_| gen_text()).collect();
        {
            let _ = fs::remove_dir_all("/tmp/windows_dupl_server_c");
            let mut app = entrypoint("ipc:///tmp/dupl_server_c".to_owned(),
                                     "/tmp/windows_dupl_server_c".to_owned(),
                                     "/tmp/dupl_server_c.key".to_owned(),
                                     None, None, None, None, None, Some(8), Some(100), None, None, None, None).unwrap();
            let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
            sock.connect("ipc:///tmp/dupl_server_c").unwrap();

            // check & fill
            for (i, text) in texts.iter().enumerate() {
                tx_sock(Req::Lookup(Workload::Single(LookupTask {
                    text: text.clone(),
                    result: LookupType::BestOrMine,
                    post_action: PostAction::InsertNew {
                        cond: InsertCond::Always,
                        assign: ClusterAssign::ClientChoice(i as u64),
                        user_data: text.clone(),
                    },
                })), &mut sock);
                match rx_sock(&mut sock) {
                    Rep::Result(Workload::Single(LookupResult::Best(Match {
                        cluster_id: id,
                        similarity: sim,
                        user_data: ref data, ..
                    }))) if sim >= 0.99 && id == i as u64 && data == text => (),
                    rep => panic!("unexpected rep: {:?}", rep),
                }
            }

            // terminate
            tx_sock(Req::Terminate, &mut sock);
            match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
            app.join();
        }
        {
            let mut app = entrypoint("ipc:///tmp/dupl_server_c".to_owned(),
                                     "/tmp/windows_dupl_server_c".to_owned(),
                                     "/tmp/dupl_server_c.key".to_owned(),
                                     None, None, None, None, None, Some(8), Some(100), None, None, None, None).unwrap();
            let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
            sock.connect("ipc:///tmp/dupl_server_c").unwrap();

            // check
            for (i, text) in texts.iter().enumerate() {
                tx_sock(Req::Lookup(Workload::Single(LookupTask {
                    text: text.clone(),
                    result: LookupType::BestOrMine,
                    post_action: PostAction::None,
                })), &mut sock);
                match rx_sock(&mut sock) {
                    Rep::Result(Workload::Single(LookupResult::Best(Match {
                        cluster_id: id,
                        similarity: sim,
                        user_data: ref data, ..
                    }))) if i >= 200 && sim >= 0.99 && id == i as u64 && data == text => (),
                    Rep::Result(Workload::Single(LookupResult::EmptySet)) if i < 200 => (),
                    rep => panic!("unexpected rep: {:?}", rep),
                }
            }

            // terminate
            tx_sock(Req::Terminate, &mut sock);
            match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
            app.join();
        }
    }
}
