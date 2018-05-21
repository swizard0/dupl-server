extern crate zmq;
extern crate serde;
extern crate yauid;
extern crate hash_dupl;
extern crate simple_signal;
extern crate unix_daemonize;
extern crate bin_merge_pile;
extern crate dupl_server_proto;
#[macro_use] extern crate clap;
#[cfg(test)] extern crate rand;

use std::{io, fs, process};
use std::io::{Write, BufRead};
use std::sync::Arc;
use std::convert::From;
use std::thread::{Builder, sleep, JoinHandle};
use std::sync::mpsc::{channel, sync_channel, Sender, Receiver, TryRecvError};
use clap::Arg;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use yauid::Yauid;
use bin_merge_pile::ntree_bkd::mmap;
use bin_merge_pile::merge::ParallelConfig;
use hash_dupl::{HashDupl, Config, Shingles, Backend};
use hash_dupl::shingler::tokens::Tokens;
use hash_dupl::backend::stream::{Params, Stream};
use hash_dupl::backend::pile_lookup::DataAccess;
use unix_daemonize::{daemonize_redirect, ChdirMode};
use dupl_server_proto as proto;
use dupl_server_proto::{
    Trans, Req, Workload, LookupTask, LookupType, PostAction, InsertCond, ClusterAssign, AssignCond, ClusterChoice,
    Rep, LookupResult, Match,
};
use dupl_server_proto::bin::{ToBin, FromBin};

mod signal;

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
    Clap(clap::Error),
    NoNodeIdFileProvided,
    OpenStopDict(String, io::Error),
    ReadStopDict(String, io::Error),
    Daemonize(unix_daemonize::Error),
    CreatePidFile(String, io::Error),
    WritePidFile(String, io::Error),
    Zmq(ZmqError),
    Proto(proto::bin::Error),
    HashDupl(hash_dupl::Error<(), hash_dupl::backend::stream::Error>),
    Yauid(yauid::Error),
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

pub fn default_stop_dict() -> Vec<String> {
    let words = ["or", "and", ".", "...", ",", "?", "!", "…", "\"", "http", "://", "-", "t", "co", ":", "/"];
    words.iter().map(|&w| w.to_owned()).collect()
}

fn make_stop_dict(maybe_filename: Option<&str>, additional_words: Option<clap::Values>) -> Result<Vec<String>, Error> {
    let mut dict: Vec<_> = if let Some(words) = additional_words {
        words.map(|s| s.to_string()).collect()
    } else {
        default_stop_dict()
    };
    if let Some(filename) = maybe_filename {
        let fd = fs::File::open(&filename).map_err(|e| Error::OpenStopDict(filename.to_string(), e))?;
        let reader = io::BufReader::new(fd);
        for maybe_line in reader.lines() {
            let line = maybe_line.map_err(|e| Error::ReadStopDict(filename.to_string(), e))?;
            let trimmed_line = line.trim_matches(|c: char| c.is_whitespace() || c == '.');
            if trimmed_line.is_empty() || trimmed_line.starts_with("//") {
                continue;
            }
            dict.push(trimmed_line.to_owned());
        }
    }
    Ok(dict)
}

fn param_usize(matches: &clap::ArgMatches, param: &str) -> Result<Option<usize>, Error> {
    if matches.value_of(param).is_some() {
        Ok(Some(value_t!(matches, param, usize).map_err(Error::Clap)?))
    } else {
        Ok(None)
    }
}

fn param_f64(matches: &clap::ArgMatches, param: &str) -> Result<Option<f64>, Error> {
    if matches.value_of(param).is_some() {
        Ok(Some(value_t!(matches, param, f64).map_err(Error::Clap)?))
    } else {
        Ok(None)
    }
}

fn bootstrap(matches: &clap::ArgMatches) -> Result<(), Error> {
    let external_zmq_addr = matches.value_of("zmq-addr").unwrap();
    let database_dir = matches.value_of("database").unwrap();
    let key_file = matches.value_of("key-file").unwrap();
    let node_id = matches.value_of("node-id");
    let min_tree_height = param_usize(matches, "min-tree-height")?;
    let max_block_size = param_usize(matches, "max-block-size")?;
    let mem_limit_power = param_usize(matches, "mem-limit-power")?;
    let merge_pile_threads = param_usize(matches, "merge-pile-threads")?;
    let windows_count = param_usize(matches, "windows-count")?;
    let rotate_count = param_usize(matches, "rotate-count")?;
    let signature_length = param_usize(matches, "signature-length")?;
    let shingle_length = param_usize(matches, "shingle-length")?;
    let similarity_threshold = param_f64(matches, "similarity-threshold")?;
    let band_min_probability = param_f64(matches, "band-min-probability")?;
    let stop_words = make_stop_dict(matches.value_of("stop-dict"), matches.values_of("sd"))?;
    let daemonize = matches.is_present("daemonize");
    let redirect_stdout = matches.value_of("redirect-stdout");
    let redirect_stderr = matches.value_of("redirect-stderr");
    let pid_file = matches.value_of("pid-file");
    let mmap_mode = match matches.value_of("mmap-type") {
        Some("mmap") =>
            mmap::MmapType::TrueMmap { madv_willneed: matches.is_present("madvise-willneed"), },
        Some("malloc") =>
            mmap::MmapType::Malloc,
        _ =>
            unreachable!(),
    };
    let data_access = match matches.value_of("data-access") {
        Some("file-seek") =>
            DataAccess::FileSeek,
        Some("memory-cache") =>
            DataAccess::MemoryCache,
        _ =>
            unreachable!(),
    };

    if daemonize {
        let pid = daemonize_redirect(redirect_stdout, redirect_stderr, ChdirMode::NoChdir).map_err(|e| Error::Daemonize(e))?;
        println!("Daemonized with pid = {:?}", pid);
        if let Some(file) = pid_file {
            let mut fd = fs::File::create(&file).map_err(|e| Error::CreatePidFile(file.to_string(), e))?;
            write!(&mut fd, "{}", pid).map_err(|e| Error::WritePidFile(file.to_string(), e))?;
        }
    }

    signal::term_on_signal(&external_zmq_addr);
    println!("Server started");
    entrypoint(external_zmq_addr,
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
               band_min_probability,
               stop_words,
               mmap_mode,
               data_access,
    )?.join();
    println!("Server terminated");
    Ok(())
}

pub fn entrypoint(
    external_zmq_addr: &str,
    database_dir_str: &str,
    key_file_str: &str,
    node_id_str: Option<&str>,
    min_tree_height: Option<usize>,
    max_block_size: Option<usize>,
    mem_limit_power: Option<usize>,
    merge_pile_threads: Option<usize>,
    windows_count: Option<usize>,
    rotate_count: Option<usize>,
    signature_length: Option<usize>,
    shingle_length: Option<usize>,
    similarity_threshold: Option<f64>,
    band_min_probability: Option<f64>,
    stop_words: Vec<String>,
    mmap_mode: mmap::MmapType,
    data_access: DataAccess,
)
    -> Result<App, Error>
{
    let zmq_ctx = zmq::Context::new();
    let ext_sock = zmq_ctx.socket(zmq::ROUTER).map_err(|e| Error::Zmq(ZmqError::Socket(e)))?;
    let int_sock_master = zmq_ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e)))?;
    let int_sock_slave = zmq_ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e)))?;
    ext_sock.bind(external_zmq_addr).map_err(|e| Error::Zmq(ZmqError::Bind(external_zmq_addr.to_string(), e)))?;
    int_sock_master.bind(INTERNAL_SOCKET_ADDR).map_err(|e| Error::Zmq(ZmqError::Bind(INTERNAL_SOCKET_ADDR.to_string(), e)))?;
    int_sock_slave.connect(INTERNAL_SOCKET_ADDR).map_err(|e| Error::Zmq(ZmqError::Connect(INTERNAL_SOCKET_ADDR.to_string(), e)))?;

    let (master_tx, slave_rx) = channel();
    let (slave_tx, master_rx) = channel();
    let (master_watchdog_tx, master_watchdog_rx) = sync_channel(0);
    let (slave_watchdog_tx, slave_watchdog_rx) = sync_channel(0);

    let master_thread = Builder::new().name("master thread".to_owned()).spawn(move || {
        master_loop(ext_sock, int_sock_master, master_tx, master_rx).unwrap();
        master_watchdog_tx.send(()).unwrap();
    }).unwrap();

    let database_dir = database_dir_str.to_string();
    let key_file = key_file_str.to_string();
    let node_id = node_id_str.map(|s| s.to_string());
    let slave_thread = Builder::new().name("slave thread".to_owned()).spawn(move || {
        slave_loop(
            database_dir,
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
            band_min_probability,
            stop_words,
            mmap_mode,
            data_access,
        ).unwrap();
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

fn rx_sock(sock: &zmq::Socket) -> Result<(Option<Headers>, Trans<Arc<String>>), Error> {
    let mut frames = Vec::new();
    loop {
        frames.push(sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))?);
        if !sock.get_rcvmore().map_err(|e| Error::Zmq(ZmqError::GetSockOpt(e)))? {
            break
        }
    }

    let load_msg = frames.pop().unwrap();
    Ok((Some(frames), Trans::decode(&load_msg)?.0))
}

fn tx_sock(packet: Rep<Arc<String>>, maybe_headers: Option<Headers>, sock: &zmq::Socket) -> Result<(), Error> {
    let required = packet.encode_len();
    let mut load_msg = zmq::Message::with_capacity(required);
    packet.encode(&mut load_msg);

    if let Some(headers) = maybe_headers {
        for header in headers {
            sock.send(header, zmq::SNDMORE).map_err(|e| Error::Zmq(ZmqError::Send(e)))?;
        }
    }
    sock.send(load_msg, 0).map_err(|e| Error::Zmq(ZmqError::Send(e)))
}

fn master_loop(ext_sock: zmq::Socket,
               int_sock: zmq::Socket,
               tx: Sender<Message<Req<Arc<String>>>>,
               rx: Receiver<Message<Rep<Arc<String>>>>) -> Result<(), Error>
{
    enum SlaveState { Online, Busy, Finished, }
    let mut slave_state = SlaveState::Online;
    let mut req_queue = Vec::new();
    loop {
        let (ext_sock_online, int_sock_online) = {
            let mut pollitems = [ext_sock.as_poll_item(zmq::POLLIN), int_sock.as_poll_item(zmq::POLLIN)];
            zmq::poll(&mut pollitems, -1).map_err(|e| Error::Zmq(ZmqError::Poll(e)))?;
            (pollitems[0].get_revents() == zmq::POLLIN, pollitems[1].get_revents() == zmq::POLLIN)
        };

        if int_sock_online {
            let _ = int_sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))?;
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
                            tx_sock(rep, message.headers, &ext_sock)?;
                        },
                        rep =>
                            tx_sock(rep, message.headers, &ext_sock)?,
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
            req_queue.push(rx_sock(&ext_sock)?)
        }

        let mut req_queue_skipped = Vec::new();
        for req_data in req_queue {
            match (req_data, &slave_state) {
                ((headers, Trans::Async(req)), &SlaveState::Online) => {
                    tx.send(Message { headers: headers, load: req, }).unwrap();
                    slave_state = SlaveState::Busy;
                },
                ((headers, Trans::Sync(req)), &SlaveState::Online) => {
                    tx.send(Message { headers: headers, load: req, }).unwrap();
                    slave_state = SlaveState::Busy;
                },
                ((headers, Trans::Async(..)), &SlaveState::Busy) =>
                    tx_sock(Rep::TooBusy, headers, &ext_sock)?,
                ((headers, trans @ Trans::Sync(..)), &SlaveState::Busy) =>
                    req_queue_skipped.push((headers, trans)),
                (_, &SlaveState::Finished) =>
                    unreachable!(),
            }
        }
        req_queue = req_queue_skipped;
    }
}

fn notify_sock(sock: &mut zmq::Socket) -> Result<(), Error> {
    sock.send(zmq::Message::new(), 0)
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        (self.cluster_id, &*self.user_data).serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for HashDuplEntry {
    fn deserialize<D>(deserializer: D) -> Result<HashDuplEntry, D::Error> where D: Deserializer<'a> {
        let (cluster_id, user_data) = Deserialize::deserialize(deserializer)?;
        Ok(HashDuplEntry { cluster_id, user_data: Arc::new(user_data), })
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
           stop_words: Vec<String>,
           rotate_count: Option<usize>,
           key_file: String,
           node_id: Option<String>) -> Result<Processor, Error> {

        let node_file: &str = if fs::metadata("/etc/node.id").as_ref().map(fs::Metadata::is_file).unwrap_or(false) {
            "/etc/node.id"
        } else if let Some(ref node_file) = node_id {
            &*node_file
        } else {
            return Err(Error::NoNodeIdFileProvided)
        };

        let yauid = Yauid::with_node_id(&key_file, node_file)?;
        let backend = Stream::new(database_dir, params).map_err(|e| hash_dupl::Error::Backend(e))?;
        let mut tokenizer = Tokens::new();
        tokenizer.set_stop_words::<_, Error>(stop_words.into_iter().map(|v| Ok(v)))?;
        let hd = HashDupl::new(tokenizer, backend, config).unwrap();
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
        self.hd.shinglify(doc_text, &mut self.shingles)?;
        let signature = self.hd.sign(&self.shingles)?;
        let (rep, best_similarity) = match lookup_type {
            LookupType::All => {
                let mut best_similarity = None;
                let mut matches: Vec<_> = self.hd.lookup_all(signature.clone())?
                    .into_iter()
                    .map(|neighbour| Match {
                        cluster_id: neighbour.document.cluster_id,
                        similarity: neighbour.similarity,
                        user_data: neighbour.document.user_data.clone(),
                    })
                    .inspect(|&Match { similarity: sim, cluster_id: c, .. }| if best_similarity.map(|(best_sim, _)| best_sim > sim).unwrap_or(true) {
                        best_similarity = Some((sim, c))
                    })
                    .collect();

                match matches.len() {
                    0 => (LookupResult::EmptySet, None),
                    1 => (LookupResult::Neighbours(Workload::Single(matches.pop().unwrap())), best_similarity),
                    _ => (LookupResult::Neighbours(Workload::Many(matches)), best_similarity),
                }
            },
            LookupType::Best | LookupType::BestOrMine =>
                match self.hd.lookup_best(signature.clone())? {
                    None =>
                        (LookupResult::EmptySet, None),
                    Some(neighbour) =>
                        (LookupResult::Best(Match {
                            cluster_id: neighbour.document.cluster_id,
                            similarity: neighbour.similarity,
                            user_data: neighbour.document.user_data.clone(),
                        }), Some((neighbour.similarity, neighbour.document.cluster_id))),
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
                (Some((best_sim, _)), &PostAction::InsertNew { cond: InsertCond::BestSimLessThan(sim), .. }) if best_sim < sim =>
                    Some(action),
                (Some(..), &PostAction::InsertNew { .. }) =>
                    None,
            } {
                let assign_choice = match (best_similarity, action_assign) {
                    (_, ClusterAssign { cond: AssignCond::Always, choice: c, }) =>
                        Ok(c),
                    (None, ClusterAssign { cond: AssignCond::BestSimLessThan(..), choice: c, }) =>
                        Ok(c),
                    (Some((best_sim, cluster_id)), ClusterAssign { cond: AssignCond::BestSimLessThan(sim), choice: c, }) =>
                        if best_sim < sim {
                            Ok(c)
                        } else {
                            Err(cluster_id)
                        },
                };

                let assigned_cluster_id = match assign_choice {
                    Ok(ClusterChoice::ServerChoice) =>
                        self.yauid.get_key()?,
                    Ok(ClusterChoice::ClientChoice(cluster_id)) =>
                        cluster_id,
                    Err(cluster_id) =>
                        cluster_id,
                };

                self.hd.insert(signature, Arc::new(HashDuplEntry { cluster_id: assigned_cluster_id, user_data: action_data.clone(), }))?;

                self.inserts_count += 1;
                if self.inserts_count >= self.rotate_count {
                    self.hd.backend_mut().rotate().map_err(|e| Error::HashDupl(hash_dupl::Error::Backend(e)))?;
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

fn slave_loop(
    database_dir: String,
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
    band_min_probability: Option<f64>,
    stop_words: Vec<String>,
    mmap_mode: mmap::MmapType,
    data_access: DataAccess,
)
    -> Result<(), Error>
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
    params.lookup_params.mmap_type = mmap_mode;
    params.lookup_params.data_access = data_access;
    merge_pile_threads.map(|v| if v == 1 {
        params.compile_params.parallel_config = ParallelConfig::SingleThread
    } else if v > 1 {
        params.compile_params.parallel_config = ParallelConfig::MultiThread {
            max_slaves: v,
            spawn_trigger_power: params.compile_params.memory_limit_power + 2,
        }
    });
    windows_count.map(|v| params.windows_count = v);

    let mut processor = Processor::new(config, params, database_dir, stop_words, rotate_count, key_file, node_id)?;
    loop {
        match rx.recv().unwrap() {
            Message { headers: hdrs, load: Req::Init, } =>
                rep_notify(Rep::InitAck, hdrs, &tx, &mut int_sock)?,
            Message { headers: hdrs, load: Req::Terminate, } => {
                rep_notify(Rep::TerminateAck, hdrs, &tx, &mut int_sock)?;
                break;
            },
            Message { headers: hdrs, load: Req::Lookup(Workload::Single(task)), } => {
                let rep = processor.handle(task).unwrap_or_else(|e| LookupResult::Error(format!("{:?}", e)));
                rep_notify(Rep::Result(Workload::Single(rep)), hdrs, &tx, &mut int_sock)?;
            },
            Message { headers: hdrs, load: Req::Lookup(Workload::Many(tasks)), } => {
                let reps: Vec<_> = tasks
                    .into_iter()
                    .map(|task| processor.handle(task).unwrap_or_else(|e| LookupResult::Error(format!("{:?}", e))))
                    .collect();
                rep_notify(Rep::Result(Workload::Many(reps)), hdrs, &tx, &mut int_sock)?;
            },
        }
    }

    Ok(())
}

fn main() {
    let matches = clap::App::new("dupl-server")
        .version(crate_version!())
        .author("Alexey Voznyuk <me@swizard.info>")
        .about("High performance documents stream clustering server.")
        .arg(Arg::with_name("zmq-addr")
             .display_order(1)
             .short("z")
             .long("zmq-addr")
             .value_name("ADDR")
             .help("Server zeromq listen address.")
             .takes_value(true)
             .default_value("ipc://./dupl_server.ipc"))
        .arg(Arg::with_name("database")
             .display_order(2)
             .short("d")
             .long("database")
             .value_name("PATH")
             .help("Database path for a backend.")
             .takes_value(true)
             .default_value("./windows"))
        .arg(Arg::with_name("windows-count")
             .display_order(3)
             .short("w")
             .long("windows-count")
             .value_name("COUNT")
             .help("Windows count for stream backend.")
             .takes_value(true)
             .default_value("32"))
        .arg(Arg::with_name("rotate-count")
             .display_order(4)
             .short("r")
             .long("rotate-count")
             .value_name("COUNT")
             .help("Windows rotate for stream backend for each 'count' documents inserted.")
             .takes_value(true)
             .default_value("32768"))
        .arg(Arg::with_name("key-file")
             .display_order(5)
             .short("k")
             .long("key-file")
             .value_name("FILE")
             .help("Yauid key file to use.")
             .takes_value(true)
             .default_value("/tmp/hbase.key"))
        .arg(Arg::with_name("node-id")
             .display_order(6)
             .short("n")
             .long("node-id")
             .value_name("FILE")
             .help("Yauid node id file to use if no \"/etc/node.id\" found.")
             .takes_value(true))
        .arg(Arg::with_name("min-tree-height")
             .display_order(7)
             .long("min-tree-height")
             .value_name("VALUE")
             .help("Minimum tree height for NTree index.")
             .takes_value(true)
             .default_value("3"))
        .arg(Arg::with_name("max-block-size")
             .display_order(8)
             .long("max-block-size")
             .value_name("VALUE")
             .help("Maximum block size for NTree index.")
             .takes_value(true)
             .default_value("64"))
        .arg(Arg::with_name("mem-limit-power")
             .display_order(9)
             .long("mem-limit-power")
             .value_name("VALUE")
             .help("Power limit for memory part of bin-merge-pile index compiler.")
             .takes_value(true)
             .default_value("16"))
        .arg(Arg::with_name("merge-pile-threads")
             .display_order(10)
             .long("merge-pile-threads")
             .value_name("COUNT")
             .help("Number of threads for bin-merge-pile index compiler.")
             .takes_value(true)
             .default_value("1"))
        .arg(Arg::with_name("mmap-type")
             .display_order(11)
             .long("mmap-type")
             .value_name("TYPE")
             .possible_values(&["mmap", "malloc"])
             .help("Mmap type for RO windows.")
             .takes_value(true)
             .default_value("mmap"))
        .arg(Arg::with_name("data-access")
             .display_order(12)
             .long("data-access")
             .value_name("ACCESS")
             .possible_values(&["file-seek", "memory-cache"])
             .help("Data access for RO windows.")
             .takes_value(true)
             .default_value("file-seek"))
        .arg(Arg::with_name("madvise-willneed")
             .display_order(13)
             .long("madvise-willneed")
             .help("Set WILLNEED madvise(2) flag for mmap area."))
        .arg(Arg::with_name("signature-length")
             .display_order(14)
             .long("signature-length")
             .value_name("LENGTH")
             .help("Signature length hd param to use.")
             .takes_value(true))
        .arg(Arg::with_name("shingle-length")
             .display_order(15)
             .long("shingle-length")
             .value_name("LENGTH")
             .help("Shingle length hd param to use.")
             .takes_value(true))
        .arg(Arg::with_name("similarity-threshold")
             .display_order(16)
             .long("similarity-threshold")
             .value_name("VALUE")
             .help("Similarity threshold hd param to use.")
             .takes_value(true))
        .arg(Arg::with_name("band-min-probability")
             .display_order(17)
             .long("band-min-probability")
             .value_name("VALUE")
             .help("Band minimum probability hd param to use.")
             .takes_value(true))
        .arg(Arg::with_name("stop-dict")
             .display_order(18)
             .short("s")
             .long("stop-dict")
             .value_name("FILE")
             .help("Stop words file (replaces default if provided).")
             .takes_value(true))
        .arg(Arg::with_name("daemonize")
             .display_order(19)
             .long("daemonize")
             .help("Daemonize server."))
        .arg(Arg::with_name("redirect-stderr")
             .display_order(20)
             .long("redirect-stderr")
             .value_name("FILE")
             .help("Redirect stderr to given file when daemonize.")
             .takes_value(true)
             .default_value("/dev/null"))
        .arg(Arg::with_name("pid-file")
             .display_order(21)
             .long("pid-file")
             .value_name("FILE")
             .help("Save pid of the process into this file.")
             .takes_value(true))
        .arg(Arg::with_name("sd")
             .display_order(22)
             .multiple(true)
             .long("sd")
             .value_name("WORD")
             .help("Additional stop word for hash-dupl.")
             .takes_value(true))
        .get_matches();

    match bootstrap(&matches) {
        Ok(()) =>
            (),
        Err(cause) => {
            let _ = match cause {
                Error::Clap(ce) =>
                    writeln!(&mut io::stderr(), "{}", ce),
                other =>
                    writeln!(&mut io::stderr(), "An error occurred:\n{:?}", other),
            };
            let _ = writeln!(&mut io::stderr(), "{}", matches.usage());
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::io::Write;
    use zmq;
    use rand::{thread_rng, Rng};
    use bin_merge_pile::ntree_bkd::mmap;
    use hash_dupl::backend::pile_lookup::DataAccess;
    use dupl_server_proto::{Trans, Req, Rep, Workload, LookupTask, LookupType, PostAction};
    use dupl_server_proto::{InsertCond, ClusterAssign, ClusterChoice, AssignCond, LookupResult, Match};
    use dupl_server_proto::bin::{ToBin, FromBin};
    use super::{entrypoint, default_stop_dict};

    fn tx_sock(packet: Trans<String>, sock: &mut zmq::Socket) {
        let required = packet.encode_len();
        let mut msg = zmq::Message::with_capacity(required);
        packet.encode(&mut msg);
        sock.send(msg, 0).unwrap();
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

    fn ensure_tmp_node_id() {
        let mut node_id = fs::File::create("/tmp/node.id").unwrap();
        let digits = "9019".as_bytes();
        node_id.write_all(digits).unwrap();
    }

    #[test]
    fn start_stop() {
        ensure_tmp_node_id();
        let _ = fs::remove_dir_all("/tmp/windows_dupl_server_a");
        let mut app = entrypoint(
            "ipc:///tmp/dupl_server_a",
            "/tmp/windows_dupl_server_a",
            "/tmp/dupl_server_a.key",
            Some("/tmp/node.id"),
            None, None, None, None, None, None, None, None, None, None, default_stop_dict(),
            mmap::MmapType::Malloc,
            DataAccess::FileSeek,
        ).unwrap();
        let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
        sock.connect("ipc:///tmp/dupl_server_a").unwrap();
        tx_sock(Trans::Sync(Req::Init), &mut sock);
        match rx_sock(&mut sock) { Rep::InitAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        tx_sock(Trans::Sync(Req::Terminate), &mut sock);
        match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        app.join();
    }

    #[test]
    fn insert_lookup() {
        ensure_tmp_node_id();
        let _ = fs::remove_dir_all("/tmp/windows_dupl_server_b");
        let mut app = entrypoint(
            "ipc:///tmp/dupl_server_b",
            "/tmp/windows_dupl_server_b",
            "/tmp/dupl_server_b.key",
            Some("/tmp/node.id"),
            None, None, None, None, None, None, None, None, None, None, default_stop_dict(),
            mmap::MmapType::Malloc,
            DataAccess::FileSeek,
        ).unwrap();
        let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
        sock.connect("ipc:///tmp/dupl_server_b").unwrap();
        // initially empty: expect EmptySet
        tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
            text: "arbitrary text".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None,
        }))), &mut sock);
        match rx_sock(&mut sock) { Rep::Result(Workload::Single(LookupResult::EmptySet)) => (), rep => panic!("unexpected rep: {:?}", rep), }
        // try to add something
        tx_sock(Trans::Sync(Req::Lookup(Workload::Many(vec![LookupTask {
            text: "cat dog mouse bird wolf bear fox hare".to_owned(),
            result: LookupType::BestOrMine,
            post_action: PostAction::InsertNew {
                cond: InsertCond::Always,
                assign: ClusterAssign {
                    cond: AssignCond::Always,
                    choice: ClusterChoice::ClientChoice(17),
                },
                user_data: "1177".to_owned(),
            },
        }, LookupTask {
            text: "elephant tiger lion jackal crocodile cat dog".to_owned(),
            result: LookupType::Best,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(1.0),
                assign: ClusterAssign {
                    cond: AssignCond::Always,
                    choice: ClusterChoice::ClientChoice(28),
                },
                user_data: "2288".to_owned(),
            },
        }, LookupTask {
            text: "coyote cat dog mouse bird wolf bear fox".to_owned(),
            result: LookupType::All,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(0.95),
                assign: ClusterAssign {
                    cond: AssignCond::Always,
                    choice: ClusterChoice::ClientChoice(39),
                },
                user_data: "3399".to_owned(),
            },
        }, LookupTask {
            text: "tiger lion jackal crocodile cat dog".to_owned(),
            result: LookupType::All,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(0.5),
                assign: ClusterAssign {
                    cond: AssignCond::Always,
                    choice: ClusterChoice::ClientChoice(40),
                },
                user_data: "4400".to_owned(),
            },
        }]))), &mut sock);
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
        tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
            text: "cat dog mouse bird wolf bear fox hare".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None, }))), &mut sock);
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
        tx_sock(Trans::Sync(Req::Lookup(Workload::Single(LookupTask {
            text: "tiger lion jackal crocodile cat".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None, }))), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Neighbours(Workload::Single(Match {
                cluster_id: 28,
                similarity: sim,
                user_data: ref data, ..
            })))) if sim >= 0.4 && data == "2288" => (),
            rep => panic!("unexpected rep: {:?}", rep),
        }
        // terminate
        tx_sock(Trans::Async(Req::Terminate), &mut sock);
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
        let texts: Vec<_> = (0 .. 100).map(|_| gen_text()).collect();
        {
            ensure_tmp_node_id();
            let _ = fs::remove_dir_all("/tmp/windows_dupl_server_c");
            let mut app = entrypoint(
                "ipc:///tmp/dupl_server_c",
                "/tmp/windows_dupl_server_c",
                "/tmp/dupl_server_c.key",
                Some("/tmp/node.id"),
                None, None, None, None, Some(8), Some(10), None, None, None, None, default_stop_dict(),
                mmap::MmapType::Malloc,
                DataAccess::FileSeek,
            ).unwrap();
            let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
            sock.connect("ipc:///tmp/dupl_server_c").unwrap();

            // check & fill
            for (i, text) in texts.iter().enumerate() {
                tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
                    text: text.clone(),
                    result: LookupType::BestOrMine,
                    post_action: PostAction::InsertNew {
                        cond: InsertCond::Always,
                        assign: ClusterAssign {
                            cond: AssignCond::Always,
                            choice: ClusterChoice::ClientChoice(i as u64),
                        },
                        user_data: text.clone(),
                    },
                }))), &mut sock);
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
            tx_sock(Trans::Sync(Req::Terminate), &mut sock);
            match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
            app.join();
        }
        {
            let mut app = entrypoint(
                "ipc:///tmp/dupl_server_c",
                "/tmp/windows_dupl_server_c",
                "/tmp/dupl_server_c.key",
                Some("/tmp/node.id"),
                None, None, None, None, Some(8), Some(10), None, None, None, None, default_stop_dict(),
                mmap::MmapType::Malloc,
                DataAccess::FileSeek,
            ).unwrap();
            let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
            sock.connect("ipc:///tmp/dupl_server_c").unwrap();

            // check
            for (i, text) in texts.iter().enumerate() {
                tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
                    text: text.clone(),
                    result: LookupType::BestOrMine,
                    post_action: PostAction::None,
                }))), &mut sock);
                match rx_sock(&mut sock) {
                    Rep::Result(Workload::Single(LookupResult::Best(Match {
                        cluster_id: id,
                        similarity: sim,
                        user_data: ref data, ..
                    }))) if i >= 20 && sim >= 0.99 && id == i as u64 && data == text => (),
                    Rep::Result(Workload::Single(LookupResult::EmptySet)) if i < 20 => (),
                    rep => panic!("unexpected rep: {:?}", rep),
                }
            }

            // terminate
            tx_sock(Trans::Sync(Req::Terminate), &mut sock);
            match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
            app.join();
        }
    }

    #[test]
    fn stop_words() {
        ensure_tmp_node_id();
        let _ = fs::remove_dir_all("/tmp/windows_dupl_server_d");
        let mut custom_stop_dict = default_stop_dict();
        custom_stop_dict.push("javascript".to_owned());
        custom_stop_dict.push("php".to_owned());
        let mut app = entrypoint(
            "ipc:///tmp/dupl_server_d",
            "/tmp/windows_dupl_server_d",
            "/tmp/dupl_server_d.key",
            Some("/tmp/node.id"),
            None, None, None, None, None, None, None, None, None, None, custom_stop_dict,
            mmap::MmapType::Malloc,
            DataAccess::FileSeek
        ).unwrap();
        let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
        sock.connect("ipc:///tmp/dupl_server_d").unwrap();

        let text_a = "The explicit continuation of FFree also makes it easier to change its representation.".to_owned();
        let text_b = "The, explicit and continuation. of? FFree! also-makes http://t.co it easier php to \"change\" its javascript representation."
            .to_owned();

        // add text_a
        tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
            text: text_a.clone(),
            result: LookupType::BestOrMine,
            post_action: PostAction::InsertNew {
                cond: InsertCond::Always,
                assign: ClusterAssign {
                    cond: AssignCond::Always,
                    choice: ClusterChoice::ClientChoice(177),
                },
                user_data: text_a.clone(),
            },
        }))), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Best(Match {
                cluster_id: 177,
                similarity: sim,
                user_data: ref data, ..
            }))) if sim >= 0.99 && data == &text_a => (),
            rep => panic!("unexpected rep: {:?}", rep),
        }

        // check text_a
        tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
            text: text_a.clone(),
            result: LookupType::BestOrMine,
            post_action: PostAction::None,
        }))), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Best(Match {
                cluster_id: 177,
                similarity: sim,
                user_data: ref data, ..
            }))) if sim >= 0.99 && data == &text_a => (),
            rep => panic!("unexpected rep: {:?}", rep),
        }

        // check text_b
        tx_sock(Trans::Sync(Req::Lookup(Workload::Single(LookupTask {
            text: text_b.clone(),
            result: LookupType::BestOrMine,
            post_action: PostAction::None,
        }))), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Best(Match {
                cluster_id: 177,
                similarity: sim,
                user_data: ref data, ..
            }))) if sim >= 0.99 && data == &text_a => (),
            rep => panic!("unexpected rep: {:?}", rep),
        }

        // terminate
        tx_sock(Trans::Async(Req::Terminate), &mut sock);
        match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        app.join();
    }

    #[test]
    fn insert_cluster_best_sim() {
        ensure_tmp_node_id();
        let _ = fs::remove_dir_all("/tmp/windows_dupl_server_e");
        let mut app = entrypoint(
            "ipc:///tmp/dupl_server_e",
            "/tmp/windows_dupl_server_e",
            "/tmp/dupl_server_e.key",
            Some("/tmp/node.id"),
            None, None, None, None, None, None, None, None, None, None, default_stop_dict(),
            mmap::MmapType::Malloc,
            DataAccess::FileSeek
        ).unwrap();
        let mut sock = app._zmq_ctx.socket(zmq::REQ).unwrap();
        sock.connect("ipc:///tmp/dupl_server_e").unwrap();
        // try to add something
        tx_sock(Trans::Sync(Req::Lookup(Workload::Many(vec![LookupTask {
            text: "cat dog mouse bird wolf bear fox hare".to_owned(),
            result: LookupType::BestOrMine,
            post_action: PostAction::InsertNew {
                cond: InsertCond::Always,
                assign: ClusterAssign {
                    cond: AssignCond::Always,
                    choice: ClusterChoice::ClientChoice(17),
                },
                user_data: "1177".to_owned(),
            },
        }, LookupTask {
            text: "cat dog mouse bird wolf bear fox".to_owned(),
            result: LookupType::Best,
            post_action: PostAction::InsertNew {
                cond: InsertCond::BestSimLessThan(1.0),
                assign: ClusterAssign {
                    cond: AssignCond::BestSimLessThan(0.6),
                    choice: ClusterChoice::ServerChoice,
                },
                user_data: "2288".to_owned(),
            },
        }]))), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Many(ref workloads)) => {
                assert_eq!(workloads.len(), 2);
                match workloads.get(0) {
                    Some(&LookupResult::Best(Match { cluster_id: 17, similarity: 1.0, user_data: ref data, })) if data == "1177" => (),
                    other => panic!("unexpected rep workload 0: {:?}", other),
                }
                match workloads.get(1) {
                    Some(&LookupResult::Best(Match { cluster_id: 17, similarity: sim, user_data: ref data, })) if sim >= 0.6 && data == "1177" => (),
                    other => panic!("unexpected rep workload 1: {:?}", other),
                }
            },
            rep => panic!("unexpected rep: {:?}", rep),
        }
        // try to search common stuff, both should be found
        tx_sock(Trans::Async(Req::Lookup(Workload::Single(LookupTask {
            text: "cat dog mouse bird wolf bear fox".to_owned(),
            result: LookupType::All,
            post_action: PostAction::None, }))), &mut sock);
        match rx_sock(&mut sock) {
            Rep::Result(Workload::Single(LookupResult::Neighbours(Workload::Many(mut workloads)))) => {
                assert_eq!(workloads.len(), 2);
                workloads.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap());
                match workloads.get(0) {
                    Some(&Match { cluster_id: 17, similarity: 1.0, user_data: ref data, .. }) if data == "2288" => (),
                    other => panic!("unexpected rep neighbour 0: {:?}", other),
                }
                match workloads.get(1) {
                    Some(&Match { cluster_id: 17, similarity: sim, user_data: ref data, .. }) if sim >= 0.6 && data == "1177" => (),
                    other => panic!("unexpected rep neighbour 1: {:?}", other),
                }
            },
            rep => panic!("unexpected rep: {:?}", rep),
        }
        // terminate
        tx_sock(Trans::Async(Req::Terminate), &mut sock);
        match rx_sock(&mut sock) { Rep::TerminateAck => (), rep => panic!("unexpected rep: {:?}", rep), }
        app.join();
    }
}
