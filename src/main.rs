extern crate zmq;
extern crate getopts;
extern crate yauid;
extern crate dupl_server_proto;

use std::{io, env, process};
use std::io::Write;
use std::thread::{Builder, sleep, JoinHandle};
use std::sync::mpsc::{channel, sync_channel, Sender, Receiver, TryRecvError};
use getopts::Options;
use yauid::Yauid;
use dupl_server_proto as proto;
use dupl_server_proto::{
    Req, Workload, LookupTask, LookupType, PostAction, InsertCond, ClusterAssign,
    Rep, LookupResult, Match,
};

const INTERNAL_SOCKET_ADDR: &'static str = "inproc://internal";

#[derive(Debug)]
pub enum ZmqError {
    Socket(zmq::Error),
    Connect(String, zmq::Error),
    Bind(String, zmq::Error),
}

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),

    Zmq(ZmqError),
    Proto(proto::bin::Error),
    Yauid(String),
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

    let app = try!(entrypoint(external_zmq_addr,
                              key_file,
                              node_id));
    shutdown_app(app)
}

pub fn entrypoint(external_zmq_addr: String,
                  key_file: String,
                  node_id: Option<String>) -> Result<App, Error>
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
        slave_loop(int_sock_slave, slave_tx, slave_rx, key_file, node_id).unwrap();
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
                Err(TryRecvError::Disconnected) => panic!("master thread is down"),
            }
        }
        if !slave_finished {
            match app.slave_watchdog_rx.try_recv() {
                Ok(()) => slave_finished = true,
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => panic!("slave thread is down"),
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

fn master_loop(ext_sock: zmq::Socket,
               int_sock_master: zmq::Socket,
               tx: Sender<Message<Req<String>>>,
               rx: Receiver<Message<Rep<String>>>) -> Result<(), Error>
{
    Ok(())
}

fn slave_loop(int_sock_slave: zmq::Socket,
              tx: Sender<Message<Rep<String>>>,
              rx: Receiver<Message<Req<String>>>,
              key_file: String,
              node_id: Option<String>) -> Result<(), Error>
{
    let yauid = try!(if let Some(ref node_file) = node_id {
        Yauid::with_node_id(&key_file, node_file)
    } else {
        Yauid::new(&key_file, 1)
    }.map_err(|e| Error::Yauid(e)));

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
    use super::{entrypoint, shutdown_app};

    #[test]
    fn start_stop() {
        let app = entrypoint("ipc:///tmp/dupl_server_a".to_owned(),
                             "/tmp/dupl_server_a.key".to_owned(),
                             None).unwrap();
        shutdown_app(app).unwrap();
    }
}
