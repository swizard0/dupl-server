extern crate zmq;
extern crate getopts;
extern crate yauid;
extern crate dupl_server_proto;

use std::{io, env, process};
use std::io::Write;
use getopts::{Options, Matches};
use dupl_server_proto as proto;
// use dupl_server_proto::{
//     Req, Workload, LookupTask, LookupType, PostAction, InsertCond, ClusterAssign,
//     Rep, LookupResult, Match,
// };

const INTERNAL_SOCKET_ADDR: &'static str = "inproc://internal";

#[derive(Debug)]
enum ZmqError {
    Socket(zmq::Error),
    Connect(String, zmq::Error),
    Bind(String, zmq::Error),
}

#[derive(Debug)]
enum Error {
    Getopts(getopts::Fail),
    Zmq(ZmqError),
    Proto(proto::bin::Error),
    Yauid(String),
}

fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));

    let mut zmq_ctx = zmq::Context::new();
    Ok(())
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.optopt("z", "zmq-addr", "server zeromq listen address", "");
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

    match entrypoint(opts.parse(args)) {
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
