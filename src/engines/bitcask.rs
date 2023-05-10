use std::{
    cell::RefCell,
    collections::{hash_map, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::SystemTime,
};

use dashmap::DashMap;
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsEngine, KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The [Bitcask] stores string key/value pairs into disk.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// ## Terminology
///
/// * `command` - A request or the representation of a request made to the database.
/// These are issued on the command line or over the network.
/// They have an in-memory representation, a textual representation, and a machine-readable serialized representation.
///
/// * `log` - An on-disk sequence of commands, in the order originally received and executed.
/// Our database's on-disk format is almost entirely made up of logs.
/// It will be simple, but also surprisingly efficient.
///
/// * `log pointer` - A file offset into the log. Sometimes we'll just call this a "file offset".
///
/// * `log compaction` - As writes are issued to the database they sometimes invalidate old log entries.
/// For example, writing key/value a = 0 then writing a = 1, makes the first log entry for "a" useless.
/// Compaction — in our database at least — is the process of reducing the size of the database by remove stale commands from the log.
///
/// * `in-memory index` (or `index`) - A map of keys to log pointers.
/// When a read request is issued, the in-memory index is searched for the appropriate log pointer,
/// and when it is found the value is retrieved from the on-disk log.
/// In our key/value store, like in bitcask, the index for the entire database is stored in memory.
///
/// * `index file` - The on-disk representation of the in-memory index.
/// Without this the log would need to be completely replayed to restore
/// the state of the in-memory index each time the database is started.
#[derive(Clone)]
pub struct Bitcask {
    /// [Bitcask] build caches to quickly find reader belongs to `fid` using `HashMap`.
    ///
    /// This hashmap insert all exsiting log files when [Bitcask]::open is called.
    reader: Reader,
    /// Current writer to write `command`s into disk
    cur_writer: Arc<Mutex<Writer>>,

    /// In-memory Index maps from keys(String) to [CmdPos].
    ///
    /// This is a `B-Tree` which would load `log files` in the disk into memory when [Bitcask]::open is called.
    index: Arc<DashMap<String, CmdPos>>,
}

impl Bitcask {
    /// Open the [Bitcask] at a given path. Return the [Bitcask].
    ///  
    /// This will create a new directory to store log files if the given one does not exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        // open or create a directory to store log files
        let data_path = Arc::new(path.into());
        fs::create_dir_all(&*data_path)?;

        let mut readers = HashMap::new();
        let mut index = Arc::new(DashMap::new());

        let fids = sorted_fids(&*data_path)?;
        let mut uncompacted = 0;

        // Indexing and building cache of readers
        for &fid in &fids {
            let mut reader = new_log_reader(&data_path, fid)?;
            uncompacted += Self::load(fid, &mut reader, &mut index)?;
            readers.insert(fid, reader);
        }

        // Create a new log file which fid = (max of fids) + 1
        let cur_fid = *fids.last().unwrap_or(&0) + 1;
        let cur_writer = new_log_writer(&data_path, cur_fid)?;

        let reader = Reader {
            data_path: Arc::clone(&data_path),
            safe_point: Arc::new(AtomicU64::new(0)),
            readers: RefCell::new(readers),
        };

        let writer = Writer {
            data_path: Arc::clone(&data_path),
            reader: reader.clone(),
            cur_writer,
            cur_fid,
            uncompacted,
            index: Arc::clone(&index),
        };

        Ok(Self {
            reader,
            cur_writer: Arc::new(Mutex::new(writer)),
            index,
        })
    }

    /// Load the whole log file and store value locations in the index map.
    ///
    /// Returns how many bytes can be saved after a compaction.
    fn load(
        fid: u64,
        reader: &mut BufReaderWithPos<File>,
        index: &DashMap<String, CmdPos>,
    ) -> Result<u64> {
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut uncompacted = 0;
        // deserialize all `command`s of this log file into a iterator
        let mut stream = Deserializer::from_reader(reader).into_iter::<Cmd>();

        // indexing
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Cmd::Set { key, .. } => {
                    if let Some(.., old_cmd) = index.insert(key, (fid, pos..new_pos).into()) {
                        uncompacted += old_cmd.len;
                    }
                }
                Cmd::Rm { key } => {
                    if let Some((.., old_cmd)) = index.remove(&key) {
                        uncompacted += old_cmd.len;
                    }
                    // the "remove" command itself can be deleted in the next compaction.
                    // so we add its length to `uncompacted`.
                    uncompacted += new_pos - pos;
                }
            }
            pos = new_pos;
        }

        Ok(uncompacted)
    }
}

impl KvsEngine for Bitcask {
    /// Set the value of a string key to a string
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.cur_writer.lock().unwrap().set(key, value)
    }

    /// Get the string value of a given string key
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            self.reader.read_command(&cmd_pos)
        } else {
            Ok(None)
        }
    }

    /// Remove a given key
    ///   
    /// ## Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn rm(&self, key: String) -> Result<()> {
        self.cur_writer.lock().unwrap().rm(key)
    }
}

///
struct Reader {
    data_path: Arc<PathBuf>,
    // generation file number of the latest compaction file
    safe_point: Arc<AtomicU64>,
    readers: RefCell<HashMap<u64, BufReaderWithPos<File>>>,
}

impl Reader {
    /// Close file handles with generation file number less than safe_point.
    ///
    /// `safe_point` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than safe_point.
    /// So we can safely close those file handles and the stale files can be deleted.
    fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_fid = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= first_fid {
                break;
            }
            readers.remove(&first_fid);
        }
    }

    /// First Call `close_stale_handles`. Then Read the on-disk command and apply `f` to that command
    fn read_and<F, R>(&self, cmd_pos: &CmdPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        self.close_stale_handles();

        let mut readers = self.readers.borrow_mut();

        // Open the file if we haven't opened it in this `KvStoreReader`.
        // Using entry API avoid double call hashmap's insert.
        if let hash_map::Entry::Vacant(entry) = readers.entry(cmd_pos.fid) {
            let new_reader =
                BufReaderWithPos::new(File::open(log_path(&self.data_path, cmd_pos.fid))?)?;

            entry.insert(new_reader);
        }

        // Get the reader via readers hashmap
        let reader_with_pos = readers.get_mut(&cmd_pos.fid).expect(&format!(
            "Unable find the log reader which fid: {}",
            &cmd_pos.fid
        ));

        reader_with_pos.seek(SeekFrom::Start(cmd_pos.pos))?;
        // cmd_reader read up to cmd_pos.len bytes
        let cmd_reader = reader_with_pos.take(cmd_pos.len);
        f(cmd_reader)
    }

    // Read the command on the disk and deserialize it to in-memory `Command`.
    fn read_command(&self, cmd_pos: &CmdPos) -> Result<Option<String>> {
        self.read_and(cmd_pos, |cmd_reader| {
            if let Cmd::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::Unknown)
            }
        })
    }
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Self {
            data_path: Arc::clone(&self.data_path),
            safe_point: Arc::clone(&self.safe_point),
            readers: RefCell::new(HashMap::new()),
        }
    }
}

///
struct Writer {
    data_path: Arc<PathBuf>,
    reader: Reader,
    cur_writer: BufWriterWithPos<File>,
    cur_fid: u64,
    /// The number of bytes representing "stale" commands that could be
    /// deleted during a compaction.
    uncompacted: u64,
    index: Arc<DashMap<String, CmdPos>>,
}

impl Writer {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd::set(key, value);
        let pos = self.cur_writer.pos;
        serde_json::to_writer(&mut self.cur_writer, &cmd)?;
        self.cur_writer.flush()?;

        if let Cmd::Set { key, .. } = cmd {
            self.uncompacted += self
                .index
                .insert(key, (self.cur_fid, pos..self.cur_writer.pos).into())
                .map(|cmd_pos| cmd_pos.len)
                .unwrap_or(0)
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            let now = SystemTime::now();
            info!("Compaction starts");
            self.compact()?;
            info!("Compaction finished, cost {:?}", now.elapsed().unwrap());
        }

        Ok(())
    }

    fn rm(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Cmd::rm(key);
            let pos = self.cur_writer.pos;
            serde_json::to_writer(&mut self.cur_writer, &cmd)?;
            self.cur_writer.flush()?;

            if let Cmd::Rm { key } = cmd {
                self.uncompacted += self
                    .index
                    .remove(&key)
                    .map(|(.., old_cmd_pos)| old_cmd_pos)
                    .expect("key not found")
                    .len;
                // the "remove" command itself can be deleted in the next compaction
                // so we add its length to `uncompacted`
                self.uncompacted += self.cur_writer.pos - pos;
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale log files.
    fn compact(&mut self) -> Result<()> {
        // increase current fid by 2. current_fid + 1 is for the compaction file.
        let compaction_fid = self.cur_fid + 1;
        self.cur_fid += 2;
        self.cur_writer = new_log_writer(&self.data_path, self.cur_fid)?;

        let mut compaction_writer = new_log_writer(&self.data_path, compaction_fid)?;

        let mut new_pos = 0;
        // copy all valid commands(from index) into compaction file, be careful about deadlock when iterating dashmap
        for mut entry in self.index.iter_mut() {
            let cmd_pos = entry.value_mut();
            let len = self.reader.read_and(cmd_pos, |mut cmd_reader| {
                Ok(io::copy(&mut cmd_reader, &mut compaction_writer)?)
            })?;

            *cmd_pos = CmdPos {
                fid: compaction_fid,
                pos: new_pos,
                len,
            };

            new_pos += len;
        }
        compaction_writer.flush()?;

        // update safe_point
        self.reader
            .safe_point
            .store(compaction_fid, Ordering::SeqCst);
        self.reader.close_stale_handles();

        // remove stale log files
        // Note that actually these files are not deleted immediately because `KvStoreReader`s
        // still keep open file handles. When `KvStoreReader` is used next time, it will clear
        // its stale file handles. On Unix, the files will be deleted after all the handles
        // are closed. On Windows, the deletions below will fail and stale files are expected
        // to be deleted in the next compaction.

        let stale_fids = sorted_fids(&*self.data_path)?
            .into_iter()
            .filter(|&fid| fid < compaction_fid);

        for stale_fid in stale_fids {
            let file_path = log_path(&self.data_path, stale_fid);
            if let Err(e) = fs::remove_file(&file_path) {
                error!("{:?} cannot be deleted: {}", file_path, e);
            }
        }
        self.uncompacted = 0;

        Ok(())
    }
}

/// Return a sorted list of log file generated by [Bitcask].
fn sorted_fids(path: impl AsRef<Path>) -> Result<Vec<u64>> {
    let mut fids: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    fids.sort_unstable();

    Ok(fids)
}

/// join path: {dir}/{fid}.log
fn log_path(dir: &Path, fid: u64) -> PathBuf {
    dir.join(format!("{}.log", fid))
}

/// Create a new [BufReaderWithPos] for `fid`'s log file.
fn new_log_reader(dir: &Path, fid: u64) -> Result<BufReaderWithPos<File>> {
    BufReaderWithPos::new(File::open(log_path(&dir, fid))?)
}

/// Creat a new log file with `fid` and return the writer to the log.
fn new_log_writer(path: &Path, fid: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, fid);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;

    Ok(writer)
}

#[derive(Debug, Serialize, Deserialize)]
enum Cmd {
    Set { key: String, value: String },
    Rm { key: String },
}

impl Cmd {
    fn set(key: String, value: String) -> Self {
        Cmd::Set { key, value }
    }

    fn rm(key: String) -> Self {
        Cmd::Rm { key }
    }
}

#[derive(Debug, Clone)]
/// In-memory representation of a `command`.
///
/// Indicates where we can find it.
pub struct CmdPos {
    /// which file this commmand belong to
    fid: u64,
    /// start position of command
    pos: u64,
    /// length of command
    len: u64,
}

impl From<(u64, Range<u64>)> for CmdPos {
    fn from((fid, range): (u64, Range<u64>)) -> Self {
        CmdPos {
            fid,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

/// A `BufReader` with position where it read to
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

/// A `BufWriter` with position where it read to
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
