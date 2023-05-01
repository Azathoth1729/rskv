use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
};

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

pub struct Bitcask {
    /// Directory for sotring log data, it contains many log file
    data_path: PathBuf,
    /// [Bitcask] build caches to quickly find reader belongs to `fid` using `HashMap`.
    ///
    /// This hashmap insert all exsiting log files when [Bitcask]::open is called.
    readers: HashMap<u64, BufReaderWithPos<File>>,
    /// Current writer to write `command`s into disk
    cur_writer: BufWriterWithPos<File>,
    /// log file id which the current writer write to
    cur_fid: u64,
    /// In-memory Index maps from keys(String) to [CmdPos].
    ///
    /// This is a `B-Tree` which would load `log files` in the disk into memory when [Bitcask]::open is called.
    index: BTreeMap<String, CmdPos>,
    /// The number of bytes representing "stale" commands that could be
    /// deleted during a compaction.
    uncompacted: u64,
}

impl Bitcask {
    /// Open the [Bitcask] at a given path. Return the [Bitcask].
    ///  
    /// This will create a new directory to store log files if the given one does not exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        // open or create a directory to store log files
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();
        let mut uncompacted = 0;

        let fids = sorted_fids(&path)?;

        // Indexing and building cache of readers
        for &fid in &fids {
            let mut reader = new_log_reader(&path, fid)?;
            uncompacted += Self::index(fid, &mut reader, &mut index)?;
            readers.insert(fid, reader);
        }

        // Create a new log file which fid = (max of fids) + 1
        let cur_fid = *fids.last().unwrap_or(&0) + 1;
        let cur_writer = new_log_writer(&path, cur_fid)?;
        readers.insert(cur_fid, new_log_reader(&path, cur_fid)?);

        Ok(Bitcask {
            data_path: path,
            readers,
            cur_writer,
            cur_fid,
            index,
            uncompacted,
        })
    }

    /// Clears stale log files.
    fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_fid = self.cur_fid + 1;
        self.cur_fid += 2;
        self.cur_writer = self.log_file(self.cur_fid)?;

        let mut compaction_writer = self.log_file(compaction_fid)?;

        // copy all indexed command into `compaction_writer`
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.fid)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let _len = io::copy(&mut entry_reader, &mut compaction_writer)?;
        }
        compaction_writer.flush()?;

        // Remove stale log files.
        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&fid| fid < compaction_fid)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.data_path, stale_gen))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer of that log file.
    fn log_file(&mut self, fid: u64) -> Result<BufWriterWithPos<File>> {
        let writer = new_log_writer(&self.data_path, fid)?;
        self.readers
            .insert(fid, new_log_reader(&self.data_path, fid)?);

        Ok(writer)
    }

    /// Indexing one log file for index
    ///
    /// Returns how many bytes can be saved after a compaction.
    fn index(
        fid: u64,
        reader: &mut BufReaderWithPos<File>,
        index: &mut BTreeMap<String, CmdPos>,
    ) -> Result<u64> {
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut uncompacted = 0; // number of bytes that can be saved after a compaction.

        // deserialize all `command`s of this log file into a iterator
        let mut stream = Deserializer::from_reader(reader).into_iter::<Cmd>();

        // indexing
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Cmd::Set { key, .. } => {
                    if let Some(old_cmd) = index.insert(key, (fid, pos..new_pos).into()) {
                        uncompacted += old_cmd.len;
                    }
                }
                Cmd::Rm { key } => {
                    if let Some(old_cmd) = index.remove(&key) {
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
            self.compact()?;
        }
        Ok(())
    }

    /// Get the string value of a given string key
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            // get the reader via reader hashmap
            let reader = self.readers.get_mut(&cmd_pos.fid).expect(&format!(
                "Unable find the log reader which fidis {}",
                &cmd_pos.fid
            ));

            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            // cmd_reader read up to cmd_pos.len bytes
            let cmd_reader = reader.take(cmd_pos.len);

            if let Cmd::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::Unknown)
            }
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
    fn rm(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Cmd::rm(key);

            serde_json::to_writer(&mut self.cur_writer, &cmd)?;
            self.cur_writer.flush()?;

            if let Cmd::Rm { key } = cmd {
                self.uncompacted += self.index.remove(&key).expect("key not found").len;
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
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

fn log_path(dir: &Path, fid: u64) -> PathBuf {
    dir.join(format!("{}.log", fid))
}

/// Create a new [BufReaderWithPos] for `fid`'s log file.
fn new_log_reader(dir: &Path, fid: u64) -> Result<BufReaderWithPos<File>> {
    BufReaderWithPos::new(File::open(log_path(&dir, fid))?)
}

/// Creat a writer representing append new log file
fn new_log_writer(dir: &Path, fid: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&dir, fid);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;

    Ok(writer)
}
