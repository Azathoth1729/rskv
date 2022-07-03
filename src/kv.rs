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

use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
pub struct KvStore {
    /// directory for log data
    path: PathBuf,
    /// because kvs is log-structured, it have may have many log files, using hashmap to index
    readers: HashMap<u64, BufReaderWithPos<File>>,
    /// writer
    writer: BufWriterWithPos<File>,
    /// current file
    cur_fid: u64,
    /// index maps from string keys to log pointers
    index: BTreeMap<String, CmdPos>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    uncompacted: u64,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    ///  
    /// This will create a new directory if the given one does not exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        // open or create dir path to store log files
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let fids = sorted_fids(&path)?;
        let mut uncompacted = 0;

        // Indexing and readers
        for &fid in &fids {
            let mut reader = new_log_reader(&path, fid)?;
            uncompacted += Self::index(fid, &mut reader, &mut index)?;
            readers.insert(fid, reader);
        }

        // Create the new last log file
        let cur_fid = *fids.last().unwrap_or(&0) + 1;
        let writer = new_log_writer(&path, cur_fid)?;

        readers.insert(cur_fid, new_log_reader(&path, cur_fid)?);

        Ok(KvStore {
            path,
            readers,
            writer,
            cur_fid,
            index,
            uncompacted,
        })
    }

    /// Set the value of a string key to a string
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Cmd::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .index
                .insert(key, (self.cur_fid, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Get the string value of a given string key
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.fid)
                .expect("Cannot find log reader");

            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
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
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Cmd::rm(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Cmd::Rm { key } = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;

                if self.uncompacted > COMPACTION_THRESHOLD {
                    self.compact()?;
                }
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log.
    pub fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_fid = self.cur_fid + 1;
        self.cur_fid += 2;
        self.writer = self.new_log_file(self.cur_fid)?;

        let mut compaction_writer = self.new_log_file(compaction_fid)?;

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

        // remove stale log files.
        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&fid| fid < compaction_fid)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, fid: u64) -> Result<BufWriterWithPos<File>> {
        let writer = new_log_writer(&self.path, fid)?;
        self.readers.insert(fid, new_log_reader(&self.path, fid)?);

        Ok(writer)
    }

    /// Indexing one log file
    ///
    /// Returns how many bytes can be saved after a compaction.
    fn index(
        fid: u64,
        reader: &mut BufReaderWithPos<File>,
        index: &mut BTreeMap<String, CmdPos>,
    ) -> Result<u64> {
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = Deserializer::from_reader(reader).into_iter::<Cmd>();
        let mut uncompacted = 0; // number of bytes that can be saved after a compaction.

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
/// struct representing one on-disk command position
pub struct CmdPos {
    /// which file does this commmand belong to
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

/// Returns sorted generation file numbers in the given directory.
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

fn new_log_reader(path: &Path, fid: u64) -> Result<BufReaderWithPos<File>> {
    BufReaderWithPos::new(File::open(log_path(&path, fid))?)
}

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
