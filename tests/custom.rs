#![allow(dead_code, unused_imports)]

use std::{ffi::OsStr, fs, path::Path};

use rskv::{get_kvstore_data_dir, KvStore, Result};

#[test]
fn test_sorted_gen_list() -> Result<()> {
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

    let path = get_kvstore_data_dir();

    println!("{:?}", sorted_fids(&path)?);

    Ok(())
}

#[test]
fn test_sled() {
    use sled::IVec;

    let db = sled::open("my_db").unwrap();
    db.insert(b"yo!", b"v1").unwrap();
    assert_eq!(db.get(b"yo!"), Ok(Some(IVec::from(b"v1"))));
}
