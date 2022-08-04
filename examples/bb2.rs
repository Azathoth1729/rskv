use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    path::Path,
};

use serde::{Deserialize, Serialize};

use rskv::Result;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
enum Move {
    Up(u32),
    Down(u32),
    Left(u32),
    Right(u32),
}

#[test]
fn ex1() {
    let up = Move::Up(3);
    let left = Move::Left(0);

    let serialized_up = serde_json::to_string(&up).unwrap();
    let serialized_left = serde_json::to_string(&left).unwrap();

    println!("serialize results:\n{}\n{}", serialized_up, serialized_left);

    let deserialized_up: Move = serde_json::from_str(&serialized_up).unwrap();
    let deserialized_left: Move = serde_json::from_str(&serialized_left).unwrap();

    assert_eq!(up, deserialized_up);
    assert_eq!(left, deserialized_left);
}

// I'm too lazy to do this exercise ^_^
#[test]
fn ex2() {}

mod ex3 {
    #![allow(dead_code)]
    use std::io::Write;

    use super::*;

    fn new_json_writer(path: impl AsRef<Path>) -> Result<BufWriter<File>> {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;

        let writer = BufWriter::new(f);
        Ok(writer)
    }

    fn new_json_reader(path: impl AsRef<Path>) -> Result<BufReader<File>> {
        let f = File::open(&path)?;

        let reader = BufReader::new(f);
        Ok(reader)
    }

    fn json_write(mut writer: BufWriter<File>, val: Move) -> Result<()> {
        serde_json::to_writer(&mut writer, &val)?;
        writer.flush()?;
        Ok(())
    }

    fn get_demo_path() -> impl AsRef<Path> {
        Path::new("data/ex3.log")
    }

    #[test]
    fn test_json_write() {
        let writer = new_json_writer(get_demo_path()).unwrap();
        json_write(writer, Move::Right(2)).unwrap();
    }

    #[test]
    fn test_json_read() {
        let reader = new_json_reader(get_demo_path()).unwrap();

        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Move>();

        while let Some(Ok(mv)) = stream.next() {
            println!("{}", serde_json::to_string(&mv).unwrap())
        }
    }
}

fn main() {}
