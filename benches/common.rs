#![allow(dead_code)]

use std::fs::{File, read_dir, remove_file};
use std::path::Path;
use std::sync::Arc;
use bytes::{Bytes, BytesMut};
use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;
use mutantdb::core::Mutant;

/**
Basically a copy from agateDb
*/
pub fn rand_value() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), 32)
}

pub fn remove_files(path: &Path) {
    read_dir(path).unwrap().for_each(|entry| {
        let entry = entry.unwrap();
        remove_file(entry.path()).unwrap();
    });
    File::open(path).unwrap().sync_all().unwrap();
}

pub fn gen_kv_pair(key: u64, value_size: usize) -> (Bytes, Bytes) {
    let key = Bytes::from(format!("vsz={:05}-k={:010}", value_size, key));

    let mut value = BytesMut::with_capacity(value_size);
    value.resize(value_size, 0);

    (key, value.freeze())
}

pub fn rand_add(
    core: Arc<Mutant>,
    key_nums: u64,
    chunk_size: u64,
    batch_size: u64,
    value_size: usize,
    seq: bool,
) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let agate = core.clone();

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let range = chunk_start..chunk_start + chunk_size;

            for batch_start in range.step_by(batch_size as usize) {
                
                (batch_start..batch_start + batch_size).for_each(|key| {
                    let (key, value) = if seq {
                        gen_kv_pair(key, value_size)
                    } else {
                        gen_kv_pair(rng.gen_range(0..key_nums), value_size)
                    };
                    agate.add(key, value, 0).unwrap();
                });
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn rand_read(mutant: Arc<Mutant>, key_nums: u64, chunk_size: u64, value_size: usize) {
    let mut handles = vec![];

    for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
        let mutant = mutant.clone();

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let range = chunk_start..chunk_start + chunk_size;

            for _ in range {
                let (key, _) = gen_kv_pair(rng.gen_range(0..key_nums), value_size);
                
                match mutant.get(&key) {
                    Some(item) => {
                        assert_eq!(item.value.len(), value_size);
                    }
                    None => {
                        
                    }
                }
            }
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}


// todo: make when iteration will be able to seek
// pub fn rand_iterate(mutant: Arc<Mutant>, key_nums: u64, chunk_size: u64, value_size: usize) {
//     let mut handles = vec![];
// 
//     for chunk_start in (0..key_nums).step_by(chunk_size as usize) {
//         let mutant = mutant.clone();
//         let (key, _) = gen_kv_pair(chunk_start, value_size);
// 
//         handles.push(std::thread::spawn(move || {
//             let mut iter = mutant.into_iter();
//             iter.seek(&key);
//             let mut count = 0;
// 
//             while iter.valid() {
//                 let item = iter.item();
//                 assert_eq!(item.value().len(), value_size);
// 
//                 iter.next();
// 
//                 count += 1;
//                 if count > chunk_size {
//                     break;
//                 }
//             }
//         }));
//     }
// 
//     handles
//         .into_iter()
//         .for_each(|handle| handle.join().unwrap());
// }