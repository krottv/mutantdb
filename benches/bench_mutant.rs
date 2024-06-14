use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;
use mutantdb::core::Mutant;
use mutantdb::db_options::DbOptions;
use crate::common::{rand_add, rand_read, remove_files};

mod common;

// We will process `CHUNK_SIZE` items in a thread, and in one certain thread,
// we will process `BATCH_SIZE` items in a transaction or write batch.
const KEY_NUMS: u64 = 10000;
const CHUNK_SIZE: u64 = 100;
const BATCH_SIZE: u64 = 10;

const SMALL_VALUE_SIZE: usize = 32;
const LARGE_VALUE_SIZE: usize = 4096;

fn get_opts() -> (TempDir, Arc<DbOptions>) {
    let dir = tempfile::Builder::new()
        .prefix("mutantdb-bench-value")
        .tempdir()
        .unwrap();
    let dir_path = dir.path();
    let opts = Arc::new(DbOptions {
        path: dir_path.to_path_buf(),
        ..Default::default()
    });

    (dir, opts)
}

fn bench_mutant(c: &mut Criterion) {
    
    // to test reading
    let (mut last_tmp_dir, mut last_opts) = get_opts();
    
    c.bench_function("mutant sequentially populate small value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                let (tmp_dir, opts) = get_opts();
                last_tmp_dir = tmp_dir;
                last_opts = opts.clone();
                let mutant = Arc::new(Mutant::open(opts).unwrap());
                
                let now = Instant::now();
                rand_add(
                    mutant,
                    KEY_NUMS,
                    CHUNK_SIZE,
                    BATCH_SIZE,
                    SMALL_VALUE_SIZE,
                    true,
                );
                total = total.add(now.elapsed());
            });

            total
        });
    });
    
    // todo: fix seg err
    // c.bench_function("mutant randomly populate small value", |b| {
    //     b.iter_custom(|iters| {
    //         let mut total = Duration::new(0, 0);
    // 
    //         (0..iters).for_each(|_| {
    //             let (tmp_dir, opts) = get_opts();
    // 
    //             last_tmp_dir = tmp_dir;
    //             last_opts = opts.clone();
    //             
    //             let mutant = Arc::new(Mutant::open(opts).unwrap());
    //             
    // 
    //             let now = Instant::now();
    //             rand_add(
    //                 mutant,
    //                 KEY_NUMS,
    //                 CHUNK_SIZE,
    //                 BATCH_SIZE,
    //                 SMALL_VALUE_SIZE,
    //                 false,
    //             );
    //             total = total.add(now.elapsed());
    //         });
    // 
    //         total
    //     });
    // });

    c.bench_function("mutant randread small value", |b| {

        let mutant = Arc::new(Mutant::open(last_opts.clone()).unwrap());
        
        b.iter(|| {
            rand_read(mutant.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
        });
    });

    // c.bench_function("mutant iterate small value", |b| {
    //     b.iter(|| {
    //         mutant_iterate(mutant.clone(), KEY_NUMS, CHUNK_SIZE, SMALL_VALUE_SIZE);
    //     });
    // });


    c.bench_function("mutant sequentially populate large value", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);

            (0..iters).for_each(|_| {
                let (tmp_dir, opts) = get_opts();
                last_tmp_dir = tmp_dir;
                last_opts = opts.clone();
                let mutant = Arc::new(Mutant::open(opts).unwrap());

                let now = Instant::now();
                rand_add(
                    mutant,
                    KEY_NUMS,
                    CHUNK_SIZE,
                    BATCH_SIZE,
                    LARGE_VALUE_SIZE,
                    true,
                );
                total = total.add(now.elapsed());
            });

            total
        });
    });
    
    // todo: fix seg err 
    // c.bench_function("mutant randomly populate large value", |b| {
    //     b.iter_custom(|iters| {
    //         let mut total = Duration::new(0, 0);
    // 
    //         (0..iters).for_each(|_| {
    //             let (tmp_dir, opts) = get_opts();
    //             last_tmp_dir = tmp_dir;
    //             last_opts = opts.clone();
    //             let mutant = Arc::new(Mutant::open(opts).unwrap());
    // 
    //             let now = Instant::now();
    //             rand_add(
    //                 mutant,
    //                 KEY_NUMS,
    //                 CHUNK_SIZE,
    //                 BATCH_SIZE,
    //                 LARGE_VALUE_SIZE,
    //                 false,
    //             );
    //             total = total.add(now.elapsed());
    //         });
    // 
    //         total
    //     });
    // });


    c.bench_function("mutant randread large value", |b| {
        let mutant = Arc::new(Mutant::open(last_opts.clone()).unwrap());
        
        b.iter(|| {
            rand_read(mutant.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
        });
    });
    
    // c.bench_function("mutant iterate large value", |b| {
    //     b.iter(|| {
    //         mutant_iterate(mutant.clone(), KEY_NUMS, CHUNK_SIZE, LARGE_VALUE_SIZE);
    //     });
    // });
}


criterion_group! {
  name = benches_agate_rocks;
  config = Criterion::default();
  targets = bench_mutant
}

criterion_main!(benches_agate_rocks);
