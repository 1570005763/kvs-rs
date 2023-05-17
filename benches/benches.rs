use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use crossbeam_utils::sync::WaitGroup;
use rand_chacha::ChaChaRng;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use tempfile::TempDir;

use kvs::{KvStore, SledKvsEngine, KvsEngine, thread_pool::*};

static SEED: u64 = 0;

static KV_LEN: u64 = 1000;
static KEY_LEN: usize = 100000;
static VALUE: &str = "0";

fn get_random_kv_vec(k_range: usize, value: String, length: u64) -> Vec<(String, String)> {
    let mut rand = ChaChaRng::seed_from_u64(SEED);

    let res: Vec<(String, String)> = (0..length)
        .map(|_| (
            get_a_string(&mut rand, k_range),
            value.clone(),
        ))
        .collect();
    return res;

    fn get_a_string(rand: &mut ChaChaRng, range: usize) -> String {
        let v: &mut [u8] = {
            &mut Vec::with_capacity(range)
        };
        rand.fill_bytes(v);
        String::from_utf8_lossy(v).into_owned()
    }
}

fn write_queued_kvstore(c: &mut Criterion) {
    let thread_nums = &[1, 2, 4, 8, 16, 32];

    c.bench_function_over_inputs("write_queued_kvstore", |b, &&cpu_core_num| {
        // do setup here
        let engine = KvStore::open(TempDir::new().unwrap().path()).unwrap();
        let pool = SharedQueueThreadPool::new(cpu_core_num).unwrap();
        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_LEN, VALUE.to_string().clone(), KV_LEN);

        b.iter(|| {
            let wg = WaitGroup::new();
            for (key, value) in kv_vec.clone() {
                let engine = engine.clone();
                let wg = wg.clone();
                pool.spawn(move || {
                    assert!(engine.set(key, value).is_ok());
                    drop(wg);
                });
            }
            wg.wait();
        });
    }, thread_nums);
}

fn read_queued_kvstore(c: &mut Criterion) {
    let thread_nums = &[1, 2, 4, 8, 16, 32];

    c.bench_function_over_inputs("read_queued_kvstore", |b, &&cpu_core_num| {
        // do setup here
        let engine = KvStore::open(TempDir::new().unwrap().path()).unwrap();
        let pool = SharedQueueThreadPool::new(cpu_core_num).unwrap();
        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_LEN, VALUE.to_string().clone(), KV_LEN);

        for (key, value) in kv_vec.clone() {
            engine.set(key, value).unwrap();
        }

        b.iter(|| {
            let wg = WaitGroup::new();
            for (key, value) in kv_vec.clone() {
                let engine = engine.clone();
                let wg = wg.clone();
                pool.spawn(move || {
                    assert_eq!(engine.get(key).unwrap(), Some(value));
                    drop(wg);
                });
            }
            wg.wait();
        });
    }, thread_nums);
}

fn write_rayon_kvstore(c: &mut Criterion) {
    let thread_nums = &[1, 2, 4, 8, 16, 32];

    c.bench_function_over_inputs("write_queued_kvstore", |b, &&cpu_core_num| {
        // do setup here
        let engine = KvStore::open(TempDir::new().unwrap().path()).unwrap();
        let pool = RayonThreadPool::new(cpu_core_num).unwrap();
        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_LEN, VALUE.to_string().clone(), KV_LEN);

        b.iter(|| {
            let wg = WaitGroup::new();
            for (key, value) in kv_vec.clone() {
                let engine = engine.clone();
                let wg = wg.clone();
                pool.spawn(move || {
                    assert!(engine.set(key, value).is_ok());
                    drop(wg);
                });
            }
            wg.wait();
        });
    }, thread_nums);
}

fn read_rayon_kvstore(c: &mut Criterion) {
    let thread_nums = &[1, 2, 4, 8, 16, 32];

    c.bench_function_over_inputs("read_queued_kvstore", |b, &&cpu_core_num| {
        // do setup here
        let engine = KvStore::open(TempDir::new().unwrap().path()).unwrap();
        let pool = RayonThreadPool::new(cpu_core_num).unwrap();
        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_LEN, VALUE.to_string().clone(), KV_LEN);

        for (key, value) in kv_vec.clone() {
            engine.set(key, value).unwrap();
        }

        b.iter(|| {
            let wg = WaitGroup::new();
            for (key, value) in kv_vec.clone() {
                let engine = engine.clone();
                let wg = wg.clone();
                pool.spawn(move || {
                    assert_eq!(engine.get(key).unwrap(), Some(value));
                    drop(wg);
                });
            }
            wg.wait();
        });
    }, thread_nums);
}

fn write_rayon_sledkvengine(c: &mut Criterion) {
    let thread_nums = &[1, 2, 4, 8, 16, 32];

    c.bench_function_over_inputs("write_queued_kvstore", |b, &&cpu_core_num| {
        // do setup here
        let engine = SledKvsEngine::open(TempDir::new().unwrap().path()).unwrap();
        let pool = RayonThreadPool::new(cpu_core_num).unwrap();
        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_LEN, VALUE.to_string().clone(), KV_LEN);

        b.iter(|| {
            let wg = WaitGroup::new();
            for (key, value) in kv_vec.clone() {
                let engine = engine.clone();
                let wg = wg.clone();
                pool.spawn(move || {
                    assert!(engine.set(key, value).is_ok());
                    drop(wg);
                });
            }
            wg.wait();
        });
    }, thread_nums);
}

fn read_rayon_sledkvengine(c: &mut Criterion) {
    let thread_nums = &[1, 2, 4, 8, 16, 32];

    c.bench_function_over_inputs("read_queued_kvstore", |b, &&cpu_core_num| {
        // do setup here
        let engine = SledKvsEngine::open(TempDir::new().unwrap().path()).unwrap();
        let pool = RayonThreadPool::new(cpu_core_num).unwrap();
        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_LEN, VALUE.to_string().clone(), KV_LEN);

        for (key, value) in kv_vec.clone() {
            engine.set(key, value).unwrap();
        }

        b.iter(|| {
            let wg = WaitGroup::new();
            for (key, value) in kv_vec.clone() {
                let engine = engine.clone();
                let wg = wg.clone();
                pool.spawn(move || {
                    assert_eq!(engine.get(key).unwrap(), Some(value));
                    drop(wg);
                });
            }
            wg.wait();
        });
    }, thread_nums);
}

criterion_group!(benches,
    // write_queued_kvstore, read_queued_kvstore,
    // write_rayon_kvstore, read_rayon_kvstore,
    write_rayon_sledkvengine, read_rayon_sledkvengine);
criterion_main!(benches);