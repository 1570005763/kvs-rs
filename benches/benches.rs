use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use rand_chacha::{ChaChaRng};
use rand_chacha::rand_core::{RngCore, SeedableRng};
use tempfile::TempDir;

use kvs::{KvStore, SledKvsEngine, KvsEngine};

static SEED: u64 = 0;

static KV_LEN: u64 = 100;
static KEY_RANGE: usize = 100000;
static VALUE_RANGE: usize = 100000;
static IDX_LEN: u64 = 1000;

fn get_random_kv_vec(k_range: usize, v_range: usize, length: u64) -> Vec<(String, String)> {
    let mut rand = ChaChaRng::seed_from_u64(SEED);

    let res: Vec<(String, String)> = (0..length)
        .map(|_| (
            get_a_string(&mut rand, k_range),
            get_a_string(&mut rand, v_range),
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

fn get_random_index_vec(range: u64, length: u64) -> Vec<usize> {
    let mut rand = ChaChaRng::seed_from_u64(SEED);
    let res: Vec<usize> = (0..length).map(|_| (rand.next_u64() % range) as usize).collect();
    return res;
}

fn kvs(c: &mut Criterion) {
    c.bench_function("kvs_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut kvs = KvStore::open(temp_dir.path()).unwrap();

        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_RANGE, VALUE_RANGE, KV_LEN);

	    b.iter(|| {
            for (k, v) in &kv_vec {
                kvs.set(k.clone(), v.clone()).unwrap();
            }
		});
	});

    c.bench_function("kvs_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut kvs = KvStore::open(temp_dir.path()).unwrap();

        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_RANGE, VALUE_RANGE, KV_LEN);
        let idx_vec: Vec<usize> = get_random_index_vec(KV_LEN, IDX_LEN);

        for (k, v) in &kv_vec {
            kvs.set(k.clone(), v.clone()).unwrap();
        }

	    b.iter(|| {
            for i in &idx_vec {
                let v = kvs.get(kv_vec[*i].0.clone()).unwrap();
                assert_eq!(v, Some(kv_vec[*i].1.clone()));
            }
		});
	});
}

fn sled(c: &mut Criterion) {
    c.bench_function("sled_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut kvs = SledKvsEngine::open(temp_dir.path().to_path_buf()).unwrap();

        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_RANGE, VALUE_RANGE, KV_LEN);

	    b.iter(|| {
            for (k, v) in &kv_vec {
                kvs.set(k.clone(), v.clone()).unwrap();
            }
		});
	});

    c.bench_function("sled_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut kvs = KvStore::open(temp_dir.path()).unwrap();

        let kv_vec: Vec<(String, String)> = get_random_kv_vec(KEY_RANGE, VALUE_RANGE, KV_LEN);
        let idx_vec: Vec<usize> = get_random_index_vec(KV_LEN, IDX_LEN);

        for (k, v) in &kv_vec {
            kvs.set(k.clone(), v.clone()).unwrap();
        }

	    b.iter(|| {
            for i in &idx_vec {
                let v = kvs.get(kv_vec[*i].0.clone()).unwrap();
                assert_eq!(v, Some(kv_vec[*i].1.clone()));
            }
		});
	});
}

criterion_group!(benches, kvs, sled);
criterion_main!(benches);