//! Implements Bustle traits for comparing performance against other kv stores.
#[cfg(feature = "bench_rocksdb")]
use ::rocksdb::DB;
use blob_store::fixtures::Payload;
use blob_store::BlobStore;
use bustle::{Mix, Workload};
use fixture::ArcStorage;

mod fixture;
mod payload_storage;
#[cfg(feature = "bench_rocksdb")]
mod rocksdb;

type PayloadStorage = BlobStore<Payload>;

fn default_opts(workload: &mut Workload) -> &mut Workload {
    let seed = [42; 32];
    workload.initial_capacity_log2(21).seed(seed)
}

fn main() {
    for num_threads in [1, 2] {
        println!("------------ {num_threads} thread(s) -------------");
        // Read heavy
        println!("**read_heavy** with prefill_fraction 0.95");
        let mut workload = Workload::new(num_threads, Mix::read_heavy());
        default_opts(&mut workload).prefill_fraction(0.95);
        println!("ValueStorage:");
        workload.run::<ArcStorage<PayloadStorage>>();

        #[cfg(feature = "bench_rocksdb")]
        {
            println!("RocksDB:");
            workload.run::<ArcStorage<DB>>();
        }
        println!(" ");

        // Insert heavy
        println!("**insert_heavy** with prefill_fraction 0.0");
        let mut workload = Workload::new(num_threads, Mix::insert_heavy());
        default_opts(&mut workload).prefill_fraction(0.0);

        println!("ValueStorage:");
        workload.run::<ArcStorage<PayloadStorage>>();

        #[cfg(feature = "bench_rocksdb")]
        {
            println!("RocksDB:");
            workload.run::<ArcStorage<DB>>();
        }
        println!(" ");

        // Update heavy
        println!("**update_heavy** with prefill_fraction 0.5");
        let mut workload = Workload::new(num_threads, Mix::update_heavy());
        default_opts(&mut workload).prefill_fraction(0.5);

        println!("ValueStorage:");
        workload.run::<ArcStorage<PayloadStorage>>();

        #[cfg(feature = "bench_rocksdb")]
        {
            println!("RocksDB:");
            workload.run::<ArcStorage<DB>>();
        }
        println!(" ");
    }
}
