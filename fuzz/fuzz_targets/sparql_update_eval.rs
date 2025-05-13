#![no_main]

use rdf_fusion::model::dataset::{CanonicalizationAlgorithm, Dataset};
use rdf_fusion::sparql::{QueryOptions, Update};
use rdf_fusion::store::Store;
use libfuzzer_sys::fuzz_target;
#[cfg(feature = "storage")]
use std::env::temp_dir;
use std::sync::OnceLock;

fuzz_target!(|data: sparql_smith::Update| {
    static DISK_STORE: OnceLock<Store> = OnceLock::new();
    let disk_store = DISK_STORE.get_or_init(|| {
        #[cfg(feature = "storage")]
        {
            Store::open(temp_dir().join("rdf-fusion-fuzz-update")).unwrap()
        }
        #[cfg(not(feature = "storage"))]
        {
            Store::new().unwrap()
        }
    });

    let update_str = data.to_string();
    if let Ok(update) = Update::parse(&update_str, None) {
        let options = QueryOptions::default();

        disk_store.clear().unwrap();
        let disk_with_opt = disk_store.update_opt(update.clone(), options.clone());
        disk_store.validate().unwrap();
        let mut dataset_disk_with_opt = disk_store.iter().collect::<Result<Dataset, _>>().unwrap();
        dataset_disk_with_opt.canonicalize(CanonicalizationAlgorithm::Unstable);

        let memory_store = Store::new().unwrap();
        let memory_without_opt = memory_store.update_opt(update, options.without_optimizations());
        memory_store.validate().unwrap();
        let mut dataset_memory_without_opt =
            memory_store.iter().collect::<Result<Dataset, _>>().unwrap();
        dataset_memory_without_opt.canonicalize(CanonicalizationAlgorithm::Unstable);

        assert_eq!(
            disk_with_opt.is_ok(),
            memory_without_opt.is_ok(),
            "Worked and failed depending on using optimizations: {disk_with_opt:?} {memory_without_opt:?}"
        );
        assert_eq!(
            dataset_disk_with_opt, dataset_memory_without_opt,
            "With optimizations on disk:\n{dataset_disk_with_opt}\nWithout optimizations in memory:\n{dataset_memory_without_opt}"
        );
    }
});
