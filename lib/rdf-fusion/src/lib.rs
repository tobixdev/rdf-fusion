#![doc = include_str!("../README.md")]
#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/logo.png"
)]

pub mod error;
pub mod io;
pub mod store;

pub mod api {
    pub use rdf_fusion_extensions::*;
}

pub mod encoding {
    pub use rdf_fusion_encoding::*;
}

pub mod functions {
    pub use rdf_fusion_functions::*;
}

pub mod model {
    pub use rdf_fusion_model::*;
}

pub mod logical {
    pub use rdf_fusion_logical::*;
}

pub mod execution {
    pub use rdf_fusion_execution::*;
}

pub mod storage {
    pub use rdf_fusion_storage::*;
}
