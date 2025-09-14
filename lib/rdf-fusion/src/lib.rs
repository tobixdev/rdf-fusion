#![doc = include_str!("../README.md")]
#![doc(test(attr(deny(warnings))))]
#![doc(test(attr(allow(deprecated))))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/oxigraph/oxigraph/main/logo.svg"
)]

pub mod error;
pub mod io;
pub mod store;

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
