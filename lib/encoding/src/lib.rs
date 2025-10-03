#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]

//! Contains the [RDF Funsion's](https://docs.rs/rdf-fusion/) term encodings.
//!
//! # Overview
//!
//! RDF term encodings allow us to bridge the gap between the [Resource Description Framework](https://www.w3.org/TR/rdf11-concepts/)
//! and the Arrow type system. Because there is no single best way to represent RDF terms in Arrow,
//! RDF Fusion supports multiple encodings. The [documentation of the main crate](https://docs.rs/rdf-fusion/latest/rdf_fusion/#sparql-on-top-of-datafusion)
//! provides some further details on this aspect.
//!
//! The following table provides an overview of the supported encodings.
//!
//! |                                                             | **Suitable For**           | **Requirements**  | **Term Identity** | **Comment**                   |
//! |-------------------------------------------------------------|----------------------------|-------------------|-------------------|-------------------------------|
//! | [**Plain Term Encoding**](plain_term::PlainTermEncoding)    | Processing literal terms   | -                 | Yes               | Result visible to users       |
//! | [**Object ID Encoding**](object_id::ObjectIdEncoding)       | Joining solution sets      | Object ID Mapping | Yes               | Must be decoded at some point |
//! | [**Typed Value Encoding**](typed_value::TypedValueEncoding) | Arithmetic and comparisons | -                 | No                |                               |
//!
//! # Encoding Trait
//!
//! All of the above encodings must implement the [TermEncoding] trait. As a result, each encoding
//! must provide an [EncodingArray] and an [EncodingScalar]. These two types wrap regular Arrow
//! arrays (or scalars) that adhere to a particular encoding. If you want to pass an array to
//! a function that is guaranteed to be of a certain encoding, use these data types.
//!
//! # Future Plans
//!
//! In the future, we would like that encodings are parameterizable. For example, this [GitHub issue](https://github.com/tobixdev/rdf-fusion/issues/50)
//! tracks the progress of allowing users to specify custom object id lengths. As these parameters
//! will influence what kind of arrays/scalars are valid instances of a given encoding. For example,
//! if the object id contains 4 bytes, an array with 6 bytes is not a valid values. This state needs
//! to be considered when validating arrays/scalars. Therefore, you should use [TermEncoding::try_new_array]
//! or [TermEncoding::try_new_scalar] for creating datum instances, as the static way of creating
//! them will no longer work at some point.

mod encoding;
mod encoding_name;
mod encodings;
pub mod object_id;
pub mod plain_term;
mod quad_storage_encoding;
mod scalar_encoder;
pub mod sortable_term;
pub mod typed_value;

pub use encoding::*;
pub use encoding_name::*;
pub use encodings::*;
pub use quad_storage_encoding::*;
pub use scalar_encoder::ScalarEncoder;
