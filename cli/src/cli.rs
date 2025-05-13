use clap::{Parser, Subcommand, ValueHint};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about, version, name = "rdf-fusion")]
/// RdfFusion command line toolkit and SPARQL HTTP server
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start RdfFusion HTTP server in read-write mode
    Serve {
        /// Host and port to listen to
        #[arg(short, long, default_value = "localhost:7878", value_hint = ValueHint::Hostname)]
        bind: String,
        /// Allows cross-origin requests
        #[arg(long)]
        cors: bool,
        /// If the SPARQL queries should look for triples in all the dataset graphs by default (ie. without `GRAPH` operations)
        ///
        /// This is equivalent as setting the union-default-graph option in all SPARQL queries
        #[arg(long)]
        union_default_graph: bool,
    },
    /// Convert a RDF serialization from one format to an other
    Convert {
        /// File to convert from
        ///
        /// If no file is given, stdin is read.
        #[arg(short, long, value_hint = ValueHint::FilePath)]
        from_file: Option<PathBuf>,
        /// The format of the file(s) to convert from
        ///
        /// It can be an extension like "nt" or a MIME type like "application/n-triples".
        ///
        /// By default the format is guessed from the input file extension.
        #[arg(long, required_unless_present = "from_file")]
        from_format: Option<String>,
        /// Base IRI of the file to read
        #[arg(long, value_hint = ValueHint::Url)]
        from_base: Option<String>,
        /// File to convert to
        ///
        /// If no file is given, stdout is written.
        #[arg(short, long, value_hint = ValueHint::FilePath)]
        to_file: Option<PathBuf>,
        /// The format of the file(s) to convert to
        ///
        /// It can be an extension like "nt" or a MIME type like "application/n-triples".
        ///
        /// By default the format is guessed from the target file extension.
        #[arg(long, required_unless_present = "to_file")]
        to_format: Option<String>,
        /// Base IRI of the file to write
        #[arg(long, value_hint = ValueHint::Url)]
        to_base: Option<String>,
        /// Attempt to keep converting even if the data file is invalid
        #[arg(long)]
        lenient: bool,
        /// Only load the given named graph from the input file
        ///
        /// By default all graphs are loaded.
        #[arg(long, conflicts_with = "from_default_graph", value_hint = ValueHint::Url)]
        from_graph: Option<String>,
        /// Only load the default graph from the input file
        #[arg(long, conflicts_with = "from_graph")]
        from_default_graph: bool,
        /// Name of the graph to map the default graph to
        ///
        /// By default the default graph is used.
        #[arg(long, value_hint = ValueHint::Url)]
        to_graph: Option<String>,
    },
}
