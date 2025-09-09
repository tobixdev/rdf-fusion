#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use crate::cli::{Args, Command};
use anyhow::{Context, bail};
use clap::Parser;
use rdf_fusion::io::{RdfFormat, RdfParser, RdfSerializer};
use rdf_fusion::model::{GraphName, NamedNode};
use rdf_fusion::store::Store;
use rdf_fusion_web::ServerConfig;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufWriter, Read, Write, stdin, stdout};
use std::path::Path;
use std::str;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod cli;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let matches = Args::parse();
    match matches.command {
        Command::Serve {
            bind,
            cors,
            union_default_graph,
        } => serve(Store::default(), &bind, false, cors, union_default_graph).await,
        Command::Convert {
            from_file,
            from_format,
            from_base,
            to_file,
            to_format,
            to_base,
            lenient,
            from_graph,
            from_default_graph,
            to_graph,
        } => {
            let from_format = if let Some(format) = from_format {
                rdf_format_from_name(&format)?
            } else if let Some(file) = &from_file {
                rdf_format_from_path(file)?
            } else {
                bail!("The --from-format option must be set when reading from stdin")
            };
            let mut parser = RdfParser::from_format(from_format);
            if let Some(base) = from_base {
                parser = parser
                    .with_base_iri(&base)
                    .with_context(|| format!("Invalid base IRI {base}"))?;
            }

            let to_format = if let Some(format) = to_format {
                rdf_format_from_name(&format)?
            } else if let Some(file) = &to_file {
                rdf_format_from_path(file)?
            } else {
                bail!("The --to-format option must be set when writing to stdout")
            };
            let serializer = RdfSerializer::from_format(to_format);

            let from_graph = if let Some(from_graph) = from_graph {
                Some(
                    NamedNode::new(&from_graph)
                        .with_context(|| {
                            format!("The source graph name {from_graph} is invalid")
                        })?
                        .into(),
                )
            } else if from_default_graph {
                Some(GraphName::DefaultGraph)
            } else {
                None
            };
            let to_graph = if let Some(to_graph) = to_graph {
                NamedNode::new(&to_graph)
                    .with_context(|| {
                        format!("The target graph name {to_graph} is invalid")
                    })?
                    .into()
            } else {
                GraphName::DefaultGraph
            };

            match (from_file, to_file) {
                (Some(from_file), Some(to_file)) => close_file_writer(do_convert(
                    parser,
                    File::open(from_file)?,
                    serializer,
                    BufWriter::new(File::create(to_file)?),
                    lenient,
                    &from_graph,
                    &to_graph,
                    to_base.as_deref(),
                )?),
                (Some(from_file), None) => do_convert(
                    parser,
                    File::open(from_file)?,
                    serializer,
                    stdout().lock(),
                    lenient,
                    &from_graph,
                    &to_graph,
                    to_base.as_deref(),
                )?
                .flush(),
                (None, Some(to_file)) => close_file_writer(do_convert(
                    parser,
                    stdin().lock(),
                    serializer,
                    BufWriter::new(File::create(to_file)?),
                    lenient,
                    &from_graph,
                    &to_graph,
                    to_base.as_deref(),
                )?),
                (None, None) => do_convert(
                    parser,
                    stdin().lock(),
                    serializer,
                    stdout().lock(),
                    lenient,
                    &from_graph,
                    &to_graph,
                    to_base.as_deref(),
                )?
                .flush(),
            }?;
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn do_convert<R: Read, W: Write>(
    parser: RdfParser,
    reader: R,
    mut serializer: RdfSerializer,
    writer: W,
    lenient: bool,
    from_graph: &Option<GraphName>,
    default_graph: &GraphName,
    to_base: Option<&str>,
) -> anyhow::Result<W> {
    let mut parser = parser.for_reader(reader);
    let first = parser.next(); // We read the first element to get prefixes and the base IRI
    if let Some(base_iri) = to_base.or_else(|| parser.base_iri()) {
        serializer = serializer
            .with_base_iri(base_iri)
            .with_context(|| format!("Invalid base IRI: {base_iri}"))?;
    }
    for (prefix_name, prefix_iri) in parser.prefixes() {
        serializer = serializer
            .with_prefix(prefix_name, prefix_iri)
            .with_context(|| {
                format!("Invalid IRI for prefix {prefix_name}: {prefix_iri}")
            })?;
    }
    let mut serializer = serializer.for_writer(writer);
    for quad_result in first.into_iter().chain(parser) {
        match quad_result {
            Ok(mut quad) => {
                if let Some(from_graph) = from_graph {
                    if quad.graph_name == *from_graph {
                        quad.graph_name = GraphName::DefaultGraph;
                    } else {
                        continue;
                    }
                }
                if quad.graph_name.is_default_graph() {
                    quad.graph_name = default_graph.clone();
                }
                serializer.serialize_quad(&quad)?;
            }
            Err(e) => {
                if lenient {
                    eprintln!("Parsing error: {e}");
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(serializer.finish()?)
}

fn format_from_path<T>(
    path: &Path,
    from_extension: impl FnOnce(&str) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    if let Some(ext) = path.extension().and_then(OsStr::to_str) {
        from_extension(ext).map_err(|e| {
            e.context(format!(
                "Not able to guess the file format from file name extension '{ext}'"
            ))
        })
    } else {
        bail!(
            "The path {} has no extension to guess a file format from",
            path.display()
        )
    }
}

fn rdf_format_from_path(path: &Path) -> anyhow::Result<RdfFormat> {
    format_from_path(path, |ext| {
        RdfFormat::from_extension(ext)
            .with_context(|| format!("The file extension '{ext}' is unknown"))
    })
}

fn rdf_format_from_name(name: &str) -> anyhow::Result<RdfFormat> {
    if let Some(t) = RdfFormat::from_extension(name) {
        return Ok(t);
    }
    if let Some(t) = RdfFormat::from_media_type(name) {
        return Ok(t);
    }
    bail!("The file format '{name}' is unknown")
}

async fn serve(
    store: Store,
    bind: &str,
    read_only: bool,
    cors: bool,
    union_default_graph: bool,
) -> anyhow::Result<()> {
    let server_config = ServerConfig {
        store,
        bind: bind.to_owned(),
        read_only,
        cors,
        union_default_graph,
    };
    rdf_fusion_web::serve(server_config).await
}

fn close_file_writer(writer: BufWriter<File>) -> io::Result<()> {
    let mut file = writer
        .into_inner()
        .map_err(io::IntoInnerError::into_error)?;
    file.flush()?;
    file.sync_all()
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use anyhow::Result;
    use assert_cmd::Command;
    use assert_fs::NamedTempFile;
    use assert_fs::prelude::*;
    use predicates::prelude::*;

    fn cli_command() -> Command {
        let mut command = Command::new(env!("CARGO"));
        command
            .arg("run")
            .arg("--bin")
            .arg("rdf-fusion")
            .arg("--no-default-features");
        command.arg("--");
        command
    }

    #[test]
    fn cli_help() {
        cli_command()
            .assert()
            .failure()
            .stdout("")
            .stderr(predicate::str::contains("RDF Fusion"));
    }

    #[test]
    fn cli_convert_file() -> Result<()> {
        let input_file = NamedTempFile::new("input.ttl")?;
        input_file.write_str("@prefix schema: <http://schema.org/> .\n<#me> a schema:Person ;\n\tschema:name \"Foo Bar\"@en .\n")?;
        let output_file = NamedTempFile::new("output.rdf")?;
        cli_command()
            .arg("convert")
            .arg("--from-file")
            .arg(input_file.path())
            .arg("--from-base")
            .arg("http://example.com/")
            .arg("--to-file")
            .arg(output_file.path())
            .assert()
            .success();
        output_file
            .assert("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rdf:RDF xml:base=\"http://example.com/\" xmlns:schema=\"http://schema.org/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n\t<schema:Person rdf:about=\"#me\">\n\t\t<schema:name xml:lang=\"en\">Foo Bar</schema:name>\n\t</schema:Person>\n</rdf:RDF>");
        Ok(())
    }

    #[test]
    fn cli_convert_from_default_graph_to_named_graph() {
        cli_command()
            .arg("convert")
            .arg("--from-format")
            .arg("trig")
            .arg("--to-format")
            .arg("nq")
            .arg("--from-default-graph")
            .arg("--to-graph")
            .arg("http://example.com/t")
            .write_stdin("@base <http://example.com/> . <s> <p> <o> . <g> { <sg> <pg> <og> . }")
            .assert()
            .stdout("<http://example.com/s> <http://example.com/p> <http://example.com/o> <http://example.com/t> .\n")
            .success();
    }

    #[test]
    fn cli_convert_from_named_graph() {
        cli_command()
            .arg("convert")
            .arg("--from-format")
            .arg("trig")
            .arg("--to-format")
            .arg("nq")
            .arg("--from-graph")
            .arg("http://example.com/g")
            .write_stdin("@base <http://example.com/> . <s> <p> <o> . <g> { <sg> <pg> <og> . }")
            .assert()
            .stdout("<http://example.com/sg> <http://example.com/pg> <http://example.com/og> .\n");
    }

    #[test]
    fn cli_convert_to_base() {
        cli_command()
            .arg("convert")
            .arg("--from-format")
            .arg("ttl")
            .arg("--to-format")
            .arg("ttl")
            .arg("--to-base")
            .arg("http://example.com")
            .write_stdin("@base <http://example.com/> . <s> <p> <o> .")
            .assert()
            .stdout("@base <http://example.com> .\n</s> </p> </o> .\n");
    }

    #[test]
    fn clap_debug() {
        use clap::CommandFactory;

        Args::command().debug_assert()
    }
}
