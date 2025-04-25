use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::Write;

#[derive(Debug)]
pub struct QueryExplanation;

impl QueryExplanation {
    #[allow(clippy::unused_self)]
    pub fn write_in_json(&self, writer: &mut BufWriter<File>) -> io::Result<()> {
        write!(writer, "QueryExplanation")
    }
}
