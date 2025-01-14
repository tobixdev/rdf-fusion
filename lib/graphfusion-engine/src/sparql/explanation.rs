use std::fs::File;
use std::io;
use std::io::BufWriter;

#[derive(Debug)]
pub struct QueryExplanation {}

impl QueryExplanation {
    pub fn write_in_json(&self, _writer: &mut BufWriter<File>) -> io::Result<()> {
        todo!()
    }
}
