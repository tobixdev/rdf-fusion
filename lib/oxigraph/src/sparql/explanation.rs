use std::fs::File;
use std::io;
use std::io::BufWriter;

#[derive(Debug)]
pub struct QueryExplanation {}

impl QueryExplanation {
    pub fn write_in_json(&self, p0: &mut BufWriter<File>) -> io::Result<()> {
        todo!()
    }
}
