use crate::decoded::model::{DecTerm, DecTermField};
use crate::AResult;
use datafusion::arrow::array::{ArrayBuilder, ArrayRef, NullBuilder, StringBuilder, StructBuilder, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::Term;
use std::sync::Arc;

pub struct DecRdfTermBuilder {
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    named_node_builder: StringBuilder,
    blank_node_builder: StringBuilder,
    string_builder: StructBuilder,
    typed_literal_builder: StructBuilder,
    null_builder: NullBuilder,
}

impl DecRdfTermBuilder {
    pub fn new() -> Self {
        Self {
            type_ids: Vec::new(),
            offsets: Vec::new(),
            named_node_builder: StringBuilder::new(),
            blank_node_builder: StringBuilder::new(),
            string_builder: StructBuilder::from_fields(DecTerm::string_fields(), 0),
            typed_literal_builder: StructBuilder::from_fields(DecTerm::typed_literal_fields(), 0),
            null_builder: NullBuilder::new()
        }
    }

    pub fn append_term(&mut self, value: &Term) -> AResult<()> {
        Ok(match value {
            Term::NamedNode(nn) => self.append_named_node(nn.as_str())?,
            Term::BlankNode(bnode) => self.append_blank_node(bnode.as_str())?,
            Term::Literal(literal) => match literal.datatype() {
                rdf::LANG_STRING => self.append_string(literal.value(), Some(literal.value()))?,
                xsd::STRING => self.append_string(literal.value(), None)?,
                _ => self.append_typed_literal(literal.value(), literal.datatype().as_str())?,
            },
            _ => unimplemented!(),
        })
    }

    pub fn append_named_node(&mut self, value: &str) -> AResult<()> {
        self.type_ids.push(DecTermField::NamedNode.type_id());
        self.offsets.push(self.named_node_builder.len() as i32);
        self.named_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_blank_node(&mut self, value: &str) -> AResult<()> {
        self.type_ids.push(DecTermField::BlankNode.type_id());
        self.offsets.push(self.blank_node_builder.len() as i32);
        self.blank_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) -> AResult<()> {
        self.type_ids.push(DecTermField::String.type_id());
        self.offsets.push(self.string_builder.len() as i32);

        self.string_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);
        let language_builder = self
            .string_builder
            .field_builder::<StringBuilder>(1)
            .unwrap();
        if let Some(language) = language {
            language_builder.append_value(language);
        } else {
            language_builder.append_null();
        }
        self.string_builder.append(true);

        Ok(())
    }

    pub fn append_typed_literal(&mut self, value: &str, type_id: &str) -> AResult<()> {
        self.type_ids.push(DecTermField::TypedLiteral.type_id());
        self.offsets.push(self.typed_literal_builder.len() as i32);

        self.typed_literal_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(type_id);
        self.typed_literal_builder.append(true);
        Ok(())
    }

    pub fn append_null(&mut self) -> AResult<()> {
        self.type_ids.push(DecTermField::Null.type_id());
        self.offsets.push(self.null_builder.len() as i32);
        self.null_builder.append_null();
        Ok(())
    }

    pub fn finish(mut self) -> AResult<ArrayRef> {
        Ok(Arc::new(UnionArray::try_new(
            DecTerm::term_type_fields(),
            ScalarBuffer::from(self.type_ids),
            Some(ScalarBuffer::from(self.offsets)),
            vec![
                Arc::new(self.named_node_builder.finish()),
                Arc::new(self.blank_node_builder.finish()),
                Arc::new(self.string_builder.finish()),
                Arc::new(self.typed_literal_builder.finish()),
            ],
        )?))
    }
}
