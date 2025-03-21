use crate::AResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::common::ScalarValue;
use datamodel::{Boolean, Decimal, Double, Float, Int, Integer, LanguageStringRef, Numeric, OwnedStringLiteral, RdfOpResult, SimpleLiteralRef, StringLiteralRef, TermRef, TypedLiteralRef};
use oxrdf::{BlankNodeRef, NamedNodeRef};

pub trait WriteEncTerm {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized;

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized;
}

impl WriteEncTerm for Boolean {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for Float {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for Decimal {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for Double {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for Int {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for Integer {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for Numeric {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for SimpleLiteralRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for StringLiteralRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for OwnedStringLiteral {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for LanguageStringRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for BlankNodeRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for NamedNodeRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for TermRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for TypedLiteralRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}