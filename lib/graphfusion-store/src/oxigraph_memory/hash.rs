use crate::oxigraph_memory::encoded_term::EncodedTerm;
use siphasher::sip128::{Hasher128, SipHasher24};
use std::hash::{Hash, Hasher};

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub struct StrHash {
    hash: [u8; 16],
}

impl StrHash {
    pub fn new(value: &str) -> Self {
        let mut hasher = SipHasher24::new();
        hasher.write(value.as_bytes());
        Self {
            hash: u128::from(hasher.finish128()).to_be_bytes(),
        }
    }

    #[inline]
    pub fn from_be_bytes(hash: [u8; 16]) -> Self {
        Self { hash }
    }

    #[inline]
    pub fn to_be_bytes(self) -> [u8; 16] {
        self.hash
    }
}

impl Hash for StrHash {
    #[inline]
    #[allow(clippy::host_endian_bytes)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u128(u128::from_ne_bytes(self.hash))
    }
}

#[derive(Default)]
pub struct StrHashHasher {
    value: u64,
}

impl Hasher for StrHashHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("Must only be used on StrHash")
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn write_u128(&mut self, i: u128) {
        self.value = i as u64;
    }
}

impl Hash for EncodedTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::NamedNode { iri_id } => iri_id.hash(state),
            Self::NumericalBlankNode { id } => id.hash(state),
            Self::SmallBlankNode(id) => id.hash(state),
            Self::BigBlankNode { id_id } => id_id.hash(state),
            Self::DefaultGraph => (),
            Self::SmallStringLiteral(value) => value.hash(state),
            Self::BigStringLiteral { value_id } => value_id.hash(state),
            Self::SmallSmallLangStringLiteral { value, language } => {
                value.hash(state);
                language.hash(state);
            }
            Self::SmallBigLangStringLiteral { value, language_id } => {
                value.hash(state);
                language_id.hash(state);
            }
            Self::BigSmallLangStringLiteral { value_id, language } => {
                value_id.hash(state);
                language.hash(state);
            }
            Self::BigBigLangStringLiteral {
                value_id,
                language_id,
            } => {
                value_id.hash(state);
                language_id.hash(state);
            }
            Self::SmallTypedLiteral { value, datatype_id } => {
                value.hash(state);
                datatype_id.hash(state);
            }
            Self::BigTypedLiteral {
                value_id,
                datatype_id,
            } => {
                value_id.hash(state);
                datatype_id.hash(state);
            }
            Self::BooleanLiteral(value) => value.hash(state),
            Self::FloatLiteral(value) => value.to_be_bytes().hash(state),
            Self::DoubleLiteral(value) => value.to_be_bytes().hash(state),
            Self::IntegerLiteral(value) => value.hash(state),
            Self::DecimalLiteral(value) => value.hash(state),
            Self::DateTimeLiteral(value) => value.hash(state),
            Self::TimeLiteral(value) => value.hash(state),
            Self::DateLiteral(value) => value.hash(state),
            Self::GYearMonthLiteral(value) => value.hash(state),
            Self::GYearLiteral(value) => value.hash(state),
            Self::GMonthDayLiteral(value) => value.hash(state),
            Self::GDayLiteral(value) => value.hash(state),
            Self::GMonthLiteral(value) => value.hash(state),
            Self::DurationLiteral(value) => value.hash(state),
            Self::YearMonthDurationLiteral(value) => value.hash(state),
            Self::DayTimeDurationLiteral(value) => value.hash(state),
            Self::Triple(value) => value.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_hash_stability() {
        const EMPTY_HASH: [u8; 16] = [
            244, 242, 206, 212, 71, 171, 2, 66, 125, 224, 163, 128, 71, 215, 73, 80,
        ];

        const FOO_HASH: [u8; 16] = [
            177, 216, 59, 176, 7, 47, 87, 243, 76, 253, 150, 32, 126, 153, 216, 19,
        ];

        assert_eq!(StrHash::new("").to_be_bytes(), EMPTY_HASH);
        assert_eq!(StrHash::from_be_bytes(EMPTY_HASH).to_be_bytes(), EMPTY_HASH);

        assert_eq!(StrHash::new("foo").to_be_bytes(), FOO_HASH);
        assert_eq!(StrHash::from_be_bytes(FOO_HASH).to_be_bytes(), FOO_HASH);
    }
}
