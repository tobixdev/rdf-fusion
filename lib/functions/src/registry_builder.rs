use crate::aggregates::{
    AvgUdafFactory, GroupConcatUdafFactory, MaxUdafFactory, MinUdafFactory, SumUdafFactory,
};
use crate::builtin::native::BooleanAsRdfTermTypedValueFactory;
use crate::builtin::native::EffectiveBooleanValueTypedValueFactory;
use crate::builtin::query::IsCompatibleUdfFactory;
use crate::builtin::{BuiltinName, GraphFusionUdfFactory};
use crate::factory::GraphFusionUdafFactory;
use crate::registry::GraphFusionFunctionRegistry;
use crate::scalar::{
    AbsTypedValueFactory, AddTypedValueFactory, AsBooleanTypedValueFactory,
    AsDateTimeTypedValueFactory, AsDecimalTypedValueFactory, AsDoubleTypedValueFactory,
    AsFloatTypedValueFactory, AsIntTypedValueFactory, AsIntegerTypedValueFactory,
    AsStringTypedValueFactory, BNodeTypedValueFactory, BoundTypedValueFactory,
    CeilTypedValueFactory, CoalesceTypedValueFactory, ConcatTypedValueFactory,
    ContainsTypedValueFactory, DatatypeTypedValueFactory, DayTypedValueFactory,
    DivTypedValueFactory, EncodeForUriTypedValueFactory, EqTypedValueFactory,
    FloorTypedValueFactory, GreaterOrEqualTypedValueFactory, GreaterThanTypedValueFactory,
    HoursTypedValueFactory, IfTypedValueFactory, IriTypedValueFactory, IsBlankTypedValueFactory,
    IsIriTypedValueFactory, IsLiteralTypedValueFactory, IsNumericTypedValueFactory,
    LCaseTypedValueFactory, LangMatchesTypedValueFactory, LangTypedValueFactory,
    LessOrEqualTypedValueFactory, LessThanTypedValueFactory, Md5TypedValueFactory,
    MinutesTypedValueFactory, MonthTypedValueFactory, MulTypedValueFactory, RandTypedValueFactory,
    RegexTypedValueFactory, ReplaceTypedValueFactory, RoundTypedValueFactory,
    SameTermTypedValueFactory, SecondsTypedValueFactory, Sha1TypedValueFactory,
    Sha256TypedValueFactory, Sha384TypedValueFactory, Sha512TypedValueFactory,
    StrAfterTypedValueFactory, StrBeforeTypedValueFactory, StrDtTypedValueFactory,
    StrEndsTypedValueFactory, StrLangTypedValueFactory, StrLenTypedValueFactory,
    StrStartsTypedValueFactory, StrTypedValueFactory, StrUuidTypedValueFactory,
    SubStrTypedValueFactory, SubTypedValueFactory, TimezoneTypedValueFactory,
    UCaseTypedValueFactory, UnaryMinusTypedValueFactory, UnaryPlusTypedValueFactory,
    UuidTypedValueFactory, YearTypedValueFactory,
};
use crate::{DFResult, FunctionName};
use std::collections::HashMap;
use std::sync::Arc;

/// TODO
#[derive(Debug)]
pub struct GraphFusionFunctionRegistryBuilder {
    /// TODO
    scalars: HashMap<FunctionName, Arc<dyn GraphFusionUdfFactory>>,
    /// TODO
    aggregates: HashMap<FunctionName, Arc<dyn GraphFusionUdafFactory>>,
}

impl GraphFusionFunctionRegistryBuilder {
    /// TODO
    pub fn build(self) -> DFResult<GraphFusionFunctionRegistry> {
        GraphFusionFunctionRegistry::try_new(self.scalars, self.aggregates)
    }
}

impl Default for GraphFusionFunctionRegistryBuilder {
    fn default() -> Self {
        let mut scalars: HashMap<FunctionName, Arc<dyn GraphFusionUdfFactory>> = HashMap::new();

        // SPARQL Builtin Scalar Functions
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Str),
            Arc::new(StrTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Lang),
            Arc::new(LangTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::LangMatches),
            Arc::new(LangMatchesTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Datatype),
            Arc::new(DatatypeTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Iri),
            Arc::new(IriTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::BNode),
            Arc::new(BNodeTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Rand),
            Arc::new(RandTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Abs),
            Arc::new(AbsTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Ceil),
            Arc::new(CeilTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Floor),
            Arc::new(FloorTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Round),
            Arc::new(RoundTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Round),
            Arc::new(ConcatTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Concat),
            Arc::new(ConcatTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::SubStr),
            Arc::new(SubStrTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrLen),
            Arc::new(StrLenTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Replace),
            Arc::new(ReplaceTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::UCase),
            Arc::new(UCaseTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::LCase),
            Arc::new(LCaseTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::EncodeForUri),
            Arc::new(EncodeForUriTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Contains),
            Arc::new(ContainsTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrStarts),
            Arc::new(StrStartsTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrEnds),
            Arc::new(StrEndsTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrBefore),
            Arc::new(StrBeforeTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrAfter),
            Arc::new(StrAfterTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Year),
            Arc::new(YearTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Month),
            Arc::new(MonthTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Day),
            Arc::new(DayTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Hours),
            Arc::new(HoursTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Minutes),
            Arc::new(MinutesTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Seconds),
            Arc::new(SecondsTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Timezone),
            Arc::new(TimezoneTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Tz),
            Arc::new(TimezoneTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Uuid),
            Arc::new(UuidTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrUuid),
            Arc::new(StrUuidTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Md5),
            Arc::new(Md5TypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Sha1),
            Arc::new(Sha1TypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Sha256),
            Arc::new(Sha256TypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Sha384),
            Arc::new(Sha384TypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Sha512),
            Arc::new(Sha512TypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrLang),
            Arc::new(StrLangTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::StrDt),
            Arc::new(StrDtTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::IsIri),
            Arc::new(IsIriTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::IsBlank),
            Arc::new(IsBlankTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::IsLiteral),
            Arc::new(IsLiteralTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::IsNumeric),
            Arc::new(IsNumericTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Regex),
            Arc::new(RegexTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Bound),
            Arc::new(BoundTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Coalesce),
            Arc::new(CoalesceTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::If),
            Arc::new(IfTypedValueFactory {}),
        );

        // Comparison Functions
        scalars.insert(
            FunctionName::Builtin(BuiltinName::SameTerm),
            Arc::new(SameTermTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Equal),
            Arc::new(EqTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::GreaterThan),
            Arc::new(GreaterThanTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::GreaterOrEqual),
            Arc::new(GreaterOrEqualTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::LessThan),
            Arc::new(LessThanTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::LessOrEqual),
            Arc::new(LessOrEqualTypedValueFactory {}),
        );

        // Numeric functions
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Add),
            Arc::new(AddTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Div),
            Arc::new(DivTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Mul),
            Arc::new(MulTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::Sub),
            Arc::new(SubTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::UnaryMinus),
            Arc::new(UnaryMinusTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::UnaryPlus),
            Arc::new(UnaryPlusTypedValueFactory {}),
        );

        // Conversion Functions
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsString),
            Arc::new(AsStringTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsInteger),
            Arc::new(AsIntegerTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsInt),
            Arc::new(AsIntTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsFloat),
            Arc::new(AsFloatTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsDouble),
            Arc::new(AsDoubleTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsDecimal),
            Arc::new(AsDecimalTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsDateTime),
            Arc::new(AsDateTimeTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::AsBoolean),
            Arc::new(AsBooleanTypedValueFactory {}),
        );

        // Other Necessary Functions
        scalars.insert(
            FunctionName::Builtin(BuiltinName::EffectiveBooleanValue),
            Arc::new(EffectiveBooleanValueTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::NativeBooleanAsTerm),
            Arc::new(BooleanAsRdfTermTypedValueFactory {}),
        );
        scalars.insert(
            FunctionName::Builtin(BuiltinName::IsCompatible),
            Arc::new(IsCompatibleUdfFactory {}),
        );

        let mut aggregates: HashMap<FunctionName, Arc<dyn GraphFusionUdafFactory>> = HashMap::new();
        aggregates.insert(
            FunctionName::Builtin(BuiltinName::Sum),
            Arc::new(SumUdafFactory {}),
        );
        aggregates.insert(
            FunctionName::Builtin(BuiltinName::Min),
            Arc::new(MinUdafFactory {}),
        );
        aggregates.insert(
            FunctionName::Builtin(BuiltinName::Max),
            Arc::new(MaxUdafFactory {}),
        );
        aggregates.insert(
            FunctionName::Builtin(BuiltinName::Avg),
            Arc::new(AvgUdafFactory {}),
        );
        aggregates.insert(
            FunctionName::Builtin(BuiltinName::GroupConcat),
            Arc::new(GroupConcatUdafFactory {}),
        );

        Self {
            scalars,
            aggregates,
        }
    }
}
