use crate::test_utils::create_context;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::join::SparqlJoinType;
use rdf_fusion_model::{
    NamedNodePattern, TermPattern, TriplePattern, Variable, VariableRef,
};

#[test]
fn test_push_down_encodings_filter() -> DFResult<()> {
    let ctx = create_context();
    let pattern = ctx.create_pattern(
        ActiveGraph::DefaultGraph,
        None,
        TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("subject")),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked("predicate")),
            object: TermPattern::Variable(Variable::new_unchecked("object")),
        },
    );
    let expr = pattern
        .expr_builder_root()
        .variable(VariableRef::new_unchecked("subject"))?
        .with_encoding(EncodingName::PlainTerm)?
        .build()?;

    let plan = pattern.filter(expr)?.build()?;
    insta::assert_snapshot!(plan, @r"
    Filter: EFFECTIVE_BOOLEAN_VALUE(WITH_TYPED_VALUE_ENCODING(_subject_pt))
      Projection: subject, predicate, object, WITH_PLAIN_TERM_ENCODING(subject) AS _subject_pt
        QuadPattern (?subject ?predicate ?object)
    ");

    Ok(())
}

#[test]
fn test_push_down_encodings_join_reuse() -> DFResult<()> {
    let ctx = create_context();

    let pattern1 = ctx.create_pattern(
        ActiveGraph::DefaultGraph,
        None,
        TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("s")),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked("p")),
            object: TermPattern::Variable(Variable::new_unchecked("o")),
        },
    );

    let pattern2 = ctx.create_pattern(
        ActiveGraph::DefaultGraph,
        None,
        TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("s")),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked("p2")),
            object: TermPattern::Variable(Variable::new_unchecked("o2")),
        },
    );

    let expr = pattern1
        .expr_builder_root()
        .variable(VariableRef::new_unchecked("s"))?
        .with_encoding(EncodingName::PlainTerm)?
        .build()?;

    let plan = pattern1
        .filter(expr.clone())?
        .join(pattern2.build()?, SparqlJoinType::Inner, None)?
        .filter(expr)?
        .build()?;

    insta::assert_snapshot!(plan, @r"
    Filter: EFFECTIVE_BOOLEAN_VALUE(WITH_TYPED_VALUE_ENCODING(_s_pt))
      Projection: s, p, o, _s_pt, p2, o2
        SparqlJoin: Inner 
          Filter: EFFECTIVE_BOOLEAN_VALUE(WITH_TYPED_VALUE_ENCODING(_s_pt))
            Projection: s, p, o, WITH_PLAIN_TERM_ENCODING(s) AS _s_pt
              QuadPattern (?s ?p ?o)
          QuadPattern (?s ?p2 ?o2)
    ");

    Ok(())
}

#[test]
fn test_push_down_encodings_reuse_in_join_filter() -> DFResult<()> {
    let ctx = create_context();

    let pattern1 = ctx.create_pattern(
        ActiveGraph::DefaultGraph,
        None,
        TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("s")),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked("p")),
            object: TermPattern::Variable(Variable::new_unchecked("o")),
        },
    );
    let pattern1_filter = pattern1
        .expr_builder_root()
        .variable(VariableRef::new_unchecked("s"))?
        .with_encoding(EncodingName::PlainTerm)?
        .build_effective_boolean_value()?;
    let pattern1 = pattern1.filter(pattern1_filter)?;

    let pattern2 = ctx.create_pattern(
        ActiveGraph::DefaultGraph,
        None,
        TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("s")),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked("p2")),
            object: TermPattern::Variable(Variable::new_unchecked("o2")),
        },
    );

    let mut join_schema = pattern1.schema().as_ref().clone();
    join_schema.merge(pattern2.schema());

    let expr = create_context()
        .expr_builder_context_with_schema(&join_schema)
        .variable(VariableRef::new_unchecked("s"))?
        .with_encoding(EncodingName::PlainTerm)?
        .build_effective_boolean_value()?;

    let plan = pattern1
        .join(pattern2.build()?, SparqlJoinType::Inner, Some(expr))?
        .build()?;

    insta::assert_snapshot!(plan, @r"
    SparqlJoin: Inner EFFECTIVE_BOOLEAN_VALUE(WITH_TYPED_VALUE_ENCODING(_s_pt))
      Filter: EFFECTIVE_BOOLEAN_VALUE(WITH_TYPED_VALUE_ENCODING(_s_pt))
        Projection: s, p, o, WITH_PLAIN_TERM_ENCODING(s) AS _s_pt
          QuadPattern (?s ?p ?o)
      QuadPattern (?s ?p2 ?o2)
    ");

    Ok(())
}
