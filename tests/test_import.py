def test_import():
    import src.analyze_xsd as m
    assert hasattr(m, "summarize_schema")
