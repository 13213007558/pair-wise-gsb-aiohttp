# Test-environment shim: yarl 1.24.2 does not re-export query_to_pairs.
try:
    import yarl

    if not hasattr(yarl, "query_to_pairs"):
        from yarl._parse import query_to_pairs

        yarl.query_to_pairs = query_to_pairs
except Exception:
    pass
