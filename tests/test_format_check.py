from editguard.tools.format_check import violations


def test_only_tables_not_at_version_2_are_reported() -> None:
    versions = {"prod_bronze.edits": 2, "prod_gold.fact_edit": 3, "prod_silver.x": None}
    assert violations(versions) == [
        "prod_gold.fact_edit: format-version 3",
        "prod_silver.x: format-version None",
    ]
    assert violations({"prod_bronze.edits": 2}) == []
