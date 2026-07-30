"""Verifies apply_table_doc issues the right DDL and escapes embedded quotes, without needing a
real catalog (Unity or local Hive metastore) -- a plain object with a `.sql()` method stands in
for SparkSession here."""

from catalog_docs import TableDoc, apply_table_doc


class _RecordingSpark:
    def __init__(self):
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_apply_table_doc_issues_table_and_column_comments():
    spark = _RecordingSpark()
    doc = TableDoc(
        comment="One row per cart_id.",
        columns={"cart_id": "Business key.", "priority_score": "0-100 heuristic."},
    )

    apply_table_doc(spark, "gold.cart_recovery_signal", doc)

    assert spark.statements[0] == (
        "COMMENT ON TABLE gold.cart_recovery_signal IS 'One row per cart_id.'"
    )
    assert (
        "ALTER TABLE gold.cart_recovery_signal ALTER COLUMN cart_id "
        "COMMENT 'Business key.'" in spark.statements
    )
    assert (
        "ALTER TABLE gold.cart_recovery_signal ALTER COLUMN priority_score "
        "COMMENT '0-100 heuristic.'" in spark.statements
    )
    assert len(spark.statements) == 3


def test_apply_table_doc_escapes_single_quotes():
    spark = _RecordingSpark()
    doc = TableDoc(comment="Cart's value", columns={})

    apply_table_doc(spark, "gold.t", doc)

    assert spark.statements == ["COMMENT ON TABLE gold.t IS 'Cart''s value'"]


def test_apply_table_doc_escapes_backslashes():
    spark = _RecordingSpark()
    doc = TableDoc(comment="Windows path C:\\data\\", columns={})

    apply_table_doc(spark, "gold.t", doc)

    assert spark.statements == [
        "COMMENT ON TABLE gold.t IS 'Windows path C:\\\\data\\\\'"
    ]
