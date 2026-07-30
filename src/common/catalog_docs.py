"""Pushes short table/column descriptions into the warehouse catalog (Unity Catalog in
production, the local Hive/Delta catalog in tests) so the contract documented in
docs/metric-glossary.md is visible directly in Catalog Explorer, not only in a markdown file an
analyst has to know to open. These strings are a hand-maintained paraphrase of that doc, not a
generated copy -- keep them in sync by hand when a metric definition changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class TableDoc:
    comment: str
    columns: dict[str, str] = field(default_factory=dict)


def apply_table_doc(spark: SparkSession, table_fqn: str, doc: TableDoc) -> None:
    spark.sql(f"COMMENT ON TABLE {table_fqn} IS '{_escape(doc.comment)}'")
    for column, comment in doc.columns.items():
        spark.sql(
            f"ALTER TABLE {table_fqn} ALTER COLUMN {column} COMMENT '{_escape(comment)}'"
        )


def _escape(text: str) -> str:
    # Spark SQL string literals use Hive-style escaping, where backslash is also a
    # metacharacter -- escape it first so a trailing/embedded backslash can't unescape
    # the quote-doubling below and break out of the literal.
    return text.replace("\\", "\\\\").replace("'", "''")
