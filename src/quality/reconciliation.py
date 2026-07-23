"""Row-conservation reconciliation: every source row must be accounted for.

The invariant is `source_rows == written_rows + deduped_rows + quarantined_rows`. Each term on
the right is a deliberate, understood outcome; anything that doesn't balance is a row that
vanished for a reason nobody chose, which is the definition of silent data loss.

This is the check that would have caught the two bugs at the base of this branch before they
reached production. Both were row-count failures at heart -- duplicate source rows collapsing
into one, null merge keys collapsing distinct rows together -- and both produced a green job.

It also makes an intentional-but-invisible behaviour visible: dedupe_on_keys drops rows by
design, and until now recorded nothing. Deduping 3 rows out of 90,000 is a normal flat export.
Deduping 40,000 is a broken upstream extract that happens to produce a clean-looking table.
"""

from __future__ import annotations


class RowConservationError(RuntimeError):
    """Raised when the row counts through a stage don't balance."""


def assert_rows_conserved(
    dataset: str,
    source_rows: int,
    written_rows: int,
    deduped_rows: int = 0,
    quarantined_rows: int = 0,
) -> None:
    """Fail if rows went missing between source and target without being accounted for.

    Exact equality, not a tolerance: unlike a value reconciliation across systems (where
    rounding and timing legitimately differ), row counts within a single deterministic
    transformation have no reason to drift. A tolerance here would only hide the bug.
    """
    accounted = written_rows + deduped_rows + quarantined_rows
    if accounted != source_rows:
        raise RowConservationError(
            f"{dataset}: {source_rows} source rows but {accounted} accounted for "
            f"(written={written_rows}, deduped={deduped_rows}, "
            f"quarantined={quarantined_rows}); {source_rows - accounted} rows "
            f"unaccounted for"
        )


# Above this share of the source being dropped as duplicates, the export itself is suspect
# rather than the data. Chosen high because some of these tables legitimately restate rows.
MAX_DEDUPE_RATE = 0.30
MIN_ROWS_FOR_DEDUPE_CHECK = 100


def assert_dedupe_rate_ok(
    dataset: str,
    source_rows: int,
    deduped_rows: int,
    max_rate: float = MAX_DEDUPE_RATE,
    min_rows: int = MIN_ROWS_FOR_DEDUPE_CHECK,
) -> float:
    """Return the dedupe rate, raising when so much of the source was duplicated that the
    extract is more likely broken than the data.

    Deduping is correct and stays correct here -- this doesn't stop it, it stops the job
    silently absorbing an extract that repeated most of its rows.
    """
    if source_rows == 0:
        return 0.0

    rate = deduped_rows / source_rows
    if source_rows >= min_rows and rate > max_rate:
        raise RowConservationError(
            f"{dataset}: {deduped_rows}/{source_rows} source rows ({rate:.1%}) were "
            f"duplicate merge keys, threshold {max_rate:.0%} -- the export is more likely "
            f"broken than the data; deduping this quietly would hide it"
        )
    return rate
