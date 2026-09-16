# Data-quality rules

Spark applies these hard rules to every month before anything reaches BigQuery
(`src/lakehouse/quality.py`). A row that fails any rule goes to the quarantine table with
**every** rule it failed (`reject_reasons`, in the precedence order below) and the first one
(`reject_reason`). Nothing is dropped silently.

Each run also enforces a conservation invariant against the files it actually wrote:

```
source rows = curated rows + quarantined rows
```

If that fails, the job raises and writes no data-quality report. The warehouse load refuses
to run without a report, so an unreconciled month cannot reach BigQuery.

## Hard rules and what they caught

Counts are the full 2019-2023 backfill: 60 months and 218,118,168 source rows, of which 2,066,185,
or 0.95%, were quarantined. A row can fail several rules; each is counted under the rule that took
precedence. Per-month figures are in [backfill_summary.json](backfill_summary.json).

| # | Code | Rule | Rows |
|---|------|------|-----:|
| 1 | `missing_required_field` | NULL vendor, timestamps, zones, payment type, distance, fare, or total | 0 |
| 2 | `invalid_location_id` | Pickup or dropoff zone outside TLC zone IDs 1-265 | 0 |
| 3 | `pickup_outside_source_month` | Pickup not inside the month of the file it came from | 10,507 |
| 4 | `dropoff_before_pickup` | Dropoff earlier than pickup | 74,956 |
| 5 | `duration_over_24h` | Meter engaged for more than 24 hours | 683 |
| 6 | `negative_distance` | Trip distance below zero | 11,440 |
| 7 | `distance_over_500_miles` | Trip distance above 500 miles | 3,764 |
| 8 | `amount_out_of_range` | A monetary column beyond $10,000 in absolute value | 51 |
| 9 | `reversal_negative_row` | Negative row that exactly negates another row of the same trip | 909,527 |
| 10 | `reversed_by_negative_row` | The original charge that row 9 cancels | 909,527 |
| 11 | `negative_amount_unmatched` | Any negative amount with no exact reversal partner | 133,522 |
| 12 | `duplicate_row` | Exact copy of a row that passed every rule; one copy is kept | 12,208 |

Refund reversals account for 88% of every quarantined row. Monthly quarantine rates run from 0.19%
in 2019-01 to 2.30% in 2023-12, climbing over the years as reversal rows grow more common.

Rules 1 and 2 have never fired on real data. They stay because they cost nothing and guard the
warehouse contract against a bad future file. Rule 8 was added after the first backfill attempt
crashed: 2022-12 holds an amount of -133,391,414, which no fixed-precision money type should
silently accept.

## Why each rule is shaped the way it is

**Out-of-period pickups (rule 3).** Every monthly file contains a few pickups from other months,
including years 2001, 2008, 2026, and 2088. Rows are quarantined rather than moved to "their"
month for two reasons. The pickup clock is the thing in doubt, and a month's file must be the
only thing that writes that month's partition. That is what makes the BigQuery load an exact,
idempotent partition swap.

**Reversal pairs (rules 8–10).** Negative amounts are mostly not junk. Profiling three months
found that most negative rows exactly negate another row with the same vendor, timestamps, and
zones: 95% in 2019-01, 86% in 2023-01, and 68% in 2024-06. Matching is strictly one-to-one;
no negative row ever matched more than one original. These are voids, refunds, and disputes.

Both halves of a pair are quarantined. Keeping only the original would overstate revenue and
count a trip whose charge was cancelled. Keeping both would double-count the trip. Removing
the pair nets revenue to what was actually collected, and both rows stay queryable.

"Exactly negates" means every monetary column is negated. Matching on fare and total alone
gave identical counts; matching on fare alone matched slightly more rows whose surcharges differ,
which are not clean reversals. Most 2024-06 unmatched negatives are payment type 0 rows with a
negative fare but a positive total, which are internally inconsistent.

**Duplicates (rule 11).** No real month had an exact full-row duplicate. A looser key (vendor,
timestamps, zones, fare) finds 11 to 241 groups per month, but those rows differ in surcharges or
tips. Without a submission timestamp there is no way to know which version is correct, so they are
kept, and they get distinct `trip_id`s because the ID hashes every column.

**Thresholds.** 24 hours and 500 miles are deliberately loose: they catch only physically
implausible values. Profiling shows a cluster of roughly 20,000 trips per month in 2019 lasting 3 to
24 hours, likely meters left running. Those, zero-distance trips, and zero-duration trips are
suspicious but possible, so they stay. Phase 2 adds soft flags and a speed-based rule for them.

## What is deliberately not a hard rule

| Condition | Where it is handled | Why |
|-----------|---------------------|-----|
| Unknown vendor, rate code, or payment code | dbt `accepted_values` tests | A new code is contract drift for a human to review, not a bad row |
| Zero passengers | Kept as recorded | Possible, and common |
| Missing passenger count | Kept as NULL, never filled | 11.6% of 2024-06 trips; a 0 would fake zero-passenger trips |
| Zero distance or zero duration | Kept (Phase 2: soft flag) | Possible: cancelled or flag-drop trips |
| Near-duplicate rows | Kept, distinct `trip_id` | No way to choose the correct version |
| New or renamed source column | `SchemaContractError` fails the whole month | Schema drift must stop the pipeline, not vanish |

## Precedence

When a row fails several rules, `reject_reason` is the first in table order. Structural problems
(missing fields, invalid zones) come before temporal ones, then distance, then monetary pairing.
`duplicate_row` is only evaluated for rows that passed everything else.
