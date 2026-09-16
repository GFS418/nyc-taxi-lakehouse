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

Hit counts are from real pipeline runs. A row can hit more than one rule.

| # | Code | Rule | 2019-01 | 2024-06 |
|---|------|------|--------:|--------:|
| 1 | `missing_required_field` | NULL vendor, timestamps, zones, payment type, distance, fare, or total | 0 | 0 |
| 2 | `invalid_location_id` | Pickup or dropoff zone outside TLC zone IDs 1–265 | 0 | 0 |
| 3 | `pickup_outside_source_month` | Pickup not inside the month of the file it came from | 537 | 51 |
| 4 | `dropoff_before_pickup` | Dropoff earlier than pickup | 4 | 8 |
| 5 | `duration_over_24h` | Meter engaged for more than 24 hours | 5 | 20 |
| 6 | `negative_distance` | Trip distance below zero | 0 | 0 |
| 7 | `distance_over_500_miles` | Trip distance above 500 miles | 2 | 119 |
| 8 | `reversal_negative_row` | Negative row that exactly negates another row of the same trip | 6,800 | 42,308 |
| 9 | `reversed_by_negative_row` | The original charge that row 8 cancels | 6,800 | 42,308 |
| 10 | `negative_amount_unmatched` | Any negative amount with no exact reversal partner | 338 | 19,624 |
| 11 | `duplicate_row` | Exact copy of a row that passed every rule; one copy is kept | 0 | 0 |

| Month | Source rows | Curated | Quarantined | Rate |
|-------|------------:|--------:|------------:|-----:|
| 2019-01 | 7,696,617 | 7,682,131 | 14,486 | 0.19% |
| 2024-06 | 3,539,193 | 3,434,764 | 104,429 | 2.95% |

Rules 1, 2, and 6 have never fired on real data. They stay because they cost nothing and guard
the warehouse contract against a bad future file.

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
