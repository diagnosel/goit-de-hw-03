# Verification — HW03

Verified on 9 October 2026 with Python 3.12.4, Java 17.0.17, and PySpark 4.0.1.

## Executed checks

All six blocks ran from the start. Two successful full runs produced the same ten stage text files and the same results, audit, and environment JSON files. SHA-256 values and exit codes are in `outputs/verification.json`.

`python -m unittest -v` ran 8 tests and passed. The log is in `outputs/test_run.log`. Six tests use small test fixtures for age boundaries, missing values, exact money, division precision, zero totals, and stable ranking. These fixtures are separate from the homework CSV results. Two tests check the execution records and real results.

An independent calculation in `checks.py` reads the original CSV files with Python's standard library. It uses Decimal values to check each category total, both overall totals, displayed percentages, raw percentage precision, joins, top 3, and three real purchases.

## Source and join checks

| Table | Before cleaning | After cleaning | Rows removed |
|---|---:|---:|---:|
| users | 100 | 95 | 5 |
| purchases | 200 | 195 | 5 |
| products | 50 | 47 | 3 |

Whitespace-only cells: 0 in all three files. Empty cells: 5 in users, 5 in purchases, and 3 in products. Products also has one absent trailing cell. No duplicate non-missing keys were found before or after cleaning. No duplicate removal or source edits took place.

The products and purchases link labels were reversed. Local names follow the CSV headers. Original source hashes and dropped CSV line numbers are in `outputs/audit.json`.

The product join keeps 187 of 195 clean purchases. The user join keeps 178 of those 187. The inclusive age filter keeps 32 purchases. Unique keys prevent row multiplication. Step 3 uses only products and purchases; missing users do not reduce its total.

## Results

| Category | All spending | Spending for ages 18–25 | Share for ages 18–25 |
|---|---:|---:|---:|
| Beauty | 459.9 | 41.4 | 3.43% |
| Clothing | 790.3 | 245.0 | 20.29% |
| Electronics | 1174.8 | 249.6 | 20.67% |
| Home | 1523.5 | 361.1 | 29.90% |
| Sports | 1802.5 | 310.5 | 25.71% |
| Total | 5751.0 | 1207.6 | 100.00% |

Top 3: Home, Sports, Electronics. Ranking uses exact amounts and then category names. Money is not rounded. The displayed percentages were not adjusted to force 100%.

Real purchases checked: ID 2 = 8 × 6.0 = 48.0; ID 11 = 8 × 5.9 = 47.2; ID 18 = 3 × 9.7 = 29.1. The category share for purchase 2 is Clothing's 245.0 / 1207.6 × 100 = 20.29%. The single purchase contributes 3.97%; those are different measures.

## Evidence and limits

Ten genuine browser screenshots show the saved output of executed `df.show()` calls. Steps 1 and 2 each show all three tables. Screenshots were captured from local plain text files because native Terminal access was unavailable. They were not drawn or edited. `outputs/screenshot_manifest.json` maps image hashes to text-output hashes.

The screenshot ZIP contains ten files. Its integrity and exact match with the local screenshots were checked. A secret-pattern scan of code, CSV, text, Markdown, and JSON found no matches. Non-empty user names follow `User_N`; emails use `userN@example.com`. Review data and access again before public publication.

Spark printed native-Hadoop and incubator-module warnings. The test run also printed Python fork and socket warnings. No run hung or failed because of these warnings. The exact logs are kept as evidence.

Zero-total and ranking-tie behavior passed on test fixtures. The actual CSV total is positive. No currency is stated in the source files. A public repository was created after owner approval. Nothing was sent to LMS.

Local installation paths in the shared logs use placeholders. The original execution values and warnings are kept. The private Notion guide and its verification record are not part of the public repository.
