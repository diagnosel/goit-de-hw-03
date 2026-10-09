# HW03 — data analysis with PySpark

This project reads three CSV files and runs all six homework steps. It calculates spending by product category for all purchases and for users aged 18–25.

## Setup

Use Python 3.9 or newer and Java 17 or newer. This project pins PySpark 4.0.1. The verified versions are stored in `outputs/environment.json` after a successful run.

Check the tools:

```bash
python3 --version
java -version
```

Create a virtual environment in this project folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python main.py
python -m unittest -v
```

`JAVA_HOME` must point to your Java installation. On this Mac, the existing Homebrew Java 17 works:

```bash
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```

For the environment already created during this homework, open Terminal in `Data_engineering_elective` and run:

```bash
source .venv/bin/activate
cd HW/goit-de-hw-03
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
python main.py
python -m unittest -v
```

This is a local job. It does not need a Spark cluster. It starts a Java process and uses local loopback ports. It does not place orders or send events to external systems.

## Files

- `main.py`: six numbered PySpark blocks and small Spark helpers.
- `checks.py`: source checks, saved output, and an independent Python Decimal calculation.
- `test_homework.py`: actual-data checks and small test fixtures for edge cases.
- `data/`: the three unchanged CSV files.
- `SOURCES.md`: original Drive URLs and official documentation.
- `outputs/p1_*.txt` through `outputs/p6_*.txt`: actual `printSchema()` and `df.show()` output.
- `outputs/results.json`: category totals, percentages, and top 3.
- `outputs/audit.json`: missing values, source checksums, joins, and three checked purchases.
- `outputs/environment.json`: versions used for the run.
- `screenshots/p1_*.jpg` through `screenshots/p6_*.jpg`: screenshots of actual saved console output displayed in a browser. They are not images drawn from data.
- `VERIFICATION.md`: completed checks and known limits.

Running `main.py` replaces the generated text and JSON outputs. It never changes the CSV files. Screenshots are separate evidence and must be captured again if the code or data changes.

## Six steps

1. Read users, purchases, and products as separate DataFrames. Show columns, schema, row count, and eight sample rows.
2. Convert empty and whitespace-only cells to null, then remove rows with any null. Show counts before and after, missing-cell counts, and all three table previews.
3. Join purchases to products by `product_id`. Sum `quantity * price` by category.
4. Join the priced purchases to users by `user_id`. Keep `18 <= age <= 25`. Sum spending by category.
5. Divide each category amount by the total spending of this age group. Multiply by 100. Round only the displayed percentage to two decimal places.
6. Select the top 3 by exact spending in descending order. Break ties by category name in ascending order.

## Data choices

The homework links mix up the labels for products and purchases. We checked the headers before naming the local files. See `SOURCES.md`.

We treat null, empty, and whitespace-only values as missing. Non-empty text is kept unchanged. Step 1 shows typed DataFrames with missing cells represented as null; no rows are removed until step 2. All fields take part in `dropna()`, including name, email, and date, as required by the homework.

We do not fix data, remove duplicates, or add extra row filters. Source checks reject invalid numeric values, dates, extra columns, and prices that would be rounded by the chosen type. Missing trailing cells remain null. Duplicate keys are reported and stop the run because they could multiply join rows.

IDs, age, and quantity use integers. Dates use `date`. Prices use `decimal(18,6)`. Source prices have at most one decimal place, so six places preserve them without rounding. Spark keeps decimal types for multiplication and sums. Money values are not rounded. `spark.sql.decimalOperations.allowPrecisionLoss=false` keeps fractional precision in wide decimal expressions. We multiply by 100 before division to avoid rounding a small ratio first. `percentage_raw` keeps eight decimal places for these inputs, before display rounding. Decimal division still has finite precision. Ranking uses the exact amount and does not depend on percentage rounding.

If the age group's total is zero, percentages are null because the share is undefined. Categories are kept and ties use their names. If the age group has no purchases, its total is zero and its category tables are empty.

## Join choices

We use `inner` joins: only matched rows can have a known price and category; only rows with a clean user can have a known age. `left_anti` joins list purchases with no matching clean product or user before the inner joins.

Step 3 does not need users, so missing users do not reduce its total. Steps 4–6 require both products and users. A join is not deduplication. The key checks confirm that each purchase matches at most one product and one user in these files.

## Submission

Prepare a public repository named `goit-de-hw-03`. Include the code, source files, README, outputs, and numbered screenshots. Do not include a virtual environment, caches, temporary files, or secrets.

Attach the screenshot archive `ДЗ3_Григорова_Ауріка_Вадимівна.zip` in LMS and add the repository link. State whether you accept the first passing grade or want to improve it after mentor feedback.

The repository is public at https://github.com/diagnosel/goit-de-hw-03. LMS submission still needs the owner's separate approval.
