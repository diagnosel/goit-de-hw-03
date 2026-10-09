"""HW03: six PySpark steps. Run with Python from the project folder."""
import os
import platform
import sys
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DateType, DecimalType, IntegerType, StringType,
                              StructField, StructType)

from checks import capture, inspect_sources, save_json, verify_results

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / 'outputs'
SCHEMAS = {
    'users': [('user_id', IntegerType()), ('name', StringType()),
              ('age', IntegerType()), ('email', StringType())],
    'purchases': [('purchase_id', IntegerType()), ('user_id', IntegerType()),
                  ('product_id', IntegerType()), ('date', DateType()),
                  ('quantity', IntegerType())],
    'products': [('product_id', IntegerType()), ('product_name', StringType()),
                 ('category', StringType()), ('price', DecimalType(18, 6))],
}


def start_spark():
    # Use the current Python and loopback for a local run.
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    spark = (SparkSession.builder.appName('goit-de-hw-03').master('local[2]')
             .config('spark.driver.host', '127.0.0.1')
             .config('spark.driver.bindAddress', '127.0.0.1')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.ui.enabled', 'false')
             .config('spark.ui.showConsoleProgress', 'false')
             # Keep fractional precision when decimal expressions reach 38 digits.
             .config('spark.sql.decimalOperations.allowPrecisionLoss', 'false')
             .config('spark.sql.ansi.enabled', 'true').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def normalize_missing(df):
    # Treat empty or whitespace-only strings as null. Keep other text unchanged.
    return df.select(*[
        F.when(F.col(c).rlike(r'^\s*$'), F.lit(None)).otherwise(F.col(c)).alias(c)
        for c in df.columns
    ])


def read_table(spark, name):
    fields = SCHEMAS[name]
    raw_schema = StructType([StructField(c, StringType(), True) for c, _ in fields])
    raw = (spark.read.option('header', True).option('mode', 'PERMISSIVE')
           .schema(raw_schema).csv(str(ROOT / 'data' / f'{name}.csv')))
    # Source checks reject invalid values before casts. A short row is a missing cell.
    return normalize_missing(raw).select(*[
        F.col(c).cast(dtype).alias(c) for c, dtype in fields
    ])


def add_amount(df):
    return df.withColumn('purchase_amount', F.col('quantity') * F.col('price'))


def age_18_25(df):
    return df.filter(F.col('age').between(18, 25))


def add_percentages(df, total):
    # A zero denominator has no defined share. Keep categories and show null.
    # Multiply first so division does not round a small ratio before scaling it.
    expression = (F.col('total_amount') * F.lit(100) / F.lit(total)
                  if total != 0 else F.lit(None).cast(DecimalType(38, 6)))
    return (df.withColumn('percentage_raw', expression)
            .withColumn('percentage', F.round('percentage_raw', 2)))


def top_three(df):
    return df.orderBy(F.desc('total_amount'), F.asc('category')).limit(3)


def run():
    OUTPUT.mkdir(exist_ok=True)
    source = inspect_sources(ROOT, SCHEMAS)
    spark = start_spark()
    try:
        print(f'Python {platform.python_version()} | Spark {spark.version} | '
              f'Java {spark._jvm.java.lang.System.getProperty("java.version")}')

        # 1. Read three separate DataFrames. Show columns, types, and sample rows.
        tables = {name: read_table(spark, name).cache() for name in SCHEMAS}
        for name, df in tables.items():
            with capture(OUTPUT, f'p1_{name}'):
                print(f'STEP 1 | {name} | rows: {df.count()} | first 8 rows')
                print('Columns:', ', '.join(df.columns))
                df.printSchema()
                df.orderBy(df.columns[0]).show(8, truncate=False)

        # 2. Remove any row with a missing value in any column. No deduplication.
        clean = {name: df.dropna(how='any').cache() for name, df in tables.items()}
        for name, df in clean.items():
            with capture(OUTPUT, f'p2_{name}'):
                before, after = tables[name].count(), df.count()
                source[name]['after'] = after
                print(f'STEP 2 | {name} | before: {before} | after: {after} | '
                      f'removed: {before - after}')
                print('Empty cells:', source[name]['empty_cells'],
                      '| absent cells:', source[name]['absent_cells'],
                      '| whitespace-only cells:', source[name]['whitespace_cells'])
                print('Dropped CSV line numbers:', source[name]['dropped_lines'])
                df.orderBy(df.columns[0]).show(8, truncate=False)

        users, purchases, products = clean['users'], clean['purchases'], clean['products']
        # Duplicate keys would multiply join rows. Stop rather than remove them.
        for name, df in clean.items():
            key = df.columns[0]
            duplicates = df.groupBy(key).count().filter(F.col('count') > 1).collect()
            assert not duplicates, f'Duplicate keys in {name}: {duplicates}'
        missing_products = purchases.join(products, 'product_id', 'left_anti')
        missing_users = purchases.join(users, 'user_id', 'left_anti')
        joins = {
            'clean_purchases': purchases.count(),
            'without_product': [r.purchase_id for r in missing_products.orderBy('purchase_id').collect()],
            'without_user': [r.purchase_id for r in missing_users.orderBy('purchase_id').collect()],
        }

        # 3. All-category spending only needs purchases and products.
        priced = add_amount(purchases.join(products, 'product_id', 'inner')).cache()
        all_categories = (priced.groupBy('category')
                          .agg(F.sum('purchase_amount').alias('total_amount'))
                          .orderBy('category'))
        total_all = priced.agg(F.sum('purchase_amount')).first()[0] or Decimal(0)
        joins['with_product'] = priced.count()
        with capture(OUTPUT, 'p3_categories'):
            print(f'STEP 3 | all buyers | priced purchases: {priced.count()}')
            print('Unmatched purchase IDs (no clean product):', joins['without_product'])
            all_categories.show(truncate=False)
            print('Total spending:', total_all)

        # 4. Attach clean users and keep both age boundaries.
        with_users = priced.join(users, 'user_id', 'inner').cache()
        young = age_18_25(with_users).cache()
        young_categories = (young.groupBy('category')
                            .agg(F.sum('purchase_amount').alias('total_amount'))
                            .orderBy('category'))
        total_young = young.agg(F.sum('purchase_amount')).first()[0] or Decimal(0)
        joins.update(with_product_and_user=with_users.count(), age_18_25=young.count())
        with capture(OUTPUT, 'p4_age_18_25'):
            print(f'STEP 4 | ages 18 through 25 | purchases: {young.count()}')
            print(f'User join: {priced.count()} before, {with_users.count()} after')
            young_categories.show(truncate=False)
            print('Total spending for ages 18-25:', total_young)

        # 5. Divide by this age group's total. Round only the displayed percentage.
        percentages = add_percentages(young_categories, total_young)
        with capture(OUTPUT, 'p5_percentages'):
            print('STEP 5 | denominator: total spending for ages 18-25')
            print('Denominator:', total_young, '| zero total: show null percentage')
            percentages.select('category', 'total_amount', 'percentage').orderBy('category').show(truncate=False)
            print('Sum of displayed percentages:', percentages.agg(F.sum('percentage')).first()[0])

        # 6. Rank by exact spending, then category name for stable ties.
        top = top_three(percentages).select('category', 'total_amount', 'percentage')
        with capture(OUTPUT, 'p6_top3'):
            print('STEP 6 | top 3 | amount descending, category ascending')
            top.show(truncate=False)

        results = {'p3': all_categories.collect(), 'p4': young_categories.collect(),
                   'p5': percentages.orderBy('category').collect(), 'p6': top.collect(),
                   'total_all': total_all, 'total_18_25': total_young}
        samples = young.orderBy('purchase_id').select(
            'purchase_id', 'user_id', 'age', 'product_id', 'product_name',
            'category', 'quantity', 'price', 'purchase_amount').limit(3).collect()
        verify_results(ROOT, results, source, joins, samples)
        save_json(OUTPUT / 'results.json', results)
        save_json(OUTPUT / 'audit.json', {'sources': source, 'joins': joins, 'samples': samples})
        save_json(OUTPUT / 'environment.json', {
            'python': platform.python_version(), 'spark': spark.version,
            'java': spark._jvm.java.lang.System.getProperty('java.version'),
            'master': spark.sparkContext.master,
        })
        print('PASS | all six steps checked against independent Python Decimal calculations')
    finally:
        spark.stop()


if __name__ == '__main__':
    run()
