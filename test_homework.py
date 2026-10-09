"""Checks for real CSV results and small, clearly separate test fixtures."""
import importlib
import json
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent


class SubmissionChecks(unittest.TestCase):
    def test_all_six_steps_have_execution_records(self):
        for name in ['p1_users', 'p1_purchases', 'p1_products',
                     'p2_users', 'p2_purchases', 'p2_products',
                     'p3_categories', 'p4_age_18_25', 'p5_percentages', 'p6_top3']:
            path = ROOT / 'outputs' / f'{name}.txt'
            self.assertTrue(path.exists(), f'Missing executed output: {name}')
            self.assertIn('+', path.read_text())

    def test_real_results_match_independent_decimal_totals(self):
        path = ROOT / 'outputs' / 'results.json'
        self.assertTrue(path.exists(), 'No executed results yet')
        result = json.loads(path.read_text())
        expected_all = {'Beauty': '459.9', 'Clothing': '790.3',
                        'Electronics': '1174.8', 'Home': '1523.5', 'Sports': '1802.5'}
        expected_young = {'Beauty': '41.4', 'Clothing': '245.0',
                          'Electronics': '249.6', 'Home': '361.1', 'Sports': '310.5'}
        for step, expected in [('p3', expected_all), ('p4', expected_young)]:
            actual = {r['category']: Decimal(r['total_amount']) for r in result[step]}
            self.assertEqual(actual, {k: Decimal(v) for k, v in expected.items()})
        self.assertEqual(Decimal(result['total_all']), Decimal('5751.0'))
        self.assertEqual(Decimal(result['total_18_25']), Decimal('1207.6'))
        self.assertEqual([r['category'] for r in result['p6']],
                         ['Home', 'Sports', 'Electronics'])
        self.assertEqual([r['percentage'] for r in result['p6']],
                         ['29.90', '25.71', '20.67'])


class SparkEdgeChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hw = importlib.import_module('main')
        cls.spark = cls.hw.start_spark()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_null_empty_and_whitespace_are_missing(self):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField('value', StringType())])
        data = self.spark.createDataFrame([(None,), ('',), ('  \t',), (' text ',)], schema)
        clean = self.hw.normalize_missing(data).dropna()
        self.assertEqual([r.value for r in clean.collect()], [' text '])

    def test_zero_total_has_no_percentage(self):
        data = self.spark.createDataFrame([('A', Decimal('0.000000'))],
                                          'category string, total_amount decimal(24,6)')
        result = self.hw.add_percentages(data, Decimal('0'))
        self.assertIsNone(result.first().percentage_raw)
        self.assertIsNone(result.first().percentage)

    def test_percentage_keeps_precision_before_display_rounding(self):
        data = self.spark.createDataFrame([('Beauty', Decimal('41.400000'))],
                                          'category string, total_amount decimal(38,6)')
        result = self.hw.add_percentages(data, Decimal('1207.600000'))
        result.printSchema()
        result.show(truncate=False)
        expected = Decimal('41.4') / Decimal('1207.6') * 100
        self.assertLess(abs(result.first().percentage_raw - expected), Decimal('0.00000001'))

    def test_ranking_uses_exact_amount_and_stable_ties(self):
        data = self.spark.createDataFrame([
            ('Z', Decimal('1.000001')), ('B', Decimal('1.000000')),
            ('A', Decimal('1.000000')), ('C', Decimal('0.999999'))],
            'category string, total_amount decimal(24,6)')
        ranked = self.hw.top_three(self.hw.add_percentages(data, Decimal('4')))
        self.assertEqual([r.category for r in ranked.collect()], ['Z', 'A', 'B'])

    def test_age_boundaries_are_inclusive(self):
        data = self.spark.createDataFrame([(17,), (18,), (25,), (26,)], 'age int')
        self.assertEqual([r.age for r in self.hw.age_18_25(data).orderBy('age').collect()],
                         [18, 25])

    def test_quantity_times_decimal_price_stays_exact(self):
        data = self.spark.createDataFrame([(3, Decimal('0.100001'))],
                                          'quantity int, price decimal(18,6)')
        self.assertEqual(self.hw.add_amount(data).first().purchase_amount,
                         Decimal('0.300003'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
