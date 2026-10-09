"""Input checks and an independent CSV calculation. No input data is changed."""
import csv
import hashlib
import io
import json
from collections import Counter, defaultdict
from contextlib import contextmanager, redirect_stdout
from datetime import date
from decimal import Decimal, ROUND_HALF_UP


@contextmanager
def capture(folder, name):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        yield
    text = buffer.getvalue()
    (folder / f'{name}.txt').write_text(text, encoding='utf-8')
    print(text, end='')


def save_json(path, value):
    def plain(item):
        if hasattr(item, 'asDict'):
            return {k: plain(v) for k, v in item.asDict().items()}
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, dict):
            return {k: plain(v) for k, v in item.items()}
        if isinstance(item, (list, tuple)):
            return [plain(v) for v in item]
        return item
    path.write_text(json.dumps(plain(value), indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def is_missing(value):
    return value is None or not value.strip()


def inspect_sources(root, schemas):
    report = {}
    originals = {}
    for name, fields in schemas.items():
        path = root / 'data' / f'{name}.csv'
        with path.open(newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == [c for c, _ in fields], f'Wrong header: {name}'
            rows = list(reader)
        originals[name] = rows
        assert all(None not in r for r in rows), f'Unexpected extra CSV columns: {name}'
        precision = 0
        for line, row in enumerate(rows, start=2):
            for column, dtype in fields:
                value = row[column]
                if is_missing(value):
                    continue
                kind = dtype.simpleString()
                if kind == 'int':
                    parsed = int(value)
                    assert -(2**31) <= parsed < 2**31, f'Integer overflow: {name}:{line}'
                elif kind == 'date':
                    date.fromisoformat(value)
                elif kind.startswith('decimal'):
                    parsed = Decimal(value)
                    assert parsed.is_finite(), f'Invalid decimal: {name}:{line}'
                    precision = max(precision, -parsed.as_tuple().exponent)
                    assert parsed == parsed.quantize(Decimal('0.000001')), 'Price would be rounded'
                    assert abs(parsed) < Decimal('1000000000000'), 'Price does not fit decimal(18,6)'
        key = fields[0][0]
        duplicate_keys = {k: v for k, v in Counter(r[key] for r in rows if not is_missing(r[key])).items() if v > 1}
        report[name] = {
            'before': len(rows), 'empty_cells': sum(v == '' for r in rows for v in r.values()),
            'absent_cells': sum(v is None for r in rows for v in r.values()),
            'whitespace_cells': sum(v is not None and v != '' and not v.strip() for r in rows for v in r.values()),
            'dropped_lines': [i for i, r in enumerate(rows, start=2) if any(is_missing(v) for v in r.values())],
            'duplicate_keys': duplicate_keys, 'price_decimal_places': precision if name == 'products' else None,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    user_keys = {r['user_id'] for r in originals['users'] if not is_missing(r['user_id'])}
    product_keys = {r['product_id'] for r in originals['products'] if not is_missing(r['product_id'])}
    report['purchases']['raw_references'] = {
        'missing_user_id': [r['purchase_id'] for r in originals['purchases'] if is_missing(r['user_id'])],
        'missing_product_id': [r['purchase_id'] for r in originals['purchases'] if is_missing(r['product_id'])],
        'user_id_not_found': [r['purchase_id'] for r in originals['purchases']
                              if not is_missing(r['user_id']) and r['user_id'] not in user_keys],
        'product_id_not_found': [r['purchase_id'] for r in originals['purchases']
                                 if not is_missing(r['product_id']) and r['product_id'] not in product_keys],
    }
    return report


def verify_results(root, results, report, joins, samples):
    # A separate Python path checks Spark; it does not call any Spark calculation.
    tables = {}
    for name in report:
        with (root / 'data' / f'{name}.csv').open(newline='', encoding='utf-8') as handle:
            raw = list(csv.DictReader(handle))
        tables[name] = [r for r in raw if all(not is_missing(v) for v in r.values())]
        assert len(tables[name]) == report[name]['after']
        assert not report[name]['duplicate_keys'], f'Duplicate source keys in {name}'
    users = {int(r['user_id']): r for r in tables['users']}
    products = {int(r['product_id']): r for r in tables['products']}
    all_totals, young_totals = defaultdict(Decimal), defaultdict(Decimal)
    missing_product, missing_user, young_rows = [], [], []
    priced_count = both_count = 0
    for row in tables['purchases']:
        product = products.get(int(row['product_id']))
        user = users.get(int(row['user_id']))
        if product is None:
            missing_product.append(int(row['purchase_id']))
        if user is None:
            missing_user.append(int(row['purchase_id']))
        if product is not None:
            priced_count += 1
            amount = Decimal(row['quantity']) * Decimal(product['price'])
            all_totals[product['category']] += amount
            if user is not None:
                both_count += 1
                if 18 <= int(user['age']) <= 25:
                    young_totals[product['category']] += amount
                    young_rows.append((row, user, product, amount))
    assert sorted(missing_product) == joins['without_product']
    assert sorted(missing_user) == joins['without_user']
    assert priced_count == joins['with_product']
    assert both_count == joins['with_product_and_user']
    assert len(young_rows) == joins['age_18_25']
    for step, expected in [('p3', all_totals), ('p4', young_totals)]:
        assert {r.category: r.total_amount for r in results[step]} == dict(expected)
    assert sum(all_totals.values()) == results['total_all']
    assert sum(young_totals.values()) == results['total_18_25']
    for row in results['p5']:
        expected = young_totals[row.category] / results['total_18_25'] * 100 if results['total_18_25'] else None
        assert row.percentage == (expected.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if expected is not None else None)
        if expected is not None:
            assert abs(row.percentage_raw - expected) < Decimal('0.00001')
    assert [r.category for r in results['p6']] == sorted(young_totals, key=lambda c: (-young_totals[c], c))[:3]
    for actual, (purchase, user, product, amount) in zip(samples, sorted(young_rows, key=lambda r: int(r[0]['purchase_id']))):
        assert actual.purchase_id == int(purchase['purchase_id'])
        assert actual.quantity == int(purchase['quantity'])
        assert actual.price == Decimal(product['price'])
        assert actual.purchase_amount == amount
        assert actual.age == int(user['age'])
    print('PASS | independent CSV check: row counts, joins, category totals, shares, top 3, and 3 purchases')
