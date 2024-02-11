import string
import random
from typing import List

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)


def generate_customer_id(min_id: int, max_id: int) -> List[int]:
    return [i for i in range(min_id, max_id + 1) for _ in range(100)]


def generate_transaction_total(rows: int) -> List[float]:
    return [random.uniform(10, 2000) for _ in range(rows)]


def generate_items(rows: int) -> List[int]:
    return [random.randint(1, 15) for _ in range(rows)]


def generate_transaction_desc(rows: int) -> List[str]:
    letters = string.ascii_letters
    return [''.join(random.choices(letters, k=random.randint(20, 50))) for _ in range(rows)]


if __name__ == '__main__':
    print('start generating Purchase dataset')
    row_num = 5000000
    df = pd.DataFrame({'TransID': range(1, row_num + 1),
                       'CustID': generate_customer_id(1, 50000),
                       'TransTotal': generate_transaction_total(row_num),
                       'ItemNum': generate_items(row_num),
                       'TransDesc': generate_transaction_desc(row_num)})
    df.to_csv('purchases.csv', index=False)
    print('mission completed!')
