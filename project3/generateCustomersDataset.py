import random
from random import randint
from typing import List

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)


def generate_names(max_id: int) -> List[str]:
    return [fake.name() for _ in range(max_id)]


def generate_ages(max_id: int) -> List[int]:
    return [randint(18, 100) for _ in range(max_id)]


def generate_country_code(max_id: int) -> List[int]:
    return [randint(1, 500) for _ in range(max_id)]


def generate_salary(max_id: int) -> List[int]:
    return [randint(100, 10000000) for _ in range(max_id)]


if __name__ == '__main__':
    print('start generating')
    MAX_ID = 50000
    df = pd.DataFrame({'ID': range(1, MAX_ID + 1),
                       'Name': generate_names(MAX_ID),
                       'Age': generate_ages(MAX_ID),
                       'CountryCode': generate_country_code(MAX_ID),
                       'Salary': generate_salary(MAX_ID)})
    df.to_csv('customers.csv', index=False)
    print('mission completed!')
