from faker import Faker
import csv
import random

def generate_data(file_path, num_records):
    # Initialize Faker with English locale for American names
    fake = Faker('en_US')

    # Create a CSV file
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(['id', 'x', 'y', 'name', 'age'])

        # Generate and write fake data
        for i in range(num_records):
            writer.writerow([
                fake.unique.uuid4(),      # Unique ID
                random.randint(1, 10000), # x coordinate
                random.randint(1, 10000), # y coordinate
                fake.name(),              # American name
                random.randint(25, 100)   # Age, between 25 and 100
            ])

# Path where the CSV file will be saved
file_path = 'PEOPLE-large.csv'

# Number of records to generate
num_records = 10_000

if __name__ == '__main__':
    # Generate the data
    generate_data(file_path, num_records)
