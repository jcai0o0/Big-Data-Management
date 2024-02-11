from pyspark.sql import SparkSession
import csv
from io import StringIO

"""
CLOSE CONTACTS, UNIQUE PEOPLE AT RISK
"""

# Function to parse a line in CSV format
def parse_line(line):
    input = StringIO(line)
    reader = csv.reader(input)
    return next(reader)

# Function to expand coordinates into a range within a 6-unit radius
def expand_coordinates(record):
    id, x, y, *_ = record
    x, y = int(x), int(y)
    results = []
    for dx in range(-6, 7):
        for dy in range(-6, 7):
            if dx**2 + dy**2 <= 6**2:
                results.append(((x + dx, y + dy), id))
    return results

def remove_header(rdd):
    """
    This function is used to remove the header of the CSV file from the RDD.
    It assumes that the first line of the RDD is the header.
    """
    header = rdd.first()
    return rdd.filter(lambda line: line != header)

# Initialize Spark session in local mode
spark = SparkSession.builder \
    .appName("Local Spark App") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

if __name__ == '__main__':
    # Load the CSV data into RDDs
    rdd_infected_with_header = sc.textFile("INFECTED-small.csv").map(parse_line)
    rdd_people_with_header = sc.textFile("PEOPLE-large.csv").map(parse_line)

    # Remove the header from each RDD
    rdd_infected = remove_header(rdd_infected_with_header)
    rdd_people = remove_header(rdd_people_with_header)

    # Expand the infected RDD by mapping each coordinate to multiple within the 6-unit radius
    rdd_infected_expanded = rdd_infected.flatMap(expand_coordinates)

    # Map the people RDD to key-value pairs where key is a tuple of coordinates
    rdd_people_kv = rdd_people.map(lambda record: ((int(record[1]), int(record[2])), record))

    # Perform the join operation based on the expanded keys
    rdd_joined = rdd_infected_expanded.join(rdd_people_kv)

    # Extract the required information (pi, infect-i)
    rdd_at_risk = rdd_joined.map(lambda pair: (pair[1][1], pair[1][0]))

    # Collect the results
    at_risk_contacts = rdd_at_risk.collect()

    unique_at_risk_people = []
    # Print the results
    for contact in at_risk_contacts:
        # Removing records of people infecting themselves.
        if contact[0][0] != contact[1]:
            # Removing duplicate at-risk people.
            if contact[0][0] not in unique_at_risk_people:
                unique_at_risk_people.append(contact[0][0])
                print(f"Person at risk: {contact[0]}, Infected individual: {contact[1]}")

    # Stop the Spark session
    spark.stop()