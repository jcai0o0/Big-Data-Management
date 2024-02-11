from pyspark import SparkContext

def parse_line(line):
    fields = line.split(',')
    return fields

def remove_header(rdd):
    header = rdd.first()
    return rdd.filter(lambda line: line != header)

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

if __name__ == '__main__':

    sc = SparkContext()
    lines = sc.textFile("PEOPLE-SOME-INFECTED-large.csv")
    header_removed_lines = remove_header(lines)
    parsed_lines = header_removed_lines.map(parse_line)

    rdd_infected = parsed_lines.filter(lambda x: x[-1] == 'yes')
    rdd_people = parsed_lines.filter(lambda x: x[-1] == 'no')

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

    infected_person_to_count_of_close_contacts = {}

    # Print the results
    for contact in at_risk_contacts:
        # Don't count people who "infect themselves".
        if contact[0][0] != contact[1]:
            infected_person_to_count_of_close_contacts[contact[1]] = (
                    infected_person_to_count_of_close_contacts.get(contact[1], 0) + 1)

    for key, value in infected_person_to_count_of_close_contacts.items():
        print(f"Infected person id: {key}, Count of close contacts: {value}")

    sc.stop()
