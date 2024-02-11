import csv

def extract_rows(input_file_path, output_file_path, step):
    with open(input_file_path, 'r', newline='', encoding='utf-8') as input_file:
        reader = csv.reader(input_file)

        with open(output_file_path, 'w', newline='', encoding='utf-8') as output_file:
            writer = csv.writer(output_file)

            # Write the header to the output file
            writer.writerow(next(reader))

            for i, row in enumerate(reader, start=1):
                if i % step == 0:
                    writer.writerow(row)

if __name__ == '__main__':
    # File paths
    input_file_path = 'PEOPLE-large.csv'  # replace with your input file path
    output_file_path = 'INFECTED-small.csv'

    # Extract every 200th row
    extract_rows(input_file_path, output_file_path, 200)
