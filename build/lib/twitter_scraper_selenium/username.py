import csv


def get_records_by_field(csv_file, field_name):
    records = []

    with open(csv_file, 'r', newline='', errors="ignore") as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            if field_name in row:
                records.append(row[field_name])

    return records


def write_records_to_csv(records, output_file):
    with open(output_file, 'a', newline='') as file:
        csv_writer = csv.writer(file)
        # csv_writer.writerow([field_name])  # Write header

        for record in records:
            names = record.split(',')
            csv_writer.writerow([','.join(names)])


# Example usage:
csv_file_path = 'gender-classifier-DFE-791531.csv'
desired_field = 'name'
output_csv_file = 'Username.csv'

result = get_records_by_field(csv_file_path, desired_field)
write_records_to_csv(result, output_csv_file)