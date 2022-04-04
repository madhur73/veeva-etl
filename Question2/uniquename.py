from .config import TXNS_FILE, OUTPUT_LOC
import csv
import boto3
from pathlib import Path

def download_data(bucket, filename, downloadpath):
    """ Downloads file from S3 bucket and stores file at path provided dataset/transactions.csv"""
    s3_resource = boto3.resource('s3')
    bucket_name = bucket
    file_name = filename

    s3_resource.Object(bucket_name, file_name).download_file(f'./{downloadpath}')


def get_customer_names(csv_file):
    """ Instead of loading entire csv into memory, this function will read line-by-line
    """
    last_names = dict()
    with open(csv_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for i, line in enumerate(reader):
            if i > 0:
                # transaction_id,   customer_name, product_name, product_price
                customer_name = line[i][1]
                last_names = get_unique_last_names(customer_name)

    return list(last_names.keys())

def get_unique_last_names(lookup, customer_names):
    """ puts unique last name in dictionary as lower case."""
    names = customer_names.split(' ')
    names = [name.lower() for name in names if name.isalpha()]
    last_name = None
    if len(names) == 3:
        """ firstname middlename lastname"""
        last_name = names[2]
    elif len(names) == 2:
        """ fistname lastname"""
        last_name = names[1]

    if last_name and last_name not in lookup:
        lookup[last_name] = 1

    return lookup

def save_result_to_file(data):
    """ write the unique last name to a csv file in Network Drive"""
    fileds = ['lastname']
    if data:
        rows = data
        with open(OUTPUT_LOC, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(fileds)
            writer.writerows(data)


def run():
    download_data("bucket_info", "transactions.csv", "dataset/")
    unqiue_names = get_customer_names("dataset/transactions.csv")
    save_result_to_file(unqiue_names)



if __name__ == '__main__':
    run()
