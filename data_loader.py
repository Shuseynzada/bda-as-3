import csv
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys

def insert_data(chunk):
    client = MongoClient('localhost', 4000)  # Adjust the port as needed
    db = client['vessel_data']
    collection = db['raw_data']
    collection.insert_many(chunk)

def read_and_insert(file_name, chunk_size=500):
    with open(file_name, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        chunk = []
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust the number of workers as needed
            for row in reader:
                chunk.append(row)
                if len(chunk) == chunk_size:
                    futures.append(executor.submit(insert_data, chunk.copy()))
                    chunk = []
            if chunk:  # Insert any remaining data
                futures.append(executor.submit(insert_data, chunk))

        # Using tqdm to display a progress bar for the futures as they complete
        pbar = tqdm(total=len(futures), desc="Inserting data")
        for future in as_completed(futures):
            future.result()  # Handle exceptions here if necessary
            pbar.update(1)
        pbar.close()

if __name__ == "__main__":
    read_and_insert('aisdk-2023-05-01.csv')
