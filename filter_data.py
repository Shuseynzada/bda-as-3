import pymongo
from pymongo import MongoClient
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# This function must be at the top level to be picklable
def process_mmsi(mmsi, db_details, criteria, min_count):
    client = MongoClient('localhost', 4000)  # Adjust connection settings as needed
    db = client[db_details['name']]
    raw_collection = db[db_details['raw_collection']]
    filtered_collection = db[db_details['filtered_collection']]

    count = raw_collection.count_documents({'MMSI': mmsi})
    if count >= min_count:
        valid_documents = list(raw_collection.find({'MMSI': mmsi, **criteria}))
        if valid_documents:
            filtered_collection.insert_many(valid_documents)
    client.close()  # Close the connection

def filter_data(db_name, raw_collection_name, filtered_collection_name, criteria, min_count=100):
    client = MongoClient('localhost', 4000)
    client.close()  # Close this client as it's not used in the child processes

    db_details = {
        'name': db_name,
        'raw_collection': raw_collection_name,
        'filtered_collection': filtered_collection_name
    }

    mmsi_list = MongoClient('localhost', 4000)[db_name][raw_collection_name].distinct("MMSI", criteria)  # Get distinct MMSI values

    # Setup multiprocessing pool
    with Pool(processes=cpu_count()) as pool:
        results = list(tqdm(pool.starmap(process_mmsi, [(mmsi, db_details, criteria, min_count) for mmsi in mmsi_list]), total=len(mmsi_list), desc="Filtering vessels"))

if __name__ == "__main__":
    criteria = {
        'Navigational status': {'$ne': None},
        'MMSI': {'$ne': None},
        'Latitude': {'$ne': None},
        'Longitude': {'$ne': None},
        'ROT': {'$ne': None},
        'SOG': {'$ne': None},
        'COG': {'$ne': None},
        'Heading': {'$ne': None}
    }
    filter_data('vessel_data', 'raw_data', 'filtered_data', criteria)
