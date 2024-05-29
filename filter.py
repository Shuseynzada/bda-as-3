from pymongo import MongoClient

def filter_and_insert_filtered(db_name='vessel_data', raw_collection_name='raw_data', filtered_collection_name='filtered_data', min_data_points=100):
    client = MongoClient('localhost', 4000)  # Adjust the port as needed
    db = client[db_name]
    raw_collection = db[raw_collection_name]
    filtered_collection = db[filtered_collection_name]

    # Define initial filter criteria for invalid or unknown fields
    invalid_query = {
        '$or': [
            {'Navigational status': {'$in': [None, '', 'unknown', 'Unknown', 'Unknown value']}},
            {'MMSI': {'$in': [None, '', 'unknown', 'Unknown', 0]}},  # Checking for 0 as an invalid MMSI
            {'Latitude': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Longitude': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'ROT': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'SOG': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'COG': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Heading': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'IMO': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Callsign': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Name': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Ship type': {'$in': [None, '', 'unknown', 'Unknown', 'Undefined']}},
            {'Cargo type': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Width': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Length': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Type of position fixing device': {'$in': [None, '', 'unknown', 'Unknown', 'Undefined']}},
            {'Draught': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'Destination': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'ETA': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'A': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'B': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'C': {'$in': [None, '', 'unknown', 'Unknown']}},
            {'D': {'$in': [None, '', 'unknown', 'Unknown']}}
        ]
    }

    # Remove invalid records
    raw_collection.delete_many(invalid_query)

    # Group by MMSI and count the number of records for each vessel
    pipeline = [
        {
            '$group': {
                '_id': '$MMSI',
                'count': {'$sum': 1}
            }
        },
        {
            '$match': {
                'count': {'$gte': min_data_points}
            }
        }
    ]

    valid_vessels = list(raw_collection.aggregate(pipeline))
    valid_mmsi = [v['_id'] for v in valid_vessels]

    # Filter out vessels with fewer than min_data_points
    query = {'MMSI': {'$in': valid_mmsi}}

    filtered_data = raw_collection.find(query)
    filtered_collection.insert_many(filtered_data)

if __name__ == "__main__":
    # Filter and insert filtered data into filtered_data collection
    filter_and_insert_filtered()
