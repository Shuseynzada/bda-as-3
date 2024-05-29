import matplotlib.pyplot as plt
from pymongo import MongoClient
from datetime import datetime

def calculate_delta_times(db_name='vessel_data', collection_name='raw_data'):
    client = MongoClient('localhost', 4000)  # Adjust the port as needed
    db = client[db_name]
    collection = db[collection_name]

    # Retrieve the first 'limit' documents from the collection
    documents = collection.find({}, {'MMSI': 1, '# Timestamp': 1}).sort([('MMSI', 1), ('# Timestamp', 1)])
    
    delta_times = []

    previous_mmsi = None
    previous_timestamp = None

    for doc in documents:
        current_mmsi = doc['MMSI']
        # Convert timestamp string to datetime object
        current_timestamp = doc['# Timestamp']
        if isinstance(current_timestamp, str):
            current_timestamp = datetime.strptime(current_timestamp, "%d/%m/%Y %H:%M:%S")
        
        if previous_mmsi == current_mmsi and previous_timestamp is not None:
            delta = (current_timestamp - previous_timestamp).total_seconds()
            delta_times.append(delta)
        
        previous_mmsi = current_mmsi
        previous_timestamp = current_timestamp
    
    return delta_times

def plot_histogram(data, bins=50, title='Histogram of Delta Data', xlabel='Value(s)', ylabel='Frequency'):
    """
    Create and display a cleaned histogram from the given data.
    
    Parameters:
    - data: List of numerical values to create the histogram from.
    - bins: Number of bins for the histogram (default is 50).
    - title: Title of the histogram (default is 'Histogram of Delta Data').
    - xlabel: Label for the x-axis (default is 'Value').
    - ylabel: Label for the y-axis (default is 'Frequency').
    """
    # Remove extreme outliers
    cleaned_data = [x for x in data if -1000 <= x <= 1000]
    
    plt.figure(figsize=(10, 6))
    plt.hist(cleaned_data, bins=bins, edgecolor='black')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    delta_times = calculate_delta_times()
    
    # Plot histogram if there are delta times calculated
    if delta_times:
        plot_histogram(delta_times)
    else:
        print("No valid delta times calculated.")
