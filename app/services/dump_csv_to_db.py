import pandas as pd
from pymongo import DESCENDING, ASCENDING

from db_service import mongo_service


crypto_dataset_collection = mongo_service.get_collection('crypto_dataset')
# Instead of inserting in one large batch, we will insert in smaller batches to ensure smooth operations
def process_and_insert_chunk(chunk):
    try:
        # Strip leading and trailing spaces from column names
        chunk.columns = chunk.columns.str.strip()

        # Check if required columns exist
        required_columns = ['user_name', 'user_location', 'user_description', 'user_created', 
                            'user_followers', 'user_friends', 'user_favourites', 'user_verified', 
                            'date', 'text', 'hashtags', 'source', 'is_retweet']
        
        missing_columns = [col for col in required_columns if col not in chunk.columns]
        if missing_columns:
            print(f"Skipping chunk due to missing columns: {missing_columns}")
            return  # Skip this chunk if any required columns are missing

        # Convert the 'date' column to datetime format
        chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')

        # Drop rows with NaT in 'date' or NaN in 'text' columns
        chunk = chunk.dropna(subset=['date', 'text'])

        # Convert DataFrame to a list of dictionaries and insert into MongoDB
        data = chunk.to_dict(orient='records')

        new_records = []

        # Check for duplicates and collect only new records
        for record in data:
            # Check if the record already exists in the collection based on 'date' and 'text'
            existing_record = crypto_dataset_collection.find_one({
                'date': record['date'], 
                'text': record['text']
            })
            
            # If no record exists, add the record to the new_records list
            if not existing_record:
                new_records.append(record)
        print("new record found!", len(new_records))

        # If there are new records, perform batch insert
        if new_records:
            batch_size = 1000  # Adjust this as needed for MongoDB Atlas compatibility
            for i in range(0, len(new_records), batch_size):
                batch = new_records[i:i + batch_size]
                crypto_dataset_collection.insert_many(batch)
                print(f"Inserted {len(batch)} new records into MongoDB!")
        
    except Exception as e:
        print(f"Error inserting chunk: {e}")


chunksize = 500  # Number of rows per chunk

csv_file = 'Bitcoin_tweets_dataset_2.csv'

crypto_dataset_collection.create_index([ ('date', ASCENDING), ('text', ASCENDING)], unique=True)

skip_rows = list(range(1, 5900))
total_chunk = 0
# Read the CSV in chunks and process each chunk
for chunk in pd.read_csv(csv_file, chunksize=chunksize, on_bad_lines='skip', skiprows=skip_rows, engine='python'):
    # Process and insert each chunk into MongoDB
    if total_chunk != 1500:
        chunk = chunk.dropna(subset=['date', 'text'])
        process_and_insert_chunk(chunk)
        total_chunk+=chunksize

print("All data has been processed and inserted into MongoDB!")

