import csv
from datetime import datetime
from app.models import crypto_tweet_repo



def import_tweets(csv_path):
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        
        for row in csv_reader:
            try:
                # Transform CSV row to our schema
                processed_data = {
                    'user_name': row['user_name'],
                    'user_location': row['user_location'],
                    'user_followers': float(row['user_followers']),
                    'text': row['text'],
                    'hashtags': row['hashtags'],
                    'coin_symbols': row['coin_symbols'],
                    'coin_names': row['coin_names'],
                    'price_data': row['price_data'],
                    'date': row['date']
                }
                
                # Insert into MongoDB
                result = crypto_tweet_repo.create_tweet(processed_data)
                print(f"Inserted document ID: {result.inserted_id}")

            except Exception as e:
                print(f"Error processing row: {e}")
                continue

import_tweets('filtered_df_extended_sorted3.csv')