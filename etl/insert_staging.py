from config.init import conn, cur
from etl.sql_queries import insert_staging_songs, insert_staging_events
import json
import os

def collect_json_files(folder_paths):
    json_files = []
    for folder_path in folder_paths:
        for root, dirs, files in os.walk(folder_path):
            for filename in files:
                if filename.endswith(".json"):
                    file_path = os.path.join(root, filename)
                    json_files.append(file_path)
    return json_files

def insert_json_files(json_files, insert_query, data_type):
    for json_file in json_files:
        with open(json_file, "r") as file:
            json_data = json.load(file)
            if data_type == "songs":
                cur.execute(insert_query, json_data)
            elif data_type == "events":
                for data in json_data:
                    cur.execute(insert_query, data)
    print(f"Insert for {data_type} staging")

def main():
    folder_paths = {
        "songs": ["data/song_data/A/A", "data/song_data/A/B", "data/song_data/A/C", "data/song_data/B/A", "data/song_data/B/B", "data/song_data/B/C"],
        "events": ["data/log_data"]
    }

    json_files_songs = collect_json_files(folder_paths["songs"])
    json_files_events = collect_json_files(folder_paths["events"])

    insert_json_files(json_files_songs, insert_staging_songs, "songs")
    insert_json_files(json_files_events, insert_staging_events, "events")
    
    conn.commit()

if __name__ == "__main__":
    main()
