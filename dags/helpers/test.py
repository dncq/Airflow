import os
import json
path = r'D:\FPT_DE\Fresher_DE\Airflow\data\song_data'
count = 0
for root, _, files in os.walk(path):
    for file in files:
        if not file.endswith('.json'):
            continue
        filepath = os.path.join(root, file)
        with open(filepath) as f:
            try:
                data = json.load(f)
                count += 1
                print(data)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON {filepath}: {e}")
                continue
print(f"Total valid JSON files: {count}")