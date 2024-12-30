import json
import time
from datetime import datetime, timezone

def generate_json_file(file_id, table_name):
    now = datetime.now(timezone.utc)
    data = {
        "file_id": file_id,
        "table_name": table_name,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # обрезка последних 3 цифр миллисекунд
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"data/{file_id}_{table_name}_{timestamp}.json"

    with open(file_name, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Generated file: {file_name}")

if __name__ == "__main__":
    file_id = 1
    table_name = "file_metadata"

    while True:
        generate_json_file(file_id, table_name)
        file_id += 1
        time.sleep(5)
