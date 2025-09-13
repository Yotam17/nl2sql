import csv, uuid, os

def save_to_csv(rows):
    os.makedirs("downloads", exist_ok=True)
    path = f"downloads/result_{uuid.uuid4().hex[:8]}.csv"
    if rows:
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    return path
