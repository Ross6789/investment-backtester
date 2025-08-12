import csv, json
from backend.core.paths import get_benchmark_metadata_csv_path, get_benchmark_metadata_json_path

benchmark_csv_path = get_benchmark_metadata_csv_path()
benchmark_json_path = get_benchmark_metadata_json_path()

active_benchmarks = []
skipped_benchmarks = 0

with open(benchmark_csv_path, newline="", encoding="utf-8-sig") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row["active"].strip().lower() == "n":
            skipped_benchmarks += 1
            continue
        active_benchmarks.append({
            "ticker": row["ticker"],
            "name": row["name"],
            "currency": row["currency"],
            "start_date": row["start_date"],
            "end_date": row["end_date"]
        })

# Create / overwrite JSON file
with open(benchmark_json_path, "w", encoding="utf-8") as jsonfile:
    json.dump(active_benchmarks, jsonfile, indent=2)

print(f"Generated {benchmark_json_path}")
print(f"{len(active_benchmarks)} active benchmarks written to JSON")
print(f"{skipped_benchmarks} inactive benchmarks skipped")
