import csv, json
from backend.core.paths import get_asset_metadata_csv_path,get_asset_metadata_json_path

asset_csv_path = get_asset_metadata_csv_path()
asset_json_path = get_asset_metadata_json_path()

active_assets = []
skipped_assets = 0

with open(asset_csv_path, newline="", encoding="utf-8-sig") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row["active"].strip().lower() == "n":
            skipped_assets += 1
            continue
        active_assets.append({
            "ticker": row["ticker"],
            "name": row["name"],
            "asset_type": row["asset_type"],
            "currency": row["currency"],
            # "source": row["source"],
            # "source_file_path": row["source_file_path"],
            "start_date": row["start_date"],
            "end_date": row["end_date"]
        })

# Create / overwrite JSON file
with open(asset_json_path, "w", encoding="utf-8") as jsonfile:
    json.dump(active_assets, jsonfile, indent=2)

print(f"Generated {asset_json_path}")
print(f"{len(active_assets)} active assets written to JSON")
print(f"{skipped_assets} inactive assets skipped")
