import os
import pandas as pd
import glob
import re

# ✅ Root folder (contains 2018, 2019, ..., 2025 subfolders)
root = r"C:\Users\ADMIN\Downloads\Bigdata\data"

# Find all CSVs recursively
csv_files = glob.glob(os.path.join(root, "**", "*.csv"), recursive=True)
print(f"Found {len(csv_files)} CSV files total (before filtering)")

# ✅ Only keep CSVs inside folders with 4-digit year names
year_pattern = re.compile(r"[\\/](20\d{2})[\\/]")
filtered_files = [f for f in csv_files if year_pattern.search(f)]
print(f"Filtered to {len(filtered_files)} CSV files inside year folders")

if not filtered_files:
    raise SystemExit("❌ No CSV files inside valid year folders found.")


# === CONFIG ===
usecols = [
    "FL_DATE", "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM",
    "ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY", "DISTANCE", "CANCELLED"
]
dtypes = {
    "DEP_DELAY": "float32",
    "ARR_DELAY": "float32",
    "DISTANCE": "float32",
    "CANCELLED": "float16"
}
chunksize = 100_000  # adjust smaller if still low RAM

# === FUNCTION ===
def read_csv_in_chunks(path):
    """Read a CSV in small chunks to avoid memory overflow."""
    chunks = []
    for chunk in pd.read_csv(path, usecols=usecols, dtype=dtypes, chunksize=chunksize, low_memory=False):
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)


# === MAIN LOOP ===
all_data = []

for path in filtered_files:
    try:
        print(f"🔹 Reading {path}")
        df = read_csv_in_chunks(path)
        # Extract year from folder path (first 4-digit sequence)
        year_match = year_pattern.search(path)
        if year_match:
            df["YEAR"] = int(year_match.group(1))
        all_data.append(df)
        print(f"✅ Loaded {path} ({len(df):,} rows)")
    except Exception as e:
        print(f"⚠️ Skipped {path}: {e}")

if not all_data:
    raise SystemExit("❌ Files were found but none could be read properly.")


# === MERGE AND SAVE ===
flights_df = pd.concat(all_data, ignore_index=True)
print(f"✅ Combined {len(flights_df):,} rows total")

output_path = r"C:\Users\ADMIN\Downloads\Bigdata\flights_batch.parquet"
flights_df.to_parquet(output_path)
print(f"💾 Saved merged dataset to {output_path}")
