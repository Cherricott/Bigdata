import os
import pandas as pd
import glob
import re

# Root folder
root = r"data"

# Find all CSV files
csv_files = glob.glob(os.path.join(root, "**", "*.csv"), recursive=True)
print(f"Found {len(csv_files)} CSV files total (before filtering)")

# Keep only valid year directories
year_pattern = re.compile(r"[\\/](20\d{2})[\\/]")
filtered_files = [f for f in csv_files if year_pattern.search(f)]
print(f"Filtered to {len(filtered_files)} CSV files inside year folders")

if not filtered_files:
    raise SystemExit("❌ No CSV files found inside year folders.")


# === LOAD ALL REQUIRED COLUMNS ===
usecols = [
    "FL_DATE", "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM",
    "ORIGIN", "DEST",
    "DEP_DELAY", "ARR_DELAY",
    "DISTANCE",
    "CANCELLED", "DIVERTED",

    # Delay causes
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
]

dtypes = {
    "DEP_DELAY": "float32",
    "ARR_DELAY": "float32",
    "DISTANCE": "float32",
    "CANCELLED": "float16",
    "DIVERTED": "float16",
    "CARRIER_DELAY": "float32",
    "WEATHER_DELAY": "float32",
    "NAS_DELAY": "float32",
    "SECURITY_DELAY": "float32",
    "LATE_AIRCRAFT_DELAY": "float32",
}

chunksize = 100_000


def read_csv_in_chunks(path):
    chunks = []
    for chunk in pd.read_csv(path, usecols=usecols, dtype=dtypes,
                             chunksize=chunksize, low_memory=False):
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)


# === MAIN LOOP ===
all_data = []

for path in filtered_files:
    try:
        print(f"🔹 Reading {path}")
        df = read_csv_in_chunks(path)

        # Extract year
        year_match = year_pattern.search(path)
        if year_match:
            df["YEAR"] = int(year_match.group(1))

        all_data.append(df)
        print(f"✅ Loaded {path} ({len(df):,} rows)")
    except Exception as e:
        print(f"⚠️ Skipped {path}: {e}")


if not all_data:
    raise SystemExit("❌ No valid data loaded!")


# === MERGE AND SAVE ===
flights_df = pd.concat(all_data, ignore_index=True)
print(f"✅ Combined {len(flights_df):,} rows total")

output_path = r"flights_batch.parquet"
flights_df.to_parquet(output_path)
print(f"💾 Saved merged dataset to {output_path}")


# Fix float16 → float32
df = pd.read_parquet(output_path)

for col in df.select_dtypes(include="float16").columns:
    df[col] = df[col].astype("float32")

df.to_parquet("merged_flights_fixed.parquet", index=False)
print("✅ Re-saved merged_flights_fixed.parquet with float32 columns.")
