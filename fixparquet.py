
import pandas as pd

df = pd.read_parquet(r"flights_batch.parquet")

# Convert any float16 columns to float32
for col in df.select_dtypes(include="float16").columns:
    df[col] = df[col].astype("float32")

# Save back
df.to_parquet(r"merged_flights_fixed.parquet", index=False)
print("✅ Re-saved Parquet with float32 columns.")