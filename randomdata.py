import pandas as pd
import numpy as np

# Read data
df = pd.read_csv("Bigdata-test_hdfs/flights_2025_06.csv")

# Convert FL_DATE to datetime and remove time
df["FL_DATE"] = pd.to_datetime(df["FL_DATE"]).dt.date
# If you prefer datetime64 instead of date objects, use:
# df["FL_DATE"] = pd.to_datetime(df["FL_DATE"]).dt.normalize()

# Shuffle rows within each FL_DATE
df_shuffled = (
    df.assign(_r=np.random.RandomState(42).rand(len(df)))
      .sort_values(["FL_DATE", "_r"])
      .drop(columns="_r")
      .reset_index(drop=True)
)

# Save to new CSV
df_shuffled.to_csv(
    "flights_2025_06_shuffled_by_date2.csv",
    index=False
)
