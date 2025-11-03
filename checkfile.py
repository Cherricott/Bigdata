import os

root = r"C:\Users\ADMIN\Downloads\Bigdata"   # change ONLY if your CSVs are deeper

print("Current working directory:", os.getcwd())
print("Scanning recursively under:", root)
count = 0

for dirpath, dirnames, filenames in os.walk(root):
    for f in filenames:
        if f.lower().endswith(".csv"):
            count += 1
            print(os.path.join(dirpath, f))

print(f"\nTotal CSV files found: {count}")
