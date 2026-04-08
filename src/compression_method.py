import pandas as pd

filename = "../data/raw/bigdata.csv"
compressed_filename = "../data/compressed/bigdata.csv.gz"  # يجب أن ينتهي بـ .gz

print("Compressing")
df_full = pd.read_csv(filename)
df_full.to_csv(compressed_filename, index=False, compression="gzip")
print("Compressing done.\n")