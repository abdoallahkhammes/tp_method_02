import os
import time
import pandas as pd
from src.utils import memory_mb

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct path relative to script location
filename = os.path.join(script_dir, "../data/raw/bigdata.csv")
filename = os.path.normpath(filename)  # Normalize the path

print(f"Looking for file at: {filename}")


def process_csv(filename, chunksize=100_000, method_name="Pandas Chunks"):
    if not os.path.exists(filename):
        print(f"Error: File not found at {filename}")
        return

    mem_before = memory_mb()
    start = time.time()

    total_rows = 0
    for chunk in pd.read_csv(filename, chunksize=chunksize):
        total_rows += len(chunk)

    time_taken = time.time() - start
    mem_after = memory_mb()
    ram_used = mem_after - mem_before
    file_size_gb = os.path.getsize(filename) / (1024 ** 3)

    print(f"{method_name} finished")
    print(f"Time taken: {time_taken:.2f} seconds")
    print(f"File size: {file_size_gb:.2f} GB")
    print(f"RAM used: {ram_used:.2f} MB")
    print(f"Total rows processed: {total_rows}")

    return [[method_name, time_taken, file_size_gb, ram_used]]


# Run the function
results = process_csv(filename)