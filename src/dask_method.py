import pandas as pd
import time
import psutil, os
import results
from src.pandas_chunks import size_chunks


filename = "../data/raw/bigdata.csv"

def memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)
mem_before = memory_mb()
start = time.time()

rows = 0
for chunk in pd.read_csv(filename, chunksize=200000):
    rows += len(chunk)

time_chunks = time.time() - start
mem_after = memory_mb()
ram_chunks = mem_after - mem_before

results.append(["Pandas Chunks", time_chunks, size_chunks, ram_chunks])
print("Pandas Chunks finished")


def impimport():
    return None