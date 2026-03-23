import os
import time

import pandas as pd
import results
from src.dask_method import memory_mb

filename = "../data/raw/bigdata.csv"

mem_before = memory_mb()
start = time.time()

for _ in pd.read_csv(filename, chunksize=100000):
    pass

time_chunks = time.time() - start
mem_after = memory_mb()
ram_chunks = mem_after - mem_before
size_chunks = os.path.getsize(filename) / (1024**3)

results.append(["Pandas Chunks", time_chunks, size_chunks, ram_chunks])
print("pandas finished")
memory_mb()

filename = "../data/raw/bigdata.csv"
mem_before = memory_mb()
start = time.time()

for _ in pd.read_csv(filename, chunksize=100000):
    pass

time_chunks = time.time() - start
mem_after = memory_mb()
ram_chunks = mem_after - mem_before
size_chunks = os.path.getsize(filename) / (1024**3)

results.append(["Pandas Chunks", time_chunks, size_chunks, ram_chunks])
print("pandas finished")