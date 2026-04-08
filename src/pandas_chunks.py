import os
import time
import pandas as pd

from src.utils import memory_mb, size_chunks

# تأكد أن results هنا قائمة
import results

filename = "../data/raw/bigdata.csv"

def process_csv(filename, chunksize=100_000, method_name="Pandas Chunks"):
    mem_before = memory_mb()
    start = time.time()

    for _ in pd.read_csv(filename, chunksize=chunksize):
        pass

    time_taken = time.time() - start
    mem_after = memory_mb()
    ram_used = mem_after - mem_before
    file_size_gb = os.path.getsize(filename) / (1024**3)

    results_list = []

    results_list.append([method_name, time_taken, file_size_gb, ram_used])
    print(f"{method_name} finished")

# استدعاء الدالة مرتين إذا تحب
process_csv(filename)
