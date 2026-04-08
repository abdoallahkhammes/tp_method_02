import pandas as pd
import time
import psutil, os

from src.utils import memory_mb, size_chunks

# تأكد أن results هنا عبارة عن قائمة
results = []

filename = "../data/raw/bigdata.csv"

def memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

mem_before = memory_mb()
start = time.time()

rows = 0
chunksize = 200_000

for chunk in pd.read_csv(filename, chunksize=chunksize):
    rows += len(chunk)

time_chunks = time.time() - start
mem_after = memory_mb()
ram_chunks = mem_after - mem_before

# استخدم size_chunks إذا هو متغير يحوي حجم البيانات
results.append(["Pandas Chunks", time_chunks, size_chunks, ram_chunks])
print("Pandas Chunks finished")

# إذا أردت دالة placeholder
def impimport():
    pass  # أو يمكنك حذفها إذا غير مستخدمة