import pandas as pd
import time
import os
import psutil

filename = "../data/raw/bigdata.csv"
compressed_filename = "../data/compressed/bigdata.csv.gz"

# إنشاء مجلد الضغط إذا لم يكن موجوداً
os.makedirs("../data/compressed", exist_ok=True)

def memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

# حجم الملف قبل الضغط
file_size_before = os.path.getsize(filename) / (1024 * 1024)  # MB

# قياس الرام والوقت قبل الضغط
mem_before = memory_mb()
start = time.time()

# عملية الضغط
print("Compressing...")
df_full = pd.read_csv(filename)
df_full.to_csv(compressed_filename, index=False, compression="gzip")

# قياس الرام والوقت بعد الضغط
time_taken = time.time() - start
mem_after = memory_mb()
ram_used = mem_after - mem_before

# حجم الملف بعد الضغط
file_size_after = os.path.getsize(compressed_filename) / (1024 * 1024)  # MB

# عرض النتائج
print("\n" + "=" * 50)
print("النتائج:")
print("=" * 50)
print(f"حجم الملف قبل الضغط:    {file_size_before:.2f} MB")
print(f"حجم الملف بعد الضغط:    {file_size_after:.2f} MB")
print(f"الرام المستخدم:         {ram_used:.2f} MB")
print(f"وقت الضغط:              {time_taken:.2f} ثانية")
print("=" * 50)
print("Compressing done.")