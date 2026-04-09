import pandas as pd
import time
import psutil
import os

from src.utils import memory_mb, size_chunks

# قائمة النتائج
results = []

filename = "../data/raw/bigdata.csv"

def memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

# التحقق من وجود الملف
if not os.path.exists(filename):
    print(f"خطأ: الملف {filename} غير موجود!")
    print(f"المجلد الحالي: {os.getcwd()}")
    exit(1)

# معلومات قبل المعالجة
print("=" * 60)
print("بدء معالجة الملف باستخدام Pandas Chunks")
print("=" * 60)

# حجم الملف قبل المعالجة
file_size_bytes = os.path.getsize(filename)
file_size_mb = file_size_bytes / (1024 * 1024)
file_size_gb = file_size_bytes / (1024 ** 3)

print(f"\n📁 معلومات الملف:")
print(f"  - المسار: {filename}")
print(f"  - الحجم: {file_size_mb:.2f} MB ({file_size_gb:.4f} GB)")

# الذاكرة قبل المعالجة
mem_before = memory_mb()
print(f"\n💾 الذاكرة قبل المعالجة: {mem_before:.2f} MB")

# بدء المعالجة
start = time.time()
rows = 0
chunksize = 200_000
chunk_count = 0

print(f"\n⚙️  جاري المعالجة بـ {chunksize:,} صف في كل قطعة...")

for chunk in pd.read_csv(filename, chunksize=chunksize):
    rows += len(chunk)
    chunk_count += 1
    if chunk_count % 10 == 0:  # عرض التقدم كل 10 قطع
        print(f"  - تمت معالجة {chunk_count} قطعة ({rows:,} صف)")

time_chunks = time.time() - start
mem_after = memory_mb()
ram_chunks = mem_after - mem_before

# معلومات بعد المعالجة
print(f"\n📊 نتائج المعالجة:")
print(f"  - إجمالي الصفوف: {rows:,}")
print(f"  - عدد القطع: {chunk_count}")
print(f"  - وقت المعالجة: {time_chunks:.2f} ثانية")
print(f"  - الذاكرة بعد المعالجة: {mem_after:.2f} MB")
print(f"  - الذاكرة المستخدمة: {ram_chunks:.2f} MB")

# حفظ النتائج
results.append(["Pandas Chunks", time_chunks, file_size_mb, ram_chunks, rows])

print(f"\n✅ Pandas Chunks finished successfully!")
print("=" * 60)

# عرض ملخص النتائج
print("\n📋 ملخص النتائج:")
print("-" * 60)
print(f"{'القياس':<20} {'القيمة':<20}")
print("-" * 60)
print(f"{'حجم الملف':<20} {file_size_mb:.2f} MB ({file_size_gb:.4f} GB)")
print(f"{'وقت المعالجة':<20} {time_chunks:.2f} ثانية")
print(f"{'الرام المستخدم':<20} {ram_chunks:.2f} MB")
print(f"{'عدد الصفوف':<20} {rows:,}")
print("-" * 60)

# إذا أردت حفظ النتائج في ملف
output_file = "processing_results.csv"
if os.path.exists(output_file):
    # إضافة إلى ملف موجود
    existing_results = pd.read_csv(output_file)
    new_results = pd.DataFrame(results, columns=['Method', 'Time(sec)', 'FileSize(MB)', 'RAM_Used(MB)', 'Rows'])
    final_results = pd.concat([existing_results, new_results], ignore_index=True)
    final_results.to_csv(output_file, index=False)
else:
    # إنشاء ملف جديد
    pd.DataFrame(results, columns=['Method', 'Time(sec)', 'FileSize(MB)', 'RAM_Used(MB)', 'Rows']).to_csv(output_file, index=False)

print(f"\n💾 تم حفظ النتائج في ملف: {output_file}")