# src/utils.py
import psutil, os

def memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024*1024)

def size_chunks(filename):
    return os.path.getsize(filename) / (1024**3)