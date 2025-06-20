import os
import shutil

print("Starting cleanup of __pycache__ directories...")
for root, dirs, files in os.walk('.'):
    if '__pycache__' in dirs:
        pycache_path = os.path.join(root, '__pycache__')
        print(f"Removing: {pycache_path}")
        shutil.rmtree(pycache_path)
print("Cleanup finished.")