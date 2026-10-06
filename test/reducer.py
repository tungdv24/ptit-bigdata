#!/usr/bin/env python3
"""
Reducer: Đếm tổng số nguyên tố và không nguyên tố.
Input:  key\tvalue (Hadoop đã sort theo key)
Output: NOT_PRIME\t<tổng>
        PRIME\t<tổng>

Dùng dict để cộng dồn theo key — an toàn tuyệt đối, không phụ thuộc
thứ tự dòng hay dữ liệu đến như thế nào.
"""
import sys

counts = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 2:
        continue

    key = parts[0]
    try:
        value = int(parts[1])
    except ValueError:
        continue

    counts[key] = counts.get(key, 0) + value

# Xuất kết quả dạng key\tvalue (giữ nguyên cho Hadoop ghi ra HDFS)
for key in sorted(counts.keys()):
    print(f"{key}\t{counts[key]}")
