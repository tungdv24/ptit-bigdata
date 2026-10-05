#!/usr/bin/env python3
"""
Định dạng kết quả MapReduce sang tiếng Việt (giống output của Spark).

Đọc từ stdin các dòng dạng:
    NOT_PRIME\t<số>
    PRIME\t<số>

In ra:
    Tổng số:            <tổng>
    Số nguyên tố:       <prime>
    Số không nguyên tố: <not_prime>

Cách dùng:
    hdfs dfs -cat /user/hdoop/prime/output_mr/part-* | python3 format_mr_output.py
"""
import sys

prime = 0
not_prime = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 2:
        continue
    key, val = parts[0], parts[1]
    try:
        val = int(val)
    except ValueError:
        continue
    if key == "PRIME":
        prime += val
    elif key == "NOT_PRIME":
        not_prime += val

total = prime + not_prime

print("=" * 50)
print(f"Tổng số:            {total}")
print(f"Số nguyên tố:       {prime}")
print(f"Số không nguyên tố: {not_prime}")
print("=" * 50)
