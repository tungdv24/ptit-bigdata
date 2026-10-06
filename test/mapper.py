#!/usr/bin/env python3
"""
Mapper: Đọc từng dòng, tách số, kiểm tra nguyên tố.
Input:  Dòng text chứa các số cách nhau bởi khoảng trắng hoặc mỗi số trên 1 dòng
Output: PRIME\t1     (cho mỗi số nguyên tố tìm thấy)
        NOT_PRIME\t1 (cho mỗi số không phải nguyên tố)
"""
import sys
import math


def is_prime(n):
    """Kiểm tra n có phải số nguyên tố không"""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True


# Đọc từng dòng từ stdin (Hadoop Streaming gửi dữ liệu qua stdin)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Tách các số trong dòng (hỗ trợ cả dấu cách, tab, phẩy)
    tokens = line.replace(',', ' ').split()

    for token in tokens:
        try:
            number = int(token)
            if is_prime(number):
                print("PRIME\t1")
            else:
                print("NOT_PRIME\t1")
        except ValueError:
            # Bỏ qua token không phải số
            continue
