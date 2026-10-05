#!/usr/bin/env python3
"""
Đếm số nguyên tố với PySpark - đọc input từ HDFS.

Cách dùng:
    spark-submit prime_spark.py <đường_dẫn_input>

Ví dụ:
    spark-submit prime_spark.py hdfs://localhost:9000/user/hdoop/prime/input/numbers_500mb.txt
    spark-submit prime_spark.py /user/hdoop/prime/input/numbers_1gb.txt

Nếu không truyền tham số → dùng đường dẫn mặc định bên dưới.
"""
import sys
from pyspark.sql import SparkSession
import math


def is_prime(n):
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


# Nhận đường dẫn input từ tham số dòng lệnh (để chạy đúng CÙNG file với MapReduce)
if len(sys.argv) > 1:
    input_path = sys.argv[1]
else:
    input_path = "hdfs://localhost:9000/user/hdoop/prime/input/numbers_500mb.txt"

# Tạo Spark session
spark = SparkSession.builder.appName("PrimeCounter").getOrCreate()
sc = spark.sparkContext

print(f"Đang đọc input: {input_path}")

# Đọc file input
rdd = sc.textFile(input_path)

# Tách số → chuyển sang int → lọc nguyên tố
# Dùng cùng logic tách như mapper.py (thay dấu phẩy bằng khoảng trắng) để khớp kết quả
numbers = rdd.flatMap(lambda line: line.replace(',', ' ').split()).map(int)

# Cache lại để tránh đọc/tính 2 lần (count total và count primes)
numbers.cache()

total = numbers.count()
primes = numbers.filter(is_prime).count()

print("=" * 50)
print(f"Tổng số:            {total}")
print(f"Số nguyên tố:       {primes}")
print(f"Số không nguyên tố: {total - primes}")
print("=" * 50)

spark.stop()
