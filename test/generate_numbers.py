#!/usr/bin/env python3
"""
Sinh file số nguyên ngẫu nhiên với dung lượng mong muốn (1GB, 20GB, 50GB...).
Tối ưu tốc độ bằng cách ghi theo buffer lớn.

Cách dùng:
    python3 generate_numbers.py <size> <output_file> [--min MIN] [--max MAX] [--per-line N]

Ví dụ:
    python3 generate_numbers.py 1GB  numbers_1gb.txt
    python3 generate_numbers.py 20GB numbers_20gb.txt
    python3 generate_numbers.py 50GB numbers_50gb.txt --min 1 --max 1000000
    python3 generate_numbers.py 100MB test.txt --per-line 20
"""
import sys
import os
import random
import argparse
import time


def parse_size(size_str):
    """Chuyển '1GB', '500MB', '20GB' thành số byte."""
    size_str = size_str.strip().upper()
    units = {
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4,
        'B': 1,
    }
    for unit in ['TB', 'GB', 'MB', 'KB', 'B']:
        if size_str.endswith(unit):
            number = float(size_str[:-len(unit)])
            return int(number * units[unit])
    # Nếu không có đơn vị → mặc định byte
    return int(size_str)


def human_readable(num_bytes):
    """Hiển thị số byte dạng dễ đọc."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


def generate(target_bytes, output_file, min_val, max_val, per_line):
    """Sinh file cho đến khi đạt dung lượng target."""
    # Buffer ~64MB trước khi ghi ra đĩa (tối ưu tốc độ)
    BUFFER_SIZE = 64 * 1024 * 1024

    written = 0
    buffer = []
    buffer_bytes = 0
    start = time.time()
    last_report = start

    print(f"Đang sinh file: {output_file}")
    print(f"Dung lượng đích: {human_readable(target_bytes)}")
    print(f"Khoảng số: [{min_val}, {max_val}], {per_line} số/dòng")
    print("-" * 50)

    with open(output_file, 'w', buffering=BUFFER_SIZE) as f:
        while written < target_bytes:
            # Tạo 1 dòng gồm 'per_line' số ngẫu nhiên
            nums = [str(random.randint(min_val, max_val)) for _ in range(per_line)]
            line = ' '.join(nums) + '\n'
            line_bytes = len(line.encode('utf-8'))

            buffer.append(line)
            buffer_bytes += line_bytes
            written += line_bytes

            # Khi buffer đầy → ghi ra đĩa
            if buffer_bytes >= BUFFER_SIZE:
                f.write(''.join(buffer))
                buffer = []
                buffer_bytes = 0

                # Báo tiến độ mỗi 2 giây
                now = time.time()
                if now - last_report >= 2:
                    pct = written / target_bytes * 100
                    speed = written / (now - start) / (1024 ** 2)
                    print(f"  Tiến độ: {pct:5.1f}%  |  "
                          f"{human_readable(written)}  |  "
                          f"{speed:.1f} MB/s")
                    last_report = now

        # Ghi phần còn lại trong buffer
        if buffer:
            f.write(''.join(buffer))

    elapsed = time.time() - start
    actual_size = os.path.getsize(output_file)
    print("-" * 50)
    print(f"HOÀN TẤT!")
    print(f"  File:        {output_file}")
    print(f"  Dung lượng:  {human_readable(actual_size)}")
    print(f"  Thời gian:   {elapsed:.1f} giây")
    print(f"  Tốc độ TB:   {actual_size / elapsed / (1024**2):.1f} MB/s")


def main():
    parser = argparse.ArgumentParser(
        description="Sinh file số nguyên ngẫu nhiên với dung lượng cho trước."
    )
    parser.add_argument("size", help="Dung lượng đích (vd: 1GB, 20GB, 500MB)")
    parser.add_argument("output", help="Tên file đầu ra (vd: numbers.txt)")
    parser.add_argument("--min", type=int, default=1,
                        help="Giá trị nhỏ nhất (mặc định: 1)")
    parser.add_argument("--max", type=int, default=1_000_000,
                        help="Giá trị lớn nhất (mặc định: 1000000)")
    parser.add_argument("--per-line", type=int, default=20,
                        help="Số lượng số trên mỗi dòng (mặc định: 20)")

    args = parser.parse_args()

    target_bytes = parse_size(args.size)
    generate(target_bytes, args.output, args.min, args.max, args.per_line)


if __name__ == "__main__":
    main()
