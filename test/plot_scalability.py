#!/usr/bin/env python3
"""
plot_scalability.py — Vẽ biểu đồ tổng hợp so sánh MapReduce vs Spark qua các mốc dữ liệu.
Đọc từ master_summary.csv và sinh ra scalability_chart.png.
"""
import sys
import os
import csv

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("Thiếu matplotlib. Cài bằng: pip3 install matplotlib")
    sys.exit(1)


def plot_scalability(csv_path, output_png):
    datasets = []
    mr_times = []
    sp_times = []
    speedups = []

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            datasets.append(row["dataset"])
            mr_times.append(float(row["mr_runtime_sec"]))
            sp_times.append(float(row["spark_runtime_sec"]))
            speedups.append(float(row["speedup"]))

    if not datasets:
        print("Không có dữ liệu trong master_summary.csv")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle("TỔNG HỢP HIỆU NĂNG: HADOOP MAPREDUCE VS APACHE SPARK (8GB RAM / 4 vCores)", fontsize=13, fontweight="bold")

    x = np.arange(len(datasets))
    width = 0.35

    # Subplot 1: Thời gian thực thi (Cột so sánh)
    rects1 = ax1.bar(x - width/2, mr_times, width, label="MapReduce", color="#e67e22", edgecolor="#d35400", alpha=0.9)
    rects2 = ax1.bar(x + width/2, sp_times, width, label="Apache Spark", color="#27ae60", edgecolor="#1e8449", alpha=0.9)

    ax1.set_ylabel("Thời gian chạy (giây - Log scale)")
    ax1.set_title("1. So sánh thời gian thực thi (Runtime)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(datasets, fontweight="bold")
    ax1.set_yscale("log")  # Dùng thang log để thấy rõ từ 500MB đến 10GB
    ax1.legend(loc="upper left")
    ax1.grid(True, which="both", linestyle="--", alpha=0.4)

    # Ghi nhãn thời gian trên từng cột
    for rect in rects1:
        h = rect.get_height()
        ax1.annotate(f"{h:.1f}s", xy=(rect.get_x() + rect.get_width() / 2, h),
                     xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold")

    for rect in rects2:
        h = rect.get_height()
        ax1.annotate(f"{h:.1f}s", xy=(rect.get_x() + rect.get_width() / 2, h),
                     xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold")

    # Subplot 2: Hệ số tăng tốc (Speedup Factor)
    ax2.plot(datasets, speedups, marker="o", color="#2980b9", linewidth=2.5, markersize=8, label="Speedup (MR / Spark)")
    ax2.fill_between(datasets, speedups, alpha=0.15, color="#2980b9")
    ax2.set_ylabel("Hệ số tăng tốc (lần - Speedup)")
    ax2.set_title("2. Tốc độ vượt trội của Spark theo dung lượng")
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.set_ylim(bottom=1.0, top=max(speedups) * 1.3)

    for i, txt in enumerate(speedups):
        ax2.annotate(f"{txt:.2f}x", (datasets[i], speedups[i]), textcoords="offset points", xytext=(0, 10),
                     ha="center", fontweight="bold", fontsize=10, color="#1a5276")

    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    print(f"✓ Đã sinh biểu đồ tổng hợp: {output_png}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Cách dùng: python3 plot_scalability.py <master_summary.csv> <output.png>")
        sys.exit(1)
    plot_scalability(sys.argv[1], sys.argv[2])
