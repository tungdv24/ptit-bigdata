#!/usr/bin/env python3
"""
plot_monitor.py — Vẽ biểu đồ giám sát tài nguyên và Disk I/O: MapReduce vs Spark.

Sinh ra:
    1. resource_chart.png: Dashboard 4 biểu đồ (2x2) GỘP ĐÈ LÊN NHAU từ t=0:
       - Hàng 1: So sánh CPU server (%) | So sánh RAM server (GB)
       - Hàng 2: Gộp (3 + 5) YARN vCores | Gộp (4 + 6) YARN RAM (GB)
    2. timeline_chart.png: Biểu đồ toàn cảnh theo dòng thời gian thực tế (wall-clock time).
    3. disk_io_chart.png: Biểu đồ Disk IOPS & Throughput MB/s.

Yêu cầu: pip3 install matplotlib
"""
import sys
import os
import csv

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError:
    print("Chưa cài matplotlib. Cài bằng: pip3 install matplotlib")
    sys.exit(1)

from datetime import datetime, timedelta

MB_TO_GB = 1024.0


def read_raw_rows(csv_file):
    """Đọc toàn bộ các dòng CSV dạng list of dict."""
    rows = []
    with open(csv_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                row_data = {
                    "epoch": int(r["epoch"]),
                    "elapsed_sec": int(r["elapsed_sec"]),
                    "cpu": float(r["server_cpu"]),
                    "ram_gb": int(r["server_ram_mb"]) / MB_TO_GB,
                    "mr_ram_gb": int(r["mr_ram_mb"]) / MB_TO_GB,
                    "mr_vcores": int(r["mr_vcores"]),
                    "sp_ram_gb": int(r["spark_ram_mb"]) / MB_TO_GB,
                    "sp_vcores": int(r["spark_vcores"]),
                    "disk_read_iops": float(r.get("disk_read_iops", 0) or 0),
                    "disk_write_iops": float(r.get("disk_write_iops", 0) or 0),
                    "disk_read_mb": float(r.get("disk_read_mb_s", 0) or 0),
                    "disk_write_mb": float(r.get("disk_write_mb_s", 0) or 0),
                }
                rows.append(row_data)
            except (ValueError, KeyError):
                continue
    return rows


def read_phases(phases_file):
    """Đọc file phases, trả về dict {name: (start_epoch, end_epoch)}."""
    phases = {}
    try:
        with open(phases_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    name = row["phase"]
                    s = int(row["start_epoch"])
                    e = int(row["end_epoch"])
                    phases[name] = (s, e)
                except (ValueError, KeyError):
                    continue
    except Exception:
        pass
    return phases


def plot_overlay_dashboard(rows, phases, out_png):
    """
    GỘP THỰC SỰ: Đưa cả MapReduce và Spark về cùng mốc t = 0 (Relative Elapsed Seconds).
    Vẽ trực tiếp đường MapReduce và đường Spark đè lên nhau trên cùng 1 đồ thị!
    """
    mr_range = phases.get("MapReduce")
    sp_range = phases.get("Spark")

    if not mr_range or not sp_range:
        print("[!] Không tìm thấy thông tin 2 pha trong phases.csv để vẽ gộp t=0.")
        return

    mr_s, mr_e = mr_range
    sp_s, sp_e = sp_range

    # Trích xuất dữ liệu riêng cho từng pha
    mr_t, mr_cpu, mr_ram, mr_vc, mr_yarn_ram = [], [], [], [], []
    sp_t, sp_cpu, sp_ram, sp_vc, sp_yarn_ram = [], [], [], [], []

    for r in rows:
        ep = r["epoch"]
        if mr_s <= ep <= mr_e:
            mr_t.append(ep - mr_s)
            mr_cpu.append(r["cpu"])
            mr_ram.append(r["ram_gb"])
            mr_vc.append(r["mr_vcores"])
            mr_yarn_ram.append(r["mr_ram_gb"])
        if sp_s <= ep <= sp_e:
            sp_t.append(ep - sp_s)
            sp_cpu.append(r["cpu"])
            sp_ram.append(r["ram_gb"])
            sp_vc.append(r["sp_vcores"])
            sp_yarn_ram.append(r["sp_ram_gb"])

    if not mr_t or not sp_t:
        print("[!] Thiếu dữ liệu điểm đo trong khoảng thời gian của 2 pha.")
        return

    mr_dur = mr_e - mr_s
    sp_dur = sp_e - sp_s

    # Tạo figure 2x2
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.suptitle(f"BẢNG SO SÁNH TRỰC DIỆN (GỘP TỪ VẠCH XUẤT PHÁT t = 0)\nMapReduce ({mr_dur}s) vs Apache Spark ({sp_dur}s)", 
                 fontsize=15, fontweight="bold")

    # ==================== 1. CPU tổng server (%) ====================
    ax1 = axes[0][0]
    ax1.plot(mr_t, mr_cpu, color="#e67e22", linewidth=2.0, label=f"MapReduce (Tổng {mr_dur}s)")
    ax1.plot(sp_t, sp_cpu, color="#27ae60", linewidth=2.0, label=f"Apache Spark (Tổng {sp_dur}s)")
    ax1.fill_between(mr_t, mr_cpu, color="#e67e22", alpha=0.15)
    ax1.fill_between(sp_t, sp_cpu, color="#27ae60", alpha=0.15)
    ax1.set_title("1. So sánh CPU tổng thể server (% theo thời gian chạy)", fontsize=12, fontweight="bold")
    ax1.set_ylabel("CPU (%)", fontsize=11)
    ax1.set_xlabel("Thời gian từ lúc bắt đầu tác vụ (giây)", fontsize=11)
    ax1.set_ylim(0, 100)
    ax1.legend(loc="upper right", framealpha=0.9)
    ax1.grid(True, linestyle="--", alpha=0.4)

    # ==================== 2. RAM tổng server (GB) ====================
    ax2 = axes[0][1]
    ax2.plot(mr_t, mr_ram, color="#e67e22", linewidth=2.0, label="MapReduce RAM server")
    ax2.plot(sp_t, sp_ram, color="#27ae60", linewidth=2.0, label="Apache Spark RAM server")
    ax2.fill_between(mr_t, mr_ram, color="#e67e22", alpha=0.15)
    ax2.fill_between(sp_t, sp_ram, color="#27ae60", alpha=0.15)
    ax2.set_title("2. So sánh RAM sử dụng của server (GB theo thời gian chạy)", fontsize=12, fontweight="bold")
    ax2.set_ylabel("RAM sử dụng (GB)", fontsize=11)
    ax2.set_xlabel("Thời gian từ lúc bắt đầu tác vụ (giây)", fontsize=11)
    ax2.legend(loc="lower right", framealpha=0.9)
    ax2.grid(True, linestyle="--", alpha=0.4)

    # ==================== 3. GỘP (3 + 5): YARN CPU vCores ====================
    ax3 = axes[1][0]
    ax3.plot(mr_t, mr_vc, color="#e67e22", linewidth=2.5, marker="o", markersize=3, label=f"MapReduce vCores (Đỉnh {max(mr_vc)})")
    ax3.plot(sp_t, sp_vc, color="#27ae60", linewidth=2.5, marker="s", markersize=3, label=f"Apache Spark vCores (Đỉnh {max(sp_vc)})")
    ax3.fill_between(mr_t, mr_vc, color="#e67e22", alpha=0.2)
    ax3.fill_between(sp_t, sp_vc, color="#27ae60", alpha=0.2)
    ax3.set_title("3. GỘP (3 + 5): So sánh trực diện YARN CPU cấp phát (vCores)", fontsize=12, fontweight="bold")
    ax3.set_ylabel("vCores được cấp", fontsize=11)
    ax3.set_xlabel("Thời gian từ lúc bắt đầu tác vụ (giây)", fontsize=11)
    ax3.set_ylim(bottom=0, top=max(max(mr_vc), max(sp_vc), 5) + 1.2)
    ax3.legend(loc="upper right", framealpha=0.9)
    ax3.grid(True, linestyle="--", alpha=0.4)

    # ==================== 4. GỘP (4 + 6): YARN RAM (GB) ====================
    ax4 = axes[1][1]
    ax4.plot(mr_t, mr_yarn_ram, color="#d35400", linewidth=2.5, marker="o", markersize=3, label=f"MapReduce RAM (Đỉnh {max(mr_yarn_ram):.1f} GB)")
    ax4.plot(sp_t, sp_yarn_ram, color="#16a085", linewidth=2.5, marker="s", markersize=3, label=f"Apache Spark RAM (Đỉnh {max(sp_yarn_ram):.1f} GB)")
    ax4.fill_between(mr_t, mr_yarn_ram, color="#d35400", alpha=0.2)
    ax4.fill_between(sp_t, sp_yarn_ram, color="#16a085", alpha=0.2)
    ax4.set_title("4. GỘP (4 + 6): So sánh trực diện YARN RAM cấp phát (GB)", fontsize=12, fontweight="bold")
    ax4.set_ylabel("RAM cấp phát (GB)", fontsize=11)
    ax4.set_xlabel("Thời gian từ lúc bắt đầu tác vụ (giây)", fontsize=11)
    ax4.set_ylim(bottom=0, top=max(max(mr_yarn_ram), max(sp_yarn_ram), 8) + 1.5)
    ax4.legend(loc="upper right", framealpha=0.9)
    ax4.grid(True, linestyle="--", alpha=0.4)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(out_png, dpi=150)
    print(f"✓ Đã lưu biểu đồ GỘP SO SÁNH TRỰC DIỆN (t=0): {out_png}")


def plot_timeline_chart(rows, phases_list, out_png, tick_minutes=5):
    """Vẽ biểu đồ toàn cảnh theo thời gian thực (wall-clock datetime)."""
    t = [datetime.fromtimestamp(r["epoch"]) for r in rows]
    cpus = [r["cpu"] for r in rows]
    rams = [r["ram_gb"] for r in rows]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 8))
    fig.suptitle("Toàn cảnh tài nguyên theo thời gian thực tế (Wall-Clock Timeline)", fontsize=14, fontweight="bold")

    colors = {"MapReduce": "#e67e22", "Spark": "#2ecc71"}
    for name, s_ep, e_ep in phases_list:
        s_dt = datetime.fromtimestamp(s_ep)
        e_dt = datetime.fromtimestamp(e_ep)
        for ax in (ax1, ax2):
            ax.axvspan(s_dt, e_dt, alpha=0.12, color=colors.get(name, "#95a5a6"))
            mid = s_dt + (e_dt - s_dt) / 2
            ax.text(mid, 0.90, name, transform=ax.get_xaxis_transform(),
                    ha="center", va="top", fontsize=10, fontweight="bold",
                    color=colors.get(name, "#d35400"))

    # CPU
    ax1.plot(t, cpus, color="#e74c3c", linewidth=1.5)
    ax1.fill_between(t, cpus, alpha=0.2, color="#e74c3c")
    ax1.set_title("1. CPU tổng thể server (%)")
    ax1.set_ylabel("CPU (%)")
    ax1.set_ylim(0, 100)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    # RAM
    ax2.plot(t, rams, color="#2980b9", linewidth=1.5)
    ax2.fill_between(t, rams, alpha=0.2, color="#2980b9")
    ax2.set_title("2. RAM tổng thể server (GB)")
    ax2.set_ylabel("RAM (GB)")
    ax2.set_xlabel("Thời gian")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(out_png, dpi=150)
    print(f"✓ Đã lưu biểu đồ dòng thời gian: {out_png}")


def plot_disk_io(rows, phases_list, out_disk_png, tick_minutes=5):
    """Vẽ riêng 2 đồ thị về Disk IOPS và Throughput MB/s."""
    t = [datetime.fromtimestamp(r["epoch"]) for r in rows]
    r_iops = [r["disk_read_iops"] for r in rows]
    w_iops = [r["disk_write_iops"] for r in rows]
    r_mb = [r["disk_read_mb"] for r in rows]
    w_mb = [r["disk_write_mb"] for r in rows]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 9))
    fig.suptitle("Giám sát Disk I/O: MapReduce vs Spark", fontsize=15, fontweight="bold")

    colors = {"MapReduce": "#e67e22", "Spark": "#2ecc71"}
    for name, s_ep, e_ep in phases_list:
        s_dt = datetime.fromtimestamp(s_ep)
        e_dt = datetime.fromtimestamp(e_ep)
        for ax in (ax1, ax2):
            ax.axvspan(s_dt, e_dt, alpha=0.12, color=colors.get(name, "#95a5a6"))
            mid = s_dt + (e_dt - s_dt) / 2
            ax.text(mid, 0.90, name, transform=ax.get_xaxis_transform(),
                    ha="center", va="top", fontsize=10, fontweight="bold",
                    color=colors.get(name, "#d35400"))

    # 1. IOPS
    ax1.plot(t, r_iops, label="Read IOPS", color="#2980b9", linewidth=1.5)
    ax1.plot(t, w_iops, label="Write IOPS", color="#e74c3c", linewidth=1.5, alpha=0.85)
    ax1.fill_between(t, w_iops, color="#e74c3c", alpha=0.15)
    ax1.set_title("1. Tần suất thao tác đĩa (Disk IOPS: Read vs Write)")
    ax1.set_ylabel("IOPS (lượt/giây)")
    ax1.legend(loc="upper right")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    # 2. Throughput
    ax2.plot(t, r_mb, label="Read MB/s", color="#3498db", linewidth=1.5)
    ax2.plot(t, w_mb, label="Write MB/s", color="#d35400", linewidth=1.5, alpha=0.85)
    ax2.fill_between(t, w_mb, color="#d35400", alpha=0.15)
    ax2.set_title("2. Băng thông đọc/ghi đĩa thực tế (Disk Throughput MB/s)")
    ax2.set_ylabel("Tốc độ (MB/s)")
    ax2.set_xlabel("Thời gian")
    ax2.legend(loc="upper right")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(out_disk_png, dpi=150)
    print(f"✓ Đã lưu biểu đồ Disk I/O: {out_disk_png}")


def main():
    if len(sys.argv) < 2:
        print("Cách dùng: python3 plot_monitor.py <monitor.csv> [phases.csv] [output.png]")
        sys.exit(1)

    csv_file = sys.argv[1]
    phases_file = None
    out_png = "resource_chart.png"
    tick_minutes = 5

    for arg in sys.argv[2:]:
        if arg.startswith("--tick="):
            try:
                tick_minutes = int(arg.split("=", 1)[1])
            except ValueError:
                pass
        elif arg.endswith(".png"):
            out_png = arg
        else:
            phases_file = arg

    rows = read_raw_rows(csv_file)
    if not rows:
        print("Không có dữ liệu trong CSV.")
        sys.exit(1)

    out_dir = os.path.dirname(out_png) or "."
    phases_dict = read_phases(phases_file) if phases_file else {}
    phases_list = []
    if phases_dict:
        for name, (s, e) in phases_dict.items():
            phases_list.append((name, s, e))

    # 1. BIỂU ĐỒ GỘP THỰC SỰ (Overlay Dashboard 2x2 từ t=0)
    plot_overlay_dashboard(rows, phases_dict, out_png)

    # 2. BIỂU ĐỒ DÒNG THỜI GIAN (Timeline Chart)
    timeline_png = os.path.join(out_dir, "timeline_chart.png")
    plot_timeline_chart(rows, phases_list, timeline_png, tick_minutes)

    # 3. BIỂU ĐỒ DISK I/O (nếu có cột disk IOPS)
    if rows and rows[0]["disk_read_iops"] is not None:
        disk_png = os.path.join(out_dir, "disk_io_chart.png")
        plot_disk_io(rows, phases_list, disk_png, tick_minutes)


if __name__ == "__main__":
    main()
