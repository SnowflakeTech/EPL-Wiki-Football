#!/usr/bin/env python
# coding: utf-8

import os, re, time, requests, pandas as pd
from bs4 import BeautifulSoup

# =========================
# Cấu hình
# =========================
DATA_DIR = "../data/node"
OUTPUT_CSV = os.path.join(DATA_DIR, "seasons.csv")
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Danh sách 5 mùa gần nhất (hoặc có thể mở rộng)
SEASONS = [
    "2024–25",
    "2023–24",
    "2022–23",
    "2021–22",
    "2020–21",
]

BASE_URL = "https://en.wikipedia.org/wiki/{}"

def now(): return time.strftime("[%H:%M:%S]")

# =========================
# Crawl thông tin mùa giải
# =========================
def get_season_info(season_str):
    """Lấy thông tin 1 mùa EPL từ Wikipedia"""
    url = BASE_URL.format(f"{season_str}_Premier_League")
    print(f"\n🟦─── {season_str} ────────────────────────────────────────────────")
    print(f"{now()} 🌐 URL: {url}")

    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        res.raise_for_status()
    except Exception as e:
        print(f"{now()} ⚠️ Không tải được {season_str}: {e}")
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    # Lấy tiêu đề
    title_tag = soup.find("h1")
    title = title_tag.text.strip() if title_tag else f"{season_str} Premier League"

    # Tách năm
    years = re.findall(r"(\d{4})", season_str)
    start_year, end_year = None, None
    if len(years) == 1:
        start_year = int(years[0])
        end_year = start_year + 1
    elif len(years) == 2:
        start_year, end_year = int(years[0]), int(years[1])

    season_id = f"EPL-{season_str}"

    print(f"{now()} ✅ {title} ({start_year}–{end_year})")

    return {
        "season_id": season_id,
        "name": title,
        "start_year": start_year,
        "end_year": end_year,
        "url": url
    }

# =========================
# Main
# =========================
def main():
    print(f"\n📘 Crawl thông tin {len(SEASONS)} mùa EPL gần nhất...\n")
    all_seasons = []

    for s in SEASONS:
        info = get_season_info(s)
        if info:
            all_seasons.append(info)
        time.sleep(1)

    if not all_seasons:
        print("❗ Không có dữ liệu mùa nào được crawl.")
        return

    df = pd.DataFrame(all_seasons)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n✅ seasons.csv → {len(df)} mùa được xuất thành công!\n")

if __name__ == "__main__":
    main()
