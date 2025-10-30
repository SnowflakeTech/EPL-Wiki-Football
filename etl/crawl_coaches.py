import os, re, time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import unidecode

# =============================
# Cấu hình
# =============================
HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://en.wikipedia.org/wiki/{}"

BASE_DIR = "../data"
NODE_DIR = os.path.join(BASE_DIR, "nodes")
REL_DIR = os.path.join(BASE_DIR, "relations")
os.makedirs(NODE_DIR, exist_ok=True)
os.makedirs(REL_DIR, exist_ok=True)

def now():
    return time.strftime("[%H:%M:%S]")

def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"\[.*?\]", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    s = s.replace("\xa0", " ").strip()
    return s

def make_id(prefix, name):
    if not name:
        return None
    s = unidecode.unidecode(name)
    s = re.sub(r"[^a-zA-Z0-9\s-]", " ", s)
    s = re.sub(r"\s+", "_", s.strip().lower())
    return f"{prefix}_{s}"

# ---------- Helpers để bóc tách tên/years từ ô nhiều dòng ----------
COUNTRY_TOKEN = re.compile(r"^[A-Z]{2,4}$")  # ENG, IRE, SCO, etc.
DATEISH = re.compile(r"\d")                  # chứa số: ngày/tháng/năm
PCTISH = re.compile(r"\d+\.\d+%?$")          # 44.34, 60.63, v.v.

def _first_scalar(x):
    """Trả về một scalar string từ nhiều kiểu khác nhau (Series/list/scalar)."""
    if isinstance(x, (list, tuple)):
        return clean_text(str(x[0])) if x else ""
    if isinstance(x, pd.Series):
        # lấy phần tử đầu tiên của series
        try:
            return clean_text(str(x.iloc[0]))
        except Exception:
            return clean_text(str(x))
    return clean_text(str(x))

def _split_lines(s):
    """Tách theo xuống dòng/br; lọc rỗng."""
    if not isinstance(s, str):
        s = clean_text(str(s))
    parts = re.split(r"[\r\n]+|<br\s*/?>", s, flags=re.I)
    return [clean_text(p) for p in parts if clean_text(p)]

def _looks_like_name(token):
    """Heuristic: coi token là tên nếu có chữ cái và không phải mã quốc tịch/thuần số."""
    if not token: 
        return False
    if COUNTRY_TOKEN.match(token): 
        return False
    # loại bỏ dòng toàn số liệu hoặc phần trăm
    if PCTISH.match(token): 
        return False
    # có ít nhất 1 chữ cái
    return re.search(r"[A-Za-z]", token) is not None

def extract_coach_name(cell):
    """Lấy đúng 'tên' từ ô có thể chứa nhiều dòng (tên + quốc tịch + ngày + stats)."""
    v = _first_scalar(cell)
    lines = _split_lines(v)
    # Ưu tiên dòng đầu thỏa điều kiện name
    for t in lines:
        t2 = re.sub(r"\s{2,}", " ", t).strip(" ,;")
        if _looks_like_name(t2):
            return t2
    # fallback: toàn bộ chuỗi nhưng rút gọn tới trước 2 dòng đầu
    return lines[0] if lines else v

def extract_years(cell):
    """Lấy trường years (thường là cột 'Years/Dates/Tenure' hoặc ghép ô)."""
    v = _first_scalar(cell)
    lines = _split_lines(v)
    # chọn dòng chứa số (thường là ngày/tháng/năm hoặc mốc năm)
    for t in lines:
        if DATEISH.search(t):
            return t
    # fallback: trả về chuỗi rút gọn 1 dòng
    return lines[0] if lines else v

# =============================
# Lấy thông tin HLV
# =============================
def get_coach_history(club_name, club_id, season):
    special_cases = {
        "Bournemouth": "AFC_Bournemouth",
        "Brighton & Hove Albion": "Brighton_&_Hove_Albion_F.C.",
        "Wolverhampton Wanderers": "Wolverhampton_Wanderers_F.C.",
        "Newcastle United": "Newcastle_United_F.C.",
        "Luton Town": "Luton_Town_F.C.",
        "West Ham United": "West_Ham_United_F.C."
    }

    name_for_url = special_cases.get(club_name, club_name)
    base_title = name_for_url.replace(" ", "_")
    if not base_title.endswith("_F.C.") and "AFC" not in base_title:
        base_title += "_F.C."
    url = BASE_URL.format(base_title)

    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        res.raise_for_status()
    except Exception as e:
        print(f"{now()} ⚠️ Không tải được {club_name}: {e}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    # 1) Lấy HLV hiện tại từ infobox
    infobox = soup.find("table", class_=re.compile("infobox", re.I))
    current_coach = None
    if infobox:
        for tr in infobox.find_all("tr"):
            th = tr.find("th")
            if th and ("manager" in th.text.lower() or "head coach" in th.text.lower()):
                td = tr.find("td")
                if td:
                    current_coach = clean_text(td.get_text("\n"))
                break

    # 2) Tìm bảng danh sách HLV
    header = soup.find(["h2", "h3"], string=re.compile(r"Managerial|Managers", re.I))
    table = None
    if header:
        table = header.find_next("table", class_=re.compile("wikitable", re.I))

    if not table:
        list_title = f"List_of_{base_title}_managers"
        list_url = BASE_URL.format(list_title)
        try:
            res2 = requests.get(list_url, headers=HEADERS, timeout=20)
            res2.raise_for_status()
            soup2 = BeautifulSoup(res2.text, "html.parser")
            tables = soup2.find_all("table", class_=re.compile("wikitable", re.I))
            for t in tables:
                text = t.get_text(" ", strip=True)
                if re.search(r"Manager|Head coach|Name", text, re.I):
                    if table is None or len(t.find_all("tr")) > len(table.find_all("tr")):
                        table = t
            if not table and tables:
                table = max(tables, key=lambda t: len(t.find_all("tr")))
        except:
            pass

    # 3) Nếu không có bảng thì chỉ lưu HLV hiện tại (nếu có)
    if not table:
        if current_coach:
            name_clean = extract_coach_name(current_coach)
            return [{
                "coach_id": make_id("coach", name_clean),
                "name": name_clean,
                "club_id": club_id,
                "club_name": club_name,
                "season": season,
                "years": "",
                "is_current": True
            }]
        return []

    # 4) Đọc bảng
    try:
        df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]
    except Exception:
        return []

    # Nếu MultiIndex columns → flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([clean_text(str(x)) for x in tup if x]).strip() for tup in df.columns]
    else:
        df.columns = [clean_text(str(c)) for c in df.columns]

    # Xác định cột chứa tên/years
    name_col = next((c for c in df.columns if re.search(r"(Manager|Head coach|Coach|Name)", c, re.I)), df.columns[min(1, len(df.columns)-1)])
    year_col = next((c for c in df.columns if re.search(r"(Year|From|To|Dates|Tenure|Period|Season)", c, re.I)), df.columns[0])

    rows = []
    for _, r in df.iterrows():
        raw_name = r.get(name_col, "")
        raw_years = r.get(year_col, "")

        name = extract_coach_name(raw_name)
        if not name:
            continue
        years = extract_years(raw_years)

        rows.append({
            "coach_id": make_id("coach", name),
            "name": name,
            "club_id": club_id,
            "club_name": club_name,
            "season": season,
            "years": years,
            "is_current": False
        })

    # 5) Gắn cờ is_current
    if rows:
        if current_coach:
            current_norm = extract_coach_name(current_coach).lower()
            matched = False
            for r in rows:
                if current_norm in r["name"].lower() or r["name"].lower() in current_norm:
                    r["is_current"] = True
                    matched = True
                    break
            if not matched:
                # nếu không match tên thì mặc định đánh dấu người cuối cùng
                rows[-1]["is_current"] = True
        else:
            rows[-1]["is_current"] = True
    elif current_coach:
        name_clean = extract_coach_name(current_coach)
        rows.append({
            "coach_id": make_id("coach", name_clean),
            "name": name_clean,
            "club_id": club_id,
            "club_name": club_name,
            "season": season,
            "years": "",
            "is_current": True
        })

    return rows

# =============================
# Main
# =============================
def main():
    clubs_csv = os.path.join(NODE_DIR, "clubs.csv")
    if not os.path.exists(clubs_csv):
        raise FileNotFoundError("⚠️ Thiếu file clubs.csv")

    clubs_df = pd.read_csv(clubs_csv)
    all_rows = []

    print(f"\n🏟️  Bắt đầu crawl danh sách HLV cho {len(clubs_df)} CLB...\n")
    for _, row in clubs_df.iterrows():
        club_name = row["Club"]
        club_id = row["club_id"]
        print(f"{now()} ⏳ {club_name}")
        coaches = get_coach_history(club_name, club_id, "2024–25")
        all_rows.extend(coaches)
        time.sleep(1)

    if not all_rows:
        print("❌ Không có dữ liệu HLV nào được lấy!")
        return

    df = pd.DataFrame(all_rows)

    # Làm sạch cuối:
    df["name"] = df["name"].astype(str).apply(lambda s: re.sub(r"\s{2,}", " ", s).strip())
    df["years"] = df["years"].fillna("").astype(str).apply(lambda s: re.sub(r"\s{2,}", " ", s).strip())
    df["is_current"] = df["is_current"].fillna(False).astype(bool)

    # Node: coaches.csv
    coaches_df = df.drop_duplicates(subset=["coach_id"])[["coach_id", "name"]]
    coaches_path = os.path.join(NODE_DIR, "coaches.csv")
    coaches_df.to_csv(coaches_path, index=False, encoding="utf-8-sig")

    # Edge: coached.csv
    coached_df = df[["coach_id", "club_id", "season", "years", "is_current"]]
    coached_path = os.path.join(REL_DIR, "coached.csv")
    coached_df.to_csv(coached_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ coaches.csv & coached.csv được tạo thành công!\n")

if __name__ == "__main__":
    main()
