import os, re, time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import unidecode
from requests.adapters import HTTPAdapter, Retry

def clean_text(s):
    if not isinstance(s, str):
        return s
    s = re.sub(r"\[.*?\]", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    s = s.replace("\xa0", " ").strip()
    return s

def normalize_dash(s):
    return s.replace("−", "-").replace("–", "-").replace("—", "-")

def season_start_year(s):
    s = normalize_dash(str(s))
    m = re.match(r"(\d{4})-(\d{2})", s)
    return int(m.group(1)) if m else -1

def last_5_seasons():
    y = 2024
    return [f"{y - i}–{(y - i + 1) % 100:02d}" for i in range(5)]

def make_club_id(name):
    if not isinstance(name, str):
        return None
    s = unidecode.unidecode(name)
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", "_", s.strip().lower())
    return f"club_{s}"

def _http_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.8, status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

SESSION = _http_session()
BASE_URL = "https://en.wikipedia.org/wiki/{}"

def _find_stadia_table(soup):
    h = soup.find(id=re.compile(r"Stadia_and_locations|Stadiums_and_locations", re.I))
    if h:
        sec = h.find_parent(["h2","h3"])
        if sec:
            tbl = sec.find_next("table", class_="wikitable")
            if tbl:
                return tbl
    for t in soup.find_all("table", class_="wikitable"):
        text = t.get_text(" ", strip=True)
        if re.search(r"(Club|Team|Participant)", text, re.I) and re.search(r"(Stadium|Ground)", text, re.I):
            return t
    return None

def get_table_for_season(season):
    season_std = normalize_dash(season).replace("-", "–")
    url = BASE_URL.format(f"{season_std}_Premier_League")
    try:
        res = SESSION.get(url, timeout=20)
        if res.status_code == 404:
            return None
        res.raise_for_status()
    except:
        return None

    soup = BeautifulSoup(res.text, "html.parser")
    table = _find_stadia_table(soup)
    if not table:
        return None

    try:
        df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]
    except:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join(clean_text(str(c)) for c in tup if c) for tup in df.columns]
    else:
        df.columns = [clean_text(str(c)) for c in df.columns]

    keep = []
    keep += [c for c in df.columns if re.search(r"(Club|Team|Participant)", c, re.I)]
    keep += [c for c in df.columns if re.search(r"(Stadium|Ground)", c, re.I)]
    keep += [c for c in df.columns if re.search(r"(Location|City|Town)", c, re.I)]
    keep = list(dict.fromkeys(keep))

    if not keep:
        return None

    df = df[[c for c in keep if c in df.columns]].copy()
    rename_map = {}
    for c in df.columns:
        if re.search(r"(Club|Team|Participant)", c, re.I): rename_map[c] = "Club"
        elif re.search(r"(Stadium|Ground)", c, re.I): rename_map[c] = "Stadium"
        elif re.search(r"(Location|City|Town)", c, re.I): rename_map[c] = "Location"
    df.rename(columns=rename_map, inplace=True)

    for col in ["Club", "Stadium", "Location"]:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(clean_text)

    df["Season"] = season_std
    return df

def basic_club_filter(df):
    if "Club" not in df.columns:
        return df
    banned = re.compile(r"(Women|Ladies|U\d{2}|Academy|Reserves|Development|Youth|Under|Girls|WFC)", re.I)
    df = df[~df["Club"].str.contains(banned, na=False)]
    return df

def main():
    seasons = last_5_seasons()
    all_dfs = []

    for season in seasons:
        df = get_table_for_season(season)
        if df is not None:
            df = basic_club_filter(df)
            all_dfs.append(df)
        time.sleep(1)

    if not all_dfs:
        return

    merged = pd.concat(all_dfs, ignore_index=True)
    merged["Club"] = merged["Club"].astype(str).str.strip()

    base_dir = os.path.join("..", "data")
    node_dir = os.path.join(base_dir, "nodes")
    rel_dir  = os.path.join(base_dir, "relations")
    os.makedirs(node_dir, exist_ok=True)
    os.makedirs(rel_dir, exist_ok=True)

    merged["club_id"] = merged["Club"].apply(make_club_id)
    rel_path = os.path.join(rel_dir, "clubs_by_season.csv")
    merged[["club_id", "Club", "Season"]].to_csv(rel_path, index=False, encoding="utf-8-sig")

    latest = merged.sort_values(by="Season", key=lambda s: s.map(season_start_year), ascending=False)
    final_df = latest.drop_duplicates(subset=["Club"], keep="first")
    final_df["club_id"] = final_df["Club"].apply(make_club_id)

    node_cols = ["club_id", "Club", "Location", "Stadium"]
    node_df = final_df[node_cols]

    node_path = os.path.join(node_dir, "clubs.csv")
    node_df.to_csv(node_path, index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
