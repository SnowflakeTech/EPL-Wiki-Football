import os, re, time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import unidecode

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://en.wikipedia.org/wiki/{}"
SEASONS = ["2024–25", "2023–24", "2022–23", "2021–22", "2020–21"]

BASE_DIR = "../data"
NODE_DIR = os.path.join(BASE_DIR, "nodes")
REL_DIR = os.path.join(BASE_DIR, "relations")
os.makedirs(NODE_DIR, exist_ok=True)
os.makedirs(REL_DIR, exist_ok=True)

def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"\[.*?\]", "", s)
    return s.replace("\xa0", " ").strip()

def make_player_id(name):
    s = unidecode.unidecode(name)
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", "_", s.strip().lower())
    return f"player_{s}"

def get_players_from_club(club_name, club_id, season):
    special_cases = {
        "Bournemouth": "AFC Bournemouth",
        "Brighton & Hove Albion": "Brighton_&_Hove_Albion_F.C.",
        "Wolverhampton Wanderers": "Wolverhampton_Wanderers_F.C.",
        "Newcastle United": "Newcastle_United_F.C.",
        "Luton Town": "Luton_Town_F.C.",
        "West Ham United": "West_Ham_United_F.C."
    }
    name_for_url = special_cases.get(club_name, club_name)
    url = BASE_URL.format(name_for_url.replace(" ", "_") + ("" if "AFC" in name_for_url else "_F.C."))

    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        if res.status_code == 404 and "_F.C." in url:
            alt_url = url.replace("_F.C.", "")
            res = requests.get(alt_url, headers=HEADERS, timeout=20)
        res.raise_for_status()
    except:
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    header = (
        soup.find(["h2", "h3"], string=re.compile(r"(First[- ]?team|Current) squad", re.I))
        or soup.find(["h2", "h3"], string=re.compile(r"First[- ]?Team", re.I))
        or soup.find(["h2", "h3"], id=re.compile(r"First[\-_ ]?team[\-_ ]?(squad)?", re.I))
    )
    if not header:
        return []

    tables = []
    presentation_table = header.find_next("table", {"role": "presentation"})
    if presentation_table:
        inner_tables = presentation_table.find_all("table", class_=re.compile(r"(football-squad|wikitable)"))
        if inner_tables:
            tables.extend(inner_tables)

    if not tables:
        for t in header.find_all_next("table", class_=re.compile(r"(football-squad|wikitable)")):
            prev_header = t.find_previous(
                lambda tag: tag.name in ["h2", "h3"] and re.search(r"Out|loan|academy", tag.text, re.I)
            )
            if prev_header:
                break
            tables.append(t)
            nxt = t.find_next_sibling("table")
            if not (nxt and re.search(r"(football-squad|wikitable)", " ".join(nxt.get("class", [])), re.I)):
                break

    if not tables:
        return []

    dfs = []
    for t in tables:
        try:
            df = pd.read_html(StringIO(str(t)))[0]
            df.columns = [c.strip() for c in df.columns]
            dfs.append(df)
        except:
            continue
    if not dfs:
        return []

    df = pd.concat(dfs, ignore_index=True)
    rename_map = {}
    for c in df.columns:
        if "Player" in c or "Name" in c:
            rename_map[c] = "Name"
        elif "Nation" in c:
            rename_map[c] = "Nation"
        elif "Pos" in c or "Position" in c:
            rename_map[c] = "Position"
    df.rename(columns=rename_map, inplace=True)
    keep_cols = ["Name", "Nation", "Position"]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    players = []
    for _, row in df.iterrows():
        name = clean_text(row.get("Name", ""))
        nation = clean_text(row.get("Nation", ""))
        position = clean_text(row.get("Position", ""))
        if not name:
            continue
        players.append({
            "player_id": make_player_id(name),
            "name": name,
            "nation": nation,
            "position": position,
            "club_id": club_id,
            "season": season
        })
    return players

def main():
    clubs_csv = os.path.join(NODE_DIR, "clubs.csv")
    if not os.path.exists(clubs_csv):
        raise FileNotFoundError("Missing clubs.csv")

    clubs_df = pd.read_csv(clubs_csv)
    all_players = []
    for _, row in clubs_df.iterrows():
        club_name = row["Club"]
        club_id = row["club_id"]
        for season in SEASONS:
            players = get_players_from_club(club_name, club_id, season)
            if players:
                all_players.extend(players)
        time.sleep(1)

    if not all_players:
        return

    df = pd.DataFrame(all_players)
    players_df = df.drop_duplicates(subset=["player_id"])[["player_id", "name", "nation", "position"]]
    rel_df = df[["player_id", "club_id", "season", "position"]].drop_duplicates()

    players_df.to_csv(os.path.join(NODE_DIR, "players.csv"), index=False, encoding="utf-8-sig")
    rel_df.to_csv(os.path.join(REL_DIR, "played_for.csv"), index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
