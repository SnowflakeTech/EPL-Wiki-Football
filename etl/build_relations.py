import os
import pandas as pd

BASE_DIR = "../data"
NODE_DIR = os.path.join(BASE_DIR, "nodes")
REL_DIR = os.path.join(BASE_DIR, "relations")
EDGE_DIR = os.path.join(BASE_DIR, "edges")
os.makedirs(EDGE_DIR, exist_ok=True)

# ===============================================================
# 1️⃣ QUAN HỆ: PART_OF (Club → Season)
# ===============================================================
def build_part_of():
    src = os.path.join(REL_DIR, "clubs_by_season.csv")
    if not os.path.exists(src):
        print("⚠️  Thiếu file clubs_by_season.csv")
        return

    df = pd.read_csv(src)
    df[":START_ID(Club)"] = df["club_id"]
    # ID mùa giải phải trùng với season_id trong seasons.csv: EPL-2024–25
    df[":END_ID(Season)"] = df["Season"].apply(lambda s: f"EPL-{s}")
    df = df[[":START_ID(Club)", ":END_ID(Season)", "Season"]]
    df[":TYPE"] = "PART_OF"

    out = os.path.join(EDGE_DIR, "part_of.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ PART_OF → {len(df)} dòng được xuất.")


# ===============================================================
# 2️⃣ QUAN HỆ: PLAYED_FOR (Player → Club)
# ===============================================================
def build_played_for():
    src = os.path.join(REL_DIR, "played_for.csv")
    if not os.path.exists(src):
        print("⚠️  Thiếu file played_for.csv")
        return

    df = pd.read_csv(src)

    # Đảm bảo có đủ cột
    for col in ["player_id", "club_id", "season"]:
        if col not in df.columns:
            raise ValueError(f"❌ Thiếu cột '{col}' trong played_for.csv")

    df[":START_ID(Player)"] = df["player_id"]
    df[":END_ID(Club)"] = df["club_id"]

    # Chuyển season → season_id khớp với node Season
    df["season_id"] = df["season"].apply(lambda s: f"EPL-{s}" if isinstance(s, str) else None)

    df = df[[":START_ID(Player)", ":END_ID(Club)", "season_id", "position"]]
    df[":TYPE"] = "PLAYED_FOR"

    out = os.path.join(EDGE_DIR, "played_for.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ PLAYED_FOR → {len(df)} dòng được xuất.")


# ===============================================================
# 3️⃣ QUAN HỆ: COACHED (Coach → Club)
# ===============================================================
def build_coached():
    src = os.path.join(REL_DIR, "coached.csv")
    if not os.path.exists(src):
        print("⚠️  Thiếu file coached.csv")
        return

    df = pd.read_csv(src)

    # Đảm bảo có đủ cột
    for col in ["coach_id", "club_id", "season"]:
        if col not in df.columns:
            raise ValueError(f"❌ Thiếu cột '{col}' trong coached.csv")

    df[":START_ID(Coach)"] = df["coach_id"]
    df[":END_ID(Club)"] = df["club_id"]

    # Chuẩn hóa season_id
    df["season_id"] = df["season"].apply(lambda s: f"EPL-{s}" if isinstance(s, str) else None)

    df = df[[":START_ID(Coach)", ":END_ID(Club)", "season_id", "years", "is_current"]]
    df[":TYPE"] = "COACHED"

    out = os.path.join(EDGE_DIR, "coached.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ COACHED → {len(df)} dòng được xuất.")


# ===============================================================
# MAIN ENTRY
# ===============================================================
def main():
    print("\n🏗️  Bắt đầu tạo các file quan hệ cho Neo4j...")
    build_part_of()
    build_played_for()
    build_coached()
    print("\n🎯 Tất cả file edges được tạo thành công trong thư mục ../data/edges/\n")


if __name__ == "__main__":
    main()
