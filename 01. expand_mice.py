# expand_mice.py
import pandas as pd, numpy as np
from faker import Faker
from datetime import timedelta, date
from pathlib import Path

# === 설정 ===
SRC = "산업통상자원부_전시사업(국내_전시회)_20231231.csv"
TARGET_N = 1000
OUT = "expanded_mice.csv"
POSSIBLE_ENCODINGS = ["euc-kr", "cp949", "utf-8-sig", "utf-8"]

# === 로드(인코딩 자동 탐색) ===
used_enc = None
df = None
for enc in POSSIBLE_ENCODINGS:
    try:
        df = pd.read_csv(SRC, encoding=enc)
        used_enc = enc
        break
    except Exception:
        continue
if df is None:
    raise RuntimeError("CSV 읽기 실패. 인코딩을 수동으로 확인하라.")

# === 기본 프로파일 ===
print(f"[load] encoding={used_enc}  shape={df.shape}")
print("[cols]", list(df.columns))

# 예상 컬럼명
req_cols = ['순번','전시회명','주최기관','전시시작일','전시종료일','전시장소',
            '총전시면적','참가업체','참가업체_해외','참관객','참관객_해외','참관객_해외바이어']
miss = [c for c in req_cols if c not in df.columns]
if miss:
    raise ValueError(f"필수 컬럼 누락: {miss}")

num_cols = ['총전시면적','참가업체','참가업체_해외','참관객','참관객_해외','참관객_해외바이어']

# 수치 범위 사전
num_minmax = {}
for c in num_cols:
    s = pd.to_numeric(df[c], errors='coerce')
    num_minmax[c] = (int(s.min()) if pd.notna(s.min()) else 0,
                     int(s.max()) if pd.notna(s.max()) else 0)
print("[ranges]", num_minmax)

# 날짜 범위
starts = pd.to_datetime(df['전시시작일'], errors='coerce')
start_min = starts.min()
start_max = starts.max()
# 안전한 기본값
if pd.isna(start_min): start_min = pd.Timestamp("2023-01-01")
if pd.isna(start_max): start_max = pd.Timestamp("2024-12-31")
print(f"[date_range] {start_min.date()} ~ {start_max.date()}")

# === 생성 준비 ===
orig_n = len(df)
need = TARGET_N - orig_n
if need <= 0:
    print(f"[skip] 이미 {orig_n}행 이상. TARGET_N={TARGET_N}")
    need = 0

np.random.seed(42)
Faker.seed(42)
fake = Faker("ko_KR")

venues = [v for v in df['전시장소'].dropna().unique()]
if not venues:
    venues = ["코엑스", "킨텍스", "벡스코", "세텍"]

def rint(lo, hi):
    lo, hi = int(lo), int(hi)
    if hi < lo: hi = lo
    return np.random.randint(lo, hi + 1)

def gen_event_name():
    return f"{fake.city()} {fake.job()} {fake.random_element(['엑스포','박람회','전시회','페어'])} {fake.random_int(2023, 2024)}"

def gen_org():
    return f"{fake.random_element(['(사)','(재)','(주)','협회','조합'])} {fake.company()}"

# === 행 생성 ===
rows = []
for _ in range(need):
    # 날짜: 종료일 >= 시작일, 기간 1~5일
    start = fake.date_between(start_date=start_min.date(), end_date=start_max.date())
    period = fake.random_int(1, 5)
    end = start + timedelta(days=period)

    # 수치 값
    area = rint(*num_minmax['총전시면적'])
    exhibitors = rint(*num_minmax['참가업체'])
    ex_overseas = np.random.randint(0, max(1, exhibitors + 1))

    visitors = rint(*num_minmax['참관객'])
    vis_overseas = np.random.randint(0, max(1, visitors + 1))
    vis_buyers = np.random.randint(0, max(1, vis_overseas + 1))

    rows.append({
        '순번': None,
        '전시회명': gen_event_name(),
        '주최기관': gen_org(),
        '전시시작일': pd.Timestamp(start).date(),
        '전시종료일': pd.Timestamp(end).date(),
        '전시장소': np.random.choice(venues),
        '총전시면적': area,
        '참가업체': exhibitors,
        '참가업체_해외': ex_overseas,
        '참관객': visitors,
        '참관객_해외': vis_overseas,
        '참관객_해외바이어': vis_buyers
    })

new_df = pd.DataFrame(rows, columns=req_cols) if need > 0 else pd.DataFrame(columns=req_cols)
out = pd.concat([df[req_cols], new_df], ignore_index=True)

# 순번 재부여 1..N
out['순번'] = range(1, len(out) + 1)

# === 검증 ===
s = pd.to_datetime(out['전시시작일'], errors='coerce')
e = pd.to_datetime(out['전시종료일'], errors='coerce')
bad_date = int((e < s).sum())

vis = pd.to_numeric(out['참관객'], errors='coerce').fillna(0).astype(int)
vis_o = pd.to_numeric(out['참관객_해외'], errors='coerce').fillna(0).astype(int)
buy = pd.to_numeric(out['참관객_해외바이어'], errors='coerce').fillna(0).astype(int)
exh = pd.to_numeric(out['참가업체'], errors='coerce').fillna(0).astype(int)
exh_o = pd.to_numeric(out['참가업체_해외'], errors='coerce').fillna(0).astype(int)

bad_logic = int(((vis_o > vis) | (buy > vis_o) | (exh_o > exh)).sum())

print(f"[gen] orig={orig_n}  add={need}  out={len(out)}")
print(f"[check] bad_date(rows with end<start)={bad_date}  bad_logic={bad_logic}")

# === 저장 ===
out.to_csv(OUT, index=False, encoding="utf-8-sig")
print(f"[save] {OUT}  encoding=utf-8-sig  shape={out.shape}")
