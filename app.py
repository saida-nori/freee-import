# app.py
"""
楽楽精算→freee仕訳CSV 生成ツール（FastAPI 単一ファイル版）

v2.3 / 最終更新: 2025-10-02
- 入力は「②データ貼付」相当の CSV/Excel のみを解析
- AMEX / 経費 / 交通費 に自動振り分け
- 摘要（借方・貸方とも同一ルールで生成）:
    AMEX   : G列 + C列(月/日) + P列
    経費   : G列 + C列(月/日) + AM列 + P列
    交通費 : G列 + C列(月/日) + M列 + K列 + P列
- freee「他社会計ソフトインポート」形式で **複合仕訳** を出力
  （同一伝票番号で複数行，貸方は1行目に合計額，それ以外は0）
- 「設定（列名・貸方ルール）」画面の内部エラーを修正（.format 廃止）
- 画面にロゴ(/static/logo.png)・バージョン・使い方マニュアル(/manual)
"""

from fastapi import FastAPI, UploadFile, Request
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import pandas as pd
import io
import json
from pathlib import Path
from datetime import datetime
import zipfile
import re
import traceback

# ─────────────────────────────────────
# 基本
# ─────────────────────────────────────
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

APP_VERSION = "v2.3"
APP_DATE = datetime.now().strftime("%Y-%m-%d")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ─────────────────────────────────────
# 設定（列名・貸方ルール）
# ─────────────────────────────────────
DEFAULT_CONFIG = {
    "INPUT_SHEET": "②データ貼付",
    "OUTPUT_COLUMNS": [
        "伝票番号",
        "日付",
        "借方勘定科目", "借方補助科目", "借方部門", "借方税区分", "借方金額", "借方摘要",
        "貸方勘定科目", "貸方補助科目", "貸方部門", "貸方税区分", "貸方金額", "貸方摘要",
        "備考",
    ],
    "SRC_HEADERS": {
        "date": "日付",
        "account": "勘定科目名",
        "subaccount": "補助科目名",
        "dept": "負担部門(選択必須)",
        "tax": "税率",
        "amount": "小計",
        "memo": "自由記入欄",
        "pay_method": "支払方法",
        "card_brand": "カード",
        "ticket_type": "伝票種別",
    },
    "TAX_MAP": {
        "課対仕入込10%": "課対仕入10%",
        "課対仕入込軽減8%": "課対仕入8%_軽減",
        "対象外": "対象外",
        None: "対象外",
    },
    "CREDIT_RULES": {
        "amex":    {"貸方勘定科目": "未払金", "貸方補助科目": "AMEX",     "貸方部門": "本社", "貸方税区分": "対象外"},
        "keihi":   {"貸方勘定科目": "未払金", "貸方補助科目": "従業員立替", "貸方部門": "本社", "貸方税区分": "対象外"},
        "kotsuhi": {"貸方勘定科目": "未払金", "貸方補助科目": "従業員立替", "貸方部門": "本社", "貸方税区分": "対象外"},
    },
}

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return DEFAULT_CONFIG

def save_config(data: dict) -> None:
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

CONFIG = load_config()

# ─────────────────────────────────────
# 小物
# ─────────────────────────────────────
def normalize_tax(name: str):
    return CONFIG["TAX_MAP"].get(name, name)

def _coerce_str(s: pd.Series) -> pd.Series:
    return s.astype(str).replace({"nan": "", "NaT": ""}).fillna("")

def _read_csv_safely(file_bytes: bytes) -> pd.DataFrame:
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(file_bytes), encoding_errors="ignore")

def _read_base(file_bytes: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        return _read_csv_safely(file_bytes)
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    sheet = CONFIG.get("INPUT_SHEET", "②データ貼付")
    if sheet not in xls.sheet_names:
        cand = [s for s in xls.sheet_names if "データ貼" in s]
        sheet = cand[0] if cand else xls.sheet_names[0]
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet)

def _col_letter_to_idx(col: str) -> int:
    col = col.strip().upper()
    val = 0
    for ch in col:
        if "A" <= ch <= "Z":
            val = val * 26 + (ord(ch) - ord("A") + 1)
    return val - 1

def _pick_by_letter(row: pd.Series, letters: list[str]) -> list[str]:
    values = []
    for col in letters:
        idx = _col_letter_to_idx(col)
        v = row.iloc[idx] if 0 <= idx < len(row) else ""
        values.append("" if pd.isna(v) else str(v))
    return values

def _join_clean(parts: list[str], sep: str = " ") -> str:
    parts = [p for p in [p.strip() for p in parts] if p]
    return sep.join(parts)

def _format_mmdd(val) -> str:
    try:
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            m = re.search(r"(\d{1,2})[/-](\d{1,2})", str(val))
            if m:
                return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"
            return str(val)
        return dt.strftime("%m/%d")
    except Exception:
        return str(val)

# ─────────────────────────────────────
# ②データ貼付 → カテゴリ振り分け
# ─────────────────────────────────────
def split_categories(df: pd.DataFrame):
    h = CONFIG["SRC_HEADERS"]
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    amex = pd.Series(False, index=df.index)
    if h.get("pay_method") in df.columns:
        amex = amex | _coerce_str(df[h["pay_method"]]).str.contains("AMEX|アメックス", case=False, na=False)
    if h.get("card_brand") in df.columns:
        amex = amex | _coerce_str(df[h["card_brand"]]).str.contains("AMEX|アメックス", case=False, na=False)

    kotsu = pd.Series(False, index=df.index)
    if h.get("ticket_type") in df.columns:
        kotsu = _coerce_str(df[h["ticket_type"]]).str.contains("交通費", na=False)

    keihi = (~amex) & (~kotsu)

    return df[amex].copy(), df[keihi].copy(), df[kotsu & (~amex)].copy()

# ─────────────────────────────────────
# 摘要（行ごと）
# ─────────────────────────────────────
_MEMO_PARTS = {
    "amex":    ["G", "C", "P"],
    "keihi":   ["G", "C", "AM", "P"],
    "kotsuhi": ["G", "C", "M", "K", "P"],
}

def build_memo_series(df: pd.DataFrame, kind: str) -> pd.Series:
    parts = _MEMO_PARTS[kind]
    memos = []
    for _, row in df.reset_index(drop=True).iterrows():
        vals = _pick_by_letter(row, parts)
        for i, letter in enumerate(parts):
            if letter.upper() == "C":
                vals[i] = _format_mmdd(vals[i])
        memos.append(_join_clean(vals, " "))
    return pd.Series(memos, index=df.index)

# ─────────────────────────────────────
# 複合仕訳の生成
# ─────────────────────────────────────
def build_compound_voucher(df: pd.DataFrame, kind: str, voucher_id: str) -> pd.DataFrame:
    h = CONFIG["SRC_HEADERS"]
    memos = build_memo_series(df, kind)

    deb = pd.DataFrame({
        "伝票番号": voucher_id,
        "日付": pd.to_datetime(df[h["date"]], errors="coerce").dt.strftime("%Y-%m-%d"),
        "借方勘定科目": df[h["account"]],
        "借方補助科目": df[h["subaccount"]] if h["subaccount"] in df.columns else "",
        "借方部門": df[h["dept"]] if h["dept"] in df.columns else "",
        "借方税区分": df[h["tax"]].map(normalize_tax) if h["tax"] in df.columns else "対象外",
        "借方金額": pd.to_numeric(df[h["amount"]], errors="coerce"),
        "借方摘要": memos,
    })

    credit = CONFIG["CREDIT_RULES"][kind]
    total = deb["借方金額"].sum(skipna=True)
    credit_amounts = [total] + [0] * (len(deb) - 1)

    cred = pd.DataFrame({
        "貸方勘定科目": credit["貸方勘定科目"],
        "貸方補助科目": credit.get("貸方補助科目", ""),
        "貸方部門": credit.get("貸方部門", ""),
        "貸方税区分": credit.get("貸方税区分", "対象外"),
        "貸方金額": credit_amounts,
        "貸方摘要": memos.values,
    }, index=deb.index)

    out = pd.concat([deb, cred], axis=1)
    out["備考"] = ""
    out = out.reindex(columns=CONFIG["OUTPUT_COLUMNS"], fill_value="")
    out = out[pd.notna(out["借方金額"])]
    return out

# ─────────────────────────────────────
# 画面（トップ・マニュアル・設定）
# ─────────────────────────────────────
def _settings_form(config: dict) -> str:
    h = config["SRC_HEADERS"]; cr = config["CREDIT_RULES"]

    def input_row(label, name, value, size=24):
        return f'<label>{label} <input name="{name}" value="{value}" size="{size}"></label>'

    html = []
    html.append('<form method="post" action="/settings">')

    html.append('<fieldset class="card"><legend><b>②データ貼付：列名マッピング</b></legend>')
    html.append('<div class="row">')
    html.append(input_row("日付","h_date",h.get("date","")))
    html.append(input_row("勘定科目名","h_account",h.get("account","")))
    html.append(input_row("補助科目名","h_subaccount",h.get("subaccount","")))
    html.append(input_row("部門","h_dept",h.get("dept","")))
    html.append(input_row("税率","h_tax",h.get("tax","")))
    html.append(input_row("金額(小計)","h_amount",h.get("amount","")))
    html.append('</div><div class="row">')
    html.append(input_row("自由記入欄","h_memo",h.get("memo","")))
    html.append(input_row("支払方法","h_pay",h.get("pay_method","")))
    html.append(input_row("カード","h_card",h.get("card_brand","")))
    html.append(input_row("伝票種別","h_ticket",h.get("ticket_type","")))
    html.append('</div></fieldset>')

    def credit_block(kind, label):
        v = cr[kind]
        block = []
        block.append(f'<div class="row"><b>{label}</b>：')
        block.append(input_row("貸方勘定科目",f"{kind}_credit_acct",v.get("貸方勘定科目","")))
        block.append(input_row("貸方補助科目",f"{kind}_credit_sub",v.get("貸方補助科目","")))
        block.append(input_row("貸方部門",f"{kind}_credit_dept",v.get("貸方部門","")))
        block.append(input_row("貸方税区分",f"{kind}_credit_tax",v.get("貸方税区分","")))
        block.append("</div>")
        return "".join(block)

    html.append('<fieldset class="card"><legend><b>貸方ルール（AMEX / 経費 / 交通費）</b></legend>')
    html.append(credit_block("amex","AMEX"))
    html.append(credit_block("keihi","経費"))
    html.append(credit_block("kotsuhi","交通費"))
    html.append("</fieldset>")

    html.append('<div class="row"><button type="submit">保存</button>')
    html.append('<a href="/" style="margin-left:12px">← 変換に戻る</a></div>')
    html.append("</form>")
    return "".join(html)

INDEX_HTML = f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>楽楽精算→freee仕訳CSV 生成ツール</title>
<style>
  body {{ font-family: system-ui, -apple-system, 'Segoe UI', Roboto, 'Hiragino Kaku Gothic ProN', 'Noto Sans JP', sans-serif; margin: 40px; }}
  header {{ margin-bottom: 24px; display:flex; align-items:center; gap:16px; }}
  .title {{ display:flex; flex-direction:column; }}
  .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 20px; margin-bottom: 20px; }}
  .row {{ display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }}
  input[type=file] {{ padding: 8px; }}
  button {{ padding: 10px 16px; border: 1px solid #111827; background: #111827; color: white; border-radius: 8px; cursor: pointer; }}
  button:hover {{ opacity: 0.9; }}
  .hint {{ color: #6b7280; font-size: 13px; }}
  .logo {{ height:60px; }}
  .version {{ font-size: 12px; color:#555; margin-top:4px; }}
</style>
</head>
<body>
  <header>
    <img src="/static/logo.png" alt="Company Logo" class="logo" />
    <div class="title">
      <h2>楽楽精算→freee仕訳CSV 生成ツール</h2>
      <div class="version">{APP_VERSION} - {APP_DATE}　|　
        <a href="/manual">使い方マニュアル</a>　|　
        <a href="/settings" title="管理者向け設定（列名・貸方ルール）">設定（列名・貸方ルール）</a>
      </div>
    </div>
  </header>

  <div class="card">
    <form action="/convert" method="post" enctype="multipart/form-data">
      <div class="row">
        <input type="file" name="file" accept=".csv,.xlsx,.xlsm,.xls" required />
        <button type="submit">変換してダウンロード</button>
      </div>
      <p class="hint">入力は <b>②データ貼付</b> 相当（CSV/Excel）。Excelは既定でシート「{CONFIG.get('INPUT_SHEET')}」。</p>
    </form>
  </div>
</body></html>"""

MANUAL_HTML = f"""<!DOCTYPE html>
<html lang="ja"><head><meta charset="utf-8"><title>使い方マニュアル</title>
<style>
  body {{ font-family: system-ui,-apple-system,'Segoe UI',Roboto,'Hiragino Kaku Gothic ProN','Noto Sans JP',sans-serif; margin:40px; line-height:1.7; }}
  h1 {{ font-size:22px; margin-bottom:8px; }}
  h2 {{ font-size:18px; margin:18px 0 8px; }}
  ul {{ margin-left:20px; }}
  code {{ background:#f3f4f6; padding:2px 6px; border-radius:6px; }}
</style></head>
<body>
  <h1>使い方マニュアル（{APP_VERSION} - {APP_DATE}）</h1>
  <h2>1. 初期設定（最初の1回）</h2>
  <ul>
    <li>設定では <b>②データ貼付の列名</b> と <b>貸方ルール</b>（勘定/補助/部門/税区分）だけを調整できます。</li>
    <li>freee側のマスタ表記と一致している必要があります。</li>
  </ul>
  <h2>2. 変換</h2>
  <ul>
    <li>トップで CSV/Excel を選び、<b>変換してダウンロード</b>。</li>
    <li>ZIP 内に <code>amex_*.csv</code> / <code>keihi_*.csv</code> / <code>kotsuhi_*.csv</code> / <code>merged_all_freee.csv</code>。</li>
  </ul>
  <h2>3. freeeでの取り込み</h2>
  <ul>
    <li>freee → 取引入力 → 振替伝票 → インポート → 他社会計ソフトインポート。</li>
    <li>同一の <b>伝票番号</b> にまとまった行が「複合仕訳」として取り込まれます。</li>
  </ul>
  <p><a href="/">← トップに戻る</a></p>
</body></html>"""

# ── 画面ルーティング
@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(INDEX_HTML)

@app.get("/manual", response_class=HTMLResponse)
async def manual():
    return HTMLResponse(MANUAL_HTML)

@app.get("/settings", response_class=HTMLResponse)
async def settings_page():
    try:
        page = f"""<html><head><meta charset="utf-8"><title>設定</title>
        <style>
          body {{ font-family: system-ui,-apple-system,'Segoe UI',Roboto,'Hiragino Kaku Gothic ProN','Noto Sans JP',sans-serif; margin:40px; }}
          .card {{ border:1px solid #e5e7eb; border-radius:12px; padding:20px; margin-bottom:20px; }}
          .row {{ display:flex; gap:16px; align-items:center; flex-wrap:wrap; }}
          input {{ padding:8px; min-width:200px; }}
          button {{ padding:10px 16px; border:1px solid #111827; background:#111827; color:#fff; border-radius:8px; cursor:pointer; }}
        </style></head><body>
          <h2>設定（管理者向け）</h2>
          {_settings_form(CONFIG)}
        </body></html>"""
        return HTMLResponse(page)
    except Exception as e:
        return PlainTextResponse("Settings page error:\n" + traceback.format_exc(), status_code=500)

@app.post("/settings")
async def save_settings(request: Request):
    form = await request.form()
    cfg = load_config()

    # 列名
    for key, fkey in [
        ("date","h_date"), ("account","h_account"), ("subaccount","h_subaccount"),
        ("dept","h_dept"), ("tax","h_tax"), ("amount","h_amount"),
        ("memo","h_memo"), ("pay_method","h_pay"), ("card_brand","h_card"), ("ticket_type","h_ticket")
    ]:
        if fkey in form:
            cfg["SRC_HEADERS"][key] = form[fkey].strip()

    # 貸方
    for kind, prefix in [("amex","amex"),("keihi","keihi"),("kotsuhi","kotsuhi")]:
        cr = cfg["CREDIT_RULES"][kind]
        cr["貸方勘定科目"] = form.get(f"{prefix}_credit_acct", cr.get("貸方勘定科目",""))
        cr["貸方補助科目"] = form.get(f"{prefix}_credit_sub", cr.get("貸方補助科目",""))
        cr["貸方部門"]   = form.get(f"{prefix}_credit_dept", cr.get("貸方部門",""))
        cr["貸方税区分"] = form.get(f"{prefix}_credit_tax", cr.get("貸方税区分","対象外"))

    save_config(cfg)
    global CONFIG
    CONFIG = load_config()
    return RedirectResponse(url="/settings", status_code=303)

# ── 変換API（複合仕訳）
@app.post("/convert")
async def convert(file: UploadFile):
    try:
        raw = await file.read()
        df = _read_base(raw, file.filename or "uploaded")

        amex_df, keihi_df, kotsu_df = split_categories(df)

        outputs = {}
        seq = 1
        today_key = datetime.now().strftime("%Y%m%d")

        if not amex_df.empty:
            outputs["amex"] = build_compound_voucher(amex_df, "amex",  f"AMEX-{today_key}-{seq:03d}"); seq += 1
        if not keihi_df.empty:
            outputs["keihi"] = build_compound_voucher(keihi_df, "keihi", f"KEIHI-{today_key}-{seq:03d}"); seq += 1
        if not kotsu_df.empty:
            outputs["kotsuhi"] = build_compound_voucher(kotsu_df, "kotsuhi", f"KOTSU-{today_key}-{seq:03d}"); seq += 1

        mem = io.BytesIO()
        with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            wrote = 0
            for kind, df_out in outputs.items():
                if df_out.empty: continue
                buf = io.StringIO(); df_out.to_csv(buf, index=False)
                zf.writestr(f"{kind}_journal_freee.csv", buf.getvalue().encode("utf-8-sig"))
                wrote += 1
            if wrote:
                merged = pd.concat([d for d in outputs.values() if not d.empty], ignore_index=True)
                buf = io.StringIO(); merged.to_csv(buf, index=False)
                zf.writestr("merged_all_freee.csv", buf.getvalue().encode("utf-8-sig"))
            else:
                zf.writestr("README.txt", "②データ貼付から振り分けできませんでした。列名や判定列をご確認ください。".encode("utf-8"))

        mem.seek(0)
        filename = f"freee_journals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        return StreamingResponse(mem, media_type="application/zip",
                                 headers={"Content-Disposition": f"attachment; filename={filename}"})
    except Exception as e:
        return PlainTextResponse("Convert error:\n" + traceback.format_exc(), status_code=500)

# ─────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)