from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st
import tushare as ts

st.set_page_config(page_title="结果解析（txt）", layout="wide")

RESULTS_DIR = Path("./results")

HEADER_RE = re.compile(r"选股结果\s*\[(?P<strategy>.+?)\]")
TRADE_DATE_RE = re.compile(r"交易日:\s*(?P<date>\d{4}-\d{2}-\d{2})")
COUNT_RE = re.compile(r"符合条件股票数:\s*(?P<count>\d+)")
NO_PICK_KEYWORD = "无符合条件股票"
TXT_DATE_RE = re.compile(r"^(?P<yyyymmdd>\d{8})\.txt$")
# 匹配日志前缀，例如：2024-01-15 10:30:45,123 - INFO - 
LOG_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\s+-\s+\w+\s+-\s+")


def to_ts_code(code: str) -> str:
    """将6位股票代码转换为TuShare格式的ts_code"""
    code = code.strip()
    if len(code) != 6:
        return code
    # 沪市：600/601/603/688开头 -> 代码.SH
    # 深市：000/002/300开头 -> 代码.SZ
    if code.startswith(('600', '601', '603', '688')):
        return f"{code}.SH"
    elif code.startswith(('000', '002', '300')):
        return f"{code}.SZ"
    else:
        return f"{code}.SZ"  # 默认深市


def get_stock_industry_by_code_tushare(codes: List[str], token: str | None = None) -> pd.DataFrame:
    """
    输入：股票代码列表，例如 ['603344','002006',...]
    输出：DataFrame(股票代码, ts_code, 股票名称, 行业, 市场, 备注)
    """
    global pro
    pro = ts.pro_api()
    pro._DataApi__token    = "792181680650588160" 
    pro._DataApi__http_url = "http://tushare.top/dataapi"
    
    # 转成 ts_code
    ts_codes = [to_ts_code(c) for c in codes]
    ts_code_set = set(ts_codes)
    
    # stock_basic 一次拉全市场基础信息，再按 ts_code 过滤
    # 注：fields 可以按需加减
    basic = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,industry,market,area"
    )
    df = basic[basic["ts_code"].isin(ts_code_set)].copy()
    
    # 有些 code 可能不在 list_status='L'（如退市/暂停），再尝试查 D / P
    missing = ts_code_set - set(df["ts_code"].tolist())
    if missing:
        basic_d = pro.stock_basic(exchange="", list_status="D", fields="ts_code,symbol,name,industry,market,area")
        df2 = basic_d[basic_d["ts_code"].isin(missing)].copy()
        missing2 = missing - set(df2["ts_code"].tolist())
        if missing2:
            basic_p = pro.stock_basic(exchange="", list_status="P", fields="ts_code,symbol,name,industry,market,area")
            df3 = basic_p[basic_p["ts_code"].isin(missing2)].copy()
            df = pd.concat([df, df2, df3], ignore_index=True)
        else:
            df = pd.concat([df, df2], ignore_index=True)
    
    # 组装输出：保持输入 codes 顺序
    # symbol 就是纯6位代码；ts_code 带交易所
    mp = {row["symbol"]: row for _, row in df.iterrows()}
    out_rows = []
    for code in codes:
        sym = code.strip()
        row = mp.get(sym)
        if row is None:
            out_rows.append({
                "股票代码": sym,
                "ts_code": to_ts_code(sym),
                "股票名称": "未知",
                "行业": "未知",
                "市场": "未知",
                "备注": "TuShare stock_basic 未找到（可能代码不对/非A股/权限或数据缺失）"
            })
        else:
            out_rows.append({
                "股票代码": sym,
                "ts_code": row["ts_code"],
                "股票名称": row["name"],
                "行业": row["industry"] if pd.notna(row["industry"]) and row["industry"] else "未知",
                "市场": row["market"] if pd.notna(row["market"]) and row["market"] else "未知",
                "备注": ""
            })
    return pd.DataFrame(out_rows)


def list_result_dates(results_dir: Path) -> List[pd.Timestamp]:
    dates: List[pd.Timestamp] = []
    if not results_dir.exists():
        return dates
    for fp in results_dir.glob("*.txt"):
        m = TXT_DATE_RE.match(fp.name)
        if m:
            dates.append(pd.to_datetime(m.group("yyyymmdd"), format="%Y%m%d"))
    return sorted(set(dates))


def parse_results_dir(results_dir: Path) -> pd.DataFrame:
    rows = []
    if not results_dir.exists():
        return pd.DataFrame(columns=["date", "strategy", "code"])

    for fp in sorted(results_dir.glob("*.txt")):
        text = fp.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()

        cur_strategy = None
        cur_trade_date = None
        expecting_picks = False

        for raw in lines:
            line = raw.strip()
            
            # 去除日志前缀（如果存在）
            line = LOG_PREFIX_RE.sub('', line).strip()

            mh = HEADER_RE.search(line)
            if mh:
                cur_strategy = mh.group("strategy").strip()
                cur_trade_date = None
                expecting_picks = False
                continue

            md = TRADE_DATE_RE.search(line)
            if md:
                cur_trade_date = pd.to_datetime(md.group("date"))
                expecting_picks = False
                continue

            mc = COUNT_RE.search(line)
            if mc and cur_strategy and cur_trade_date is not None:
                expecting_picks = True
                continue

            if expecting_picks and cur_strategy and cur_trade_date is not None:
                if not line:
                    continue
                if NO_PICK_KEYWORD in line:
                    expecting_picks = False
                    continue

                if "," in line:
                    codes = [c.strip() for c in line.split(",") if c.strip()]
                else:
                    codes = [line] if line else []

                for code in codes:
                    rows.append({"date": cur_trade_date, "strategy": cur_strategy, "code": code})
                expecting_picks = False

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["date", "strategy", "code"])
    df["date"] = pd.to_datetime(df["date"])
    return df.drop_duplicates().sort_values(["date", "strategy", "code"])


def filter_hist_by_day(hist: pd.DataFrame, day: pd.Timestamp) -> pd.DataFrame:
    day = pd.to_datetime(day).normalize()
    return hist[hist["date"].dt.normalize() == day].copy()


@st.cache_data
def load_history() -> pd.DataFrame:
    return parse_results_dir(RESULTS_DIR)


st.title("📌 结果解析（txt）")
st.caption("本页只做 results/YYYYMMDD.txt 的解析与浏览：当天选股、战法→股票、股票→战法。")

result_dates = list_result_dates(RESULTS_DIR)
if not result_dates:
    st.warning("results/ 下未找到 YYYYMMDD.txt")
    st.stop()

picked_day = st.sidebar.selectbox(
    "选择结果日期",
    options=result_dates,
    index=len(result_dates) - 1,
    format_func=lambda d: d.strftime("%Y-%m-%d"),
)

hist = load_history()
day_hist = filter_hist_by_day(hist, picked_day)

st.subheader(f"🗓️ 当天选股：{picked_day.strftime('%Y-%m-%d')}")

if day_hist.empty:
    st.warning('当天没有解析到选股记录（可能全是"无符合条件股票"，或日志格式有变化）。')
    st.stop()

c1, c2 = st.columns([2, 1])

with c1:
    st.markdown("**按战法查看（战法 → 股票）**")
    by_strategy = (
        day_hist.groupby("strategy")["code"]
        .apply(lambda s: sorted(set(s)))
        .reset_index(name="codes")
        .sort_values("strategy")
    )
    for _, r in by_strategy.iterrows():
        st.write(f"**{r['strategy']}**（{len(r['codes'])}）")
        st.code(", ".join(r["codes"]) if r["codes"] else "无")

with c2:
    st.markdown("**当天命中股票数（按战法）**")
    stat = (
        day_hist.groupby("strategy")
        .size()
        .sort_values(ascending=False)
        .reset_index(name="命中股票数")
    )
    st.dataframe(stat, use_container_width=True, height=280)

st.divider()

st.markdown("**按股票查看（股票 → 命中战法）**")
by_code = (
    day_hist.groupby("code")["strategy"]
    .apply(lambda s: sorted(set(s)))
    .reset_index(name="strategies")
    .sort_values("code")
)

# 添加行业信息功能
if st.checkbox("显示股票行业信息", value=False):
    with st.spinner("正在获取股票行业信息..."):
        try:
            codes_list = by_code["code"].tolist()
            industry_df = get_stock_industry_by_code_tushare(codes_list)
            
            # 合并行业信息到by_code
            by_code_with_industry = by_code.merge(
                industry_df[["股票代码", "股票名称", "行业", "市场"]],
                left_on="code",
                right_on="股票代码",
                how="left"
            )
            by_code_with_industry = by_code_with_industry.drop(columns=["股票代码"])
            
            st.dataframe(
                by_code_with_industry.rename(columns={
                    "code": "股票代码",
                    "strategies": "命中战法",
                    "股票名称": "名称",
                    "行业": "行业",
                    "市场": "市场"
                }),
                use_container_width=True,
                height=420,
            )
        except Exception as e:
            st.error(f"获取行业信息失败：{str(e)}")
            st.dataframe(
                by_code.rename(columns={"code": "股票", "strategies": "命中战法"}),
                use_container_width=True,
                height=420,
            )
else:
    st.dataframe(
        by_code.rename(columns={"code": "股票", "strategies": "命中战法"}),
        use_container_width=True,
        height=420,
    )