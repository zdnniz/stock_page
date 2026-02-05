from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Stock Dashboard", layout="wide")

DATA_DIR = Path("./data")
RESULTS_DIR = Path("./results")

# ===== 解析器（与之前一致，供两页共用）=====
HEADER_RE = re.compile(r"选股结果\s*\[(?P<strategy>.+?)\]")
TRADE_DATE_RE = re.compile(r"交易日:\s*(?P<date>\d{4}-\d{2}-\d{2})")
COUNT_RE = re.compile(r"符合条件股票数:\s*(?P<count>\d+)")
NO_PICK_KEYWORD = "无符合条件股票"
TXT_DATE_RE = re.compile(r"^(?P<yyyymmdd>\d{8})\.txt$")


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
    return df.drop_duplicates().sort_values(["code", "date", "strategy"])


@st.cache_data
def load_one(code: str) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / f"{code}.csv", parse_dates=["date"]).sort_values("date")
    return df

def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    将日线 OHLCV 按 rule 聚合成更高周期K线
    rule: 'D' 日线, 'W' 周线, 'M' 月线, 'Q' 季线
    """
    if rule == "D":
        return df.copy()

    d = df.copy()
    d = d.sort_values("date")
    d = d.set_index("date")

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }
    if "volume" in d.columns:
        agg["volume"] = "sum"

    out = d.resample(rule).agg(agg).dropna(subset=["open", "high", "low", "close"])
    out = out.reset_index()
    return out


@st.cache_data
def load_history() -> pd.DataFrame:
    return parse_results_dir(RESULTS_DIR)

# ===== 首页 UI =====
st.title("📈 行情看板（首页）")

if not DATA_DIR.exists():
    st.error("未找到 data/ 目录。")
    st.stop()

codes = sorted([p.stem for p in DATA_DIR.glob("*.csv")])
if not codes:
    st.error("data/ 目录下没有任何 .csv 文件。")
    st.stop()

st.sidebar.title("控制面板")
code = st.sidebar.selectbox("选择股票", codes)

tf = st.sidebar.radio(
    "K线周期",
    options=["1D", "1W", "1M", "1Q"],
    horizontal=True
)

tf_rule = {"1D": "D", "1W": "W", "1M": "M", "1Q": "Q"}[tf]

df_raw = load_one(code)  # 原始日线
df_tf = resample_ohlcv(df_raw, tf_rule)  # 多周期K线
hist = load_history()  # date, strategy, code

# 时间范围
min_d, max_d = df_tf["date"].min().date(), df_tf["date"].max().date()
start, end = st.sidebar.date_input("行情时间范围", value=(min_d, max_d))

mask = (df_tf["date"].dt.date >= start) & (df_tf["date"].dt.date <= end)
df_view = df_tf.loc[mask].copy()

bars = st.sidebar.slider(
    "显示最近K线数量",
    min_value=100,
    max_value=1500,
    value=400,
    step=50,
)

df_view = df_view.tail(bars)

# K线
fig = go.Figure(
    data=[
        go.Candlestick(
            x=df_view["date"],
            open=df_view["open"],
            high=df_view["high"],
            low=df_view["low"],
            close=df_view["close"],
            name="K线",
        )
    ]
)
fig.update_layout(height=520, xaxis_rangeslider_visible=False)

# ===== 新增：双均线按钮 + 6条均线叠加 =====

if "show_dual_ma" not in st.session_state:
    st.session_state.show_dual_ma = False

# 你可以放在 sidebar，也可以放在主页面；这里放在 sidebar
if st.sidebar.button("双均线"):
    st.session_state.show_dual_ma = not st.session_state.show_dual_ma

if st.session_state.show_dual_ma:
    # 计算 MA / EMA（基于 df_view）
    close = df_view["close"]

    df_view["ma20"] = close.rolling(20).mean()
    df_view["ma60"] = close.rolling(60).mean()
    df_view["ma120"] = close.rolling(120).mean()

    df_view["ema20"] = close.ewm(span=20, adjust=False).mean()
    df_view["ema60"] = close.ewm(span=60, adjust=False).mean()
    df_view["ema120"] = close.ewm(span=120, adjust=False).mean()

    # 6条线颜色不同（你也可以改成你喜欢的配色）
    COLORS = {
        "MA20": "#1f77b4",
        "MA60": "#ff7f0e",
        "MA120": "#2ca02c",
        "EMA20": "#d62728",
        "EMA60": "#9467bd",
        "EMA120": "#8c564b",
    }

    # 叠加到 plotly 图上（用 Scatter 线）
    fig.add_trace(
        go.Scatter(x=df_view["date"], y=df_view["ma20"], mode="lines",
                   name="MA20", line=dict(color=COLORS["MA20"], width=1.6))
    )
    fig.add_trace(
        go.Scatter(x=df_view["date"], y=df_view["ma60"], mode="lines",
                   name="MA60", line=dict(color=COLORS["MA60"], width=1.6))
    )
    fig.add_trace(
        go.Scatter(x=df_view["date"], y=df_view["ma120"], mode="lines",
                   name="MA120", line=dict(color=COLORS["MA120"], width=1.6))
    )

    fig.add_trace(
        go.Scatter(x=df_view["date"], y=df_view["ema20"], mode="lines",
                   name="EMA20", line=dict(color=COLORS["EMA20"], width=1.6, dash="dot"))
    )
    fig.add_trace(
        go.Scatter(x=df_view["date"], y=df_view["ema60"], mode="lines",
                   name="EMA60", line=dict(color=COLORS["EMA60"], width=1.6, dash="dot"))
    )
    fig.add_trace(
        go.Scatter(x=df_view["date"], y=df_view["ema120"], mode="lines",
                   name="EMA120", line=dict(color=COLORS["EMA120"], width=1.6, dash="dot"))
    )

    # 可选：让图例更好看
    fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0))


# 当前股票命中日打点（来自解析结果）
hist_one = hist[hist["code"] == code].copy()
if not hist_one.empty:
    hist_in_range = hist_one[
        (hist_one["date"].dt.date >= start) & (hist_one["date"].dt.date <= end)
    ].copy()
    if not hist_in_range.empty:
        tmp = hist_in_range.copy()
        tmp["hit_date"] = tmp["date"].dt.date

        agg = (
                tmp.groupby("hit_date")["strategy"]
                .apply(lambda s: sorted(set(s)))
                .reset_index()
                )

        close_map = df_raw.set_index(df_raw["date"].dt.date)["close"].to_dict()
        agg["close"] = agg["hit_date"].map(close_map)
        agg["text"] = agg["strategy"].apply(lambda xs: "命中战法：<br>" + "<br>".join(xs))
        agg = agg.dropna(subset=["close"])

        fig.add_trace(
            go.Scatter(
                x=pd.to_datetime(agg["hit_date"]),
                y=agg["close"],
                mode="markers",
                name="命中日",
                text=agg["text"],
                hovertemplate="%{x|%Y-%m-%d}<br>%{text}<extra></extra>",
            )
        )

st.plotly_chart(fig, use_container_width=True)

# 加入战法历史（该股票）
st.subheader("🧭 加入战法历史（该股票何时命中过哪些战法）")
if hist_one.empty:
    st.info("该股票在当前 results/*.txt 中没有被任何战法选中过。")
else:
    c1, c2 = st.columns([2, 1])
    with c1:
        show = hist_one.sort_values(["date", "strategy"]).copy()
        show["date"] = show["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(
            show.rename(columns={"date": "日期", "strategy": "战法", "code": "代码"}),
            use_container_width=True,
            height=360,
        )
    with c2:
        stat = (
            hist_one.groupby("strategy")
            .size()
            .sort_values(ascending=False)
            .reset_index(name="命中次数")
        )
        st.dataframe(stat, use_container_width=True, height=360)

with st.expander("查看行情数据（最新在前，200 行）"):
    st.dataframe(
        df_view.sort_values("date", ascending=False).head(200),
        use_container_width=True,
    )
