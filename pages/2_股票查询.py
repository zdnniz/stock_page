from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
import tushare as ts

st.set_page_config(page_title="股票查询", layout="wide")

# 数据库路径
DB_PATH = Path("stock_selections.db")


# ============================================
# 战法参数配置
# ============================================

STRATEGY_CONFIG = {
    "前期战法": {
        "name": "前期战法 (BBIKDJSelector)",
        "type": "中短线",
        "holding_days": "5-15天",
        "stop_profit": 15.0,  # 止盈%
        "stop_loss": 5.0,     # 止损%
        "risk_level": "中等",
        "description": "低位盘整后的启动信号，适合抓反弹或突破后第一波上涨"
    },
    "SuperB1战法": {
        "name": "SuperB1战法 (SuperB1Selector)",
        "type": "短线",
        "holding_days": "3-10天",
        "stop_profit": 12.0,
        "stop_loss": 3.0,
        "risk_level": "较高",
        "description": "缩量下跌后抄底，V型反转或二次探底策略"
    },
    "补票战法": {
        "name": "补票战法 (BBIShortLongSelector)",
        "type": "中线",
        "holding_days": "10-30天",
        "stop_profit": 22.0,
        "stop_loss": 8.0,
        "risk_level": "中等",
        "description": "双均线金叉趋势启动，适合抓中期波段"
    },
    "填坑战法": {
        "name": "填坑战法 (PeakKDJSelector)",
        "type": "超短线",
        "holding_days": "1-5天",
        "stop_profit": 7.0,
        "stop_loss": 2.0,
        "risk_level": "高",
        "description": "快速下跌后的反弹，专抓暴跌后填坑行情"
    },
    "上穿60放量战法": {
        "name": "上穿60放量战法 (MA60CrossVolumeWaveSelector)",
        "type": "中长线",
        "holding_days": "20-60天",
        "stop_profit": 40.0,
        "stop_loss": 10.0,
        "risk_level": "较低",
        "description": "突破60日均线+放量确认，趋势反转抓主升浪"
    }
}


def get_strategy_recommendation(strategies: list, current_price: float) -> dict:
    """
    根据命中的战法给出止盈止损建议
    strategies: 命中的战法列表
    current_price: 当前价格
    返回：综合建议
    """
    if not strategies:
        return None
    
    # 收集所有战法的参数
    all_stop_profits = []
    all_stop_losses = []
    strategy_details = []
    
    for strategy in strategies:
        config = STRATEGY_CONFIG.get(strategy)
        if config:
            all_stop_profits.append(config["stop_profit"])
            all_stop_losses.append(config["stop_loss"])
            strategy_details.append(config)
    
    if not all_stop_profits:
        return None
    
    # 计算综合建议（使用加权平均）
    # 如果多个战法命中，使用较保守的策略
    avg_stop_profit = sum(all_stop_profits) / len(all_stop_profits)
    avg_stop_loss = sum(all_stop_losses) / len(all_stop_losses)
    
    # 计算目标价位
    target_price = current_price * (1 + avg_stop_profit / 100)
    stop_loss_price = current_price * (1 - avg_stop_loss / 100)
    
    # 确定持股周期（取最长的）
    holding_days = max([config["holding_days"] for config in strategy_details])
    
    # 确定风险等级
    risk_levels = [config["risk_level"] for config in strategy_details]
    if "高" in risk_levels:
        risk_level = "高"
    elif "较高" in risk_levels:
        risk_level = "较高"
    elif "较低" in risk_levels and len(risk_levels) == 1:
        risk_level = "较低"
    else:
        risk_level = "中等"
    
    return {
        "strategies": strategy_details,
        "stop_profit_pct": avg_stop_profit,
        "stop_loss_pct": avg_stop_loss,
        "target_price": target_price,
        "stop_loss_price": stop_loss_price,
        "holding_days": holding_days,
        "risk_level": risk_level,
        "strategy_count": len(strategies)
    }

@st.cache_resource
def get_db_connection():
    """获取数据库连接（缓存）"""
    if not DB_PATH.exists():
        st.error(f"❌ 数据库文件不存在: {DB_PATH}")
        st.info("💡 请先运行 db_manager.py 初始化数据库")
        st.stop()
    
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def get_stock_selections(code: str) -> pd.DataFrame:
    """从数据库查询股票的选股历史"""
    conn = get_db_connection()
    query = """
    SELECT date, strategy, code
    FROM selections
    WHERE code = ?
    ORDER BY date DESC, strategy
    """
    return pd.read_sql_query(query, conn, params=(code,))


def get_stock_info_from_db(code: str) -> dict:
    """从数据库获取股票基本信息"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    result = cursor.execute(
        "SELECT name, industry, market FROM stock_info WHERE code = ?",
        (code,)
    ).fetchone()
    
    if result:
        return {
            "name": result[0] or "未知",
            "industry": result[1] or "未知",
            "market": result[2] or "未知"
        }
    else:
        return {"name": "未知", "industry": "未知", "market": "未知"}


# ============================================
# TuShare价格查询
# ============================================

def init_tushare():
    """初始化TuShare"""
    import os
    os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    
    ts.set_token("803773065895874560")
    pro = ts.pro_api()
    pro._DataApi__token = "803773065895874560"
    pro._DataApi__http_url = "http://tushare.top/dataapi"
    return pro


@st.cache_data(ttl=3600)  # 缓存1小时
def get_stock_info_from_tushare(code: str) -> dict:
    """从TuShare获取股票基本信息"""
    pro = init_tushare()
    ts_code = to_ts_code(code)
    
    try:
        basic = pro.stock_basic(
            ts_code=ts_code,
            fields="ts_code,symbol,name,industry,market,list_date"
        )
        
        if basic.empty:
            return {"name": "未知", "industry": "未知", "market": "未知"}
        
        row = basic.iloc[0]
        return {
            "name": row["name"],
            "industry": row["industry"] if pd.notna(row["industry"]) else "未知",
            "market": row["market"] if pd.notna(row["market"]) else "未知"
        }
    except Exception as e:
        st.warning(f"获取股票信息失败: {e}")
        return {"name": "未知", "industry": "未知", "market": "未知"}


def to_ts_code(code: str) -> str:
    """将6位股票代码转换为TuShare格式的ts_code"""
    code = code.strip()
    if len(code) != 6:
        return code
    if code.startswith(('600', '601', '603', '688')):
        return f"{code}.SH"
    elif code.startswith(('000', '002', '300')):
        return f"{code}.SZ"
    else:
        return f"{code}.SZ"


@st.cache_data(ttl=300)  # 缓存5分钟
def get_stock_price_change(code: str, start_date: pd.Timestamp, end_date: pd.Timestamp = None) -> dict:
    """
    获取股票从start_date到end_date的价格变化
    返回：{"start_price": xxx, "end_price": xxx, "change": xxx, "change_pct": xxx}
    """
    pro = init_tushare()
    
    if end_date is None:
        end_date = pd.Timestamp.today()
    
    ts_code = to_ts_code(code)
    
    try:
        # 获取日线数据
        df = pro.daily(
            ts_code=ts_code,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d")
        )
        
        if df.empty:
            return {
                "start_price": None,
                "end_price": None,
                "change": None,
                "change_pct": None,
                "error": "无数据"
            }
        
        # 按日期排序
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date")
        
        start_price = df.iloc[0]["close"]
        end_price = df.iloc[-1]["close"]
        change = end_price - start_price
        change_pct = (change / start_price) * 100
        
        return {
            "start_date": df.iloc[0]["trade_date"],
            "end_date": df.iloc[-1]["trade_date"],
            "start_price": start_price,
            "end_price": end_price,
            "change": change,
            "change_pct": change_pct,
            "error": None
        }
    except Exception as e:
        return {
            "start_price": None,
            "end_price": None,
            "change": None,
            "change_pct": None,
            "error": str(e)
        }


# ============================================
# 页面主体
# ============================================

st.title("🔍 股票查询")
st.caption("输入股票代码，从数据库查询该股票被哪些战法选中，以及选中后的涨跌情况")

# 检查数据库
conn = get_db_connection()

# 输入股票代码
col1, col2 = st.columns([3, 1])
with col1:
    stock_code = st.text_input(
        "请输入股票代码（6位数字）",
        placeholder="例如：600000, 000001, 300750",
        max_chars=6
    )

with col2:
    st.write("")  # 占位
    st.write("")  # 占位
    search_btn = st.button("🔍 查询", type="primary", use_container_width=True)

if search_btn and stock_code:
    stock_code = stock_code.strip()
    
    # 验证代码格式
    if not stock_code.isdigit() or len(stock_code) != 6:
        st.error("请输入有效的6位股票代码")
        st.stop()
    
    # 从数据库查询该股票的记录
    with st.spinner("正在从数据库查询..."):
        stock_hist = get_stock_selections(stock_code)
    
    if stock_hist.empty:
        st.warning(f"未找到股票 {stock_code} 的选股记录")
        st.info("💡 该股票可能从未被任何战法选中，或者数据库尚未导入相关数据")
        st.stop()
    
    # 获取股票基本信息（优先从数据库，如果没有则从TuShare获取）
    stock_info = get_stock_info_from_db(stock_code)
    if stock_info["name"] == "未知":
        with st.spinner("正在获取股票信息..."):
            stock_info = get_stock_info_from_tushare(stock_code)
    
    # 显示股票基本信息
    st.success(f"✅ 找到 {len(stock_hist)} 条选股记录")
    
    info_col1, info_col2, info_col3 = st.columns(3)
    with info_col1:
        st.metric("股票代码", stock_code)
    with info_col2:
        st.metric("股票名称", stock_info["name"])
    with info_col3:
        st.metric("所属行业", stock_info["industry"])
    
    st.divider()
    
    # 按日期分组显示
    st.subheader("📊 选股记录与操作建议")
    
    # 转换日期列为datetime类型
    stock_hist["date"] = pd.to_datetime(stock_hist["date"])
    
    # 按日期分组
    grouped = stock_hist.groupby("date")["strategy"].apply(list).reset_index()
    grouped = grouped.sort_values("date", ascending=False)
    
    # 获取最新的选股记录（用于显示当前操作建议）
    latest_record = grouped.iloc[0] if not grouped.empty else None
    
    # 计算涨跌情况
    result_rows = []
    
    with st.spinner("正在计算涨跌情况..."):
        progress_bar = st.progress(0)
        total = len(grouped)
        
        for idx, row in grouped.iterrows():
            select_date = row["date"]
            strategies = row["strategy"]
            
            # 获取价格变化
            price_change = get_stock_price_change(stock_code, select_date)
            
            result_rows.append({
                "选中日期": select_date.strftime("%Y-%m-%d"),
                "战法数量": len(strategies),
                "战法列表": ", ".join(strategies),
                "起始价格": price_change["start_price"],
                "最新价格": price_change["end_price"],
                "涨跌额": price_change["change"],
                "涨跌幅(%)": price_change["change_pct"],
                "实际起始日": price_change.get("start_date").strftime("%Y-%m-%d") if price_change.get("start_date") else None,
                "实际结束日": price_change.get("end_date").strftime("%Y-%m-%d") if price_change.get("end_date") else None,
                "错误信息": price_change.get("error")
            })
            
            # 更新进度
            progress_bar.progress((idx + 1) / total)
        
        progress_bar.empty()
    
    result_df = pd.DataFrame(result_rows)
    
    # ============================================
    # 显示最新操作建议（如果有最新记录）
    # ============================================
    if latest_record is not None and not result_df.empty:
        latest_strategies = latest_record["strategy"]
        latest_date = latest_record["date"]
        
        # 获取该日期的价格信息
        latest_result = result_df.iloc[0]
        start_price = latest_result["起始价格"]
        current_price = latest_result["最新价格"]
        
        if pd.notna(start_price) and pd.notna(current_price):
            st.success("📌 **最新选中记录操作建议**")
            
            recommendation = get_strategy_recommendation(latest_strategies, start_price)
            
            if recommendation:
                # 显示建议卡片
                rec_col1, rec_col2, rec_col3, rec_col4 = st.columns(4)
                
                with rec_col1:
                    st.metric(
                        "建议止盈价",
                        f"¥{recommendation['target_price']:.2f}",
                        f"+{recommendation['stop_profit_pct']:.1f}%"
                    )
                
                with rec_col2:
                    st.metric(
                        "建议止损价",
                        f"¥{recommendation['stop_loss_price']:.2f}",
                        f"-{recommendation['stop_loss_pct']:.1f}%",
                        delta_color="inverse"
                    )
                
                with rec_col3:
                    st.metric("建议持股周期", recommendation['holding_days'])
                
                with rec_col4:
                    # 根据风险等级设置颜色
                    risk_color = {
                        "低": "🟢",
                        "较低": "🟢", 
                        "中等": "🟡",
                        "较高": "🟠",
                        "高": "🔴"
                    }.get(recommendation['risk_level'], "⚪")
                    st.metric("风险等级", f"{risk_color} {recommendation['risk_level']}")
                
                # 显示当前状态
                st.markdown("---")
                st.markdown("**📍 当前状态分析**")
                
                current_change_pct = ((current_price - start_price) / start_price) * 100
                
                status_col1, status_col2, status_col3 = st.columns(3)
                
                with status_col1:
                    st.metric("选中日期", latest_date.strftime("%Y-%m-%d"))
                    st.metric("选中价格", f"¥{start_price:.2f}")
                
                with status_col2:
                    st.metric("当前价格", f"¥{current_price:.2f}")
                    
                    # 判断操作建议
                    if current_price >= recommendation['target_price']:
                        st.success("✅ **建议：已达止盈目标，考虑获利了结**")
                    elif current_price <= recommendation['stop_loss_price']:
                        st.error("⚠️ **建议：已触及止损位，建议止损离场**")
                    else:
                        profit_ratio = (current_price - start_price) / (recommendation['target_price'] - start_price)
                        st.info(f"📊 **持仓中** (已实现目标收益的 {profit_ratio*100:.1f}%)")
                
                with status_col3:
                    # 显示当前收益
                    delta_color = "normal" if current_change_pct >= 0 else "inverse"
                    st.metric(
                        "当前收益率",
                        f"{current_change_pct:+.2f}%",
                        delta_color=delta_color
                    )
                    
                    # 显示距离止盈/止损的距离
                    to_profit = ((recommendation['target_price'] - current_price) / current_price) * 100
                    to_loss = ((current_price - recommendation['stop_loss_price']) / current_price) * 100
                    
                    if to_profit > 0:
                        st.write(f"距止盈: +{to_profit:.1f}%")
                    if to_loss > 0:
                        st.write(f"距止损: -{to_loss:.1f}%")
                
                # 显示战法详情
                with st.expander("📋 查看战法详细说明"):
                    for strategy_detail in recommendation['strategies']:
                        st.markdown(f"""
                        **{strategy_detail['name']}**
                        - 类型：{strategy_detail['type']}
                        - 持股周期：{strategy_detail['holding_days']}
                        - 止盈目标：{strategy_detail['stop_profit']}%
                        - 止损位：{strategy_detail['stop_loss']}%
                        - 风险等级：{strategy_detail['risk_level']}
                        - 说明：{strategy_detail['description']}
                        """)
                        st.markdown("---")
                
                st.markdown("---")
    
    # ============================================
    # 显示历史记录表格
    # ============================================
    st.subheader("📈 历史选股记录")
    
    def format_price(x):
        return f"{x:.2f}" if pd.notna(x) else "N/A"
    
    def format_change(x):
        return f"{x:+.2f}" if pd.notna(x) else "N/A"
    
    st.dataframe(
        result_df.style.format({
            "起始价格": format_price,
            "最新价格": format_price,
            "涨跌额": format_change,
            "涨跌幅(%)": format_change
        }).applymap(
            lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else ("color: green" if isinstance(x, (int, float)) and x < 0 else ""),
            subset=["涨跌额", "涨跌幅(%)"]
        ),
        use_container_width=True,
        height=400
    )
    
    # 统计信息
    st.divider()
    st.subheader("📈 统计信息")
    
    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
    
    valid_records = result_df[result_df["涨跌幅(%)"].notna()]
    
    with stat_col1:
        st.metric("总选中次数", len(result_df))
    
    with stat_col2:
        if not valid_records.empty:
            avg_change = valid_records["涨跌幅(%)"].mean()
            st.metric("平均涨跌幅", f"{avg_change:+.2f}%")
        else:
            st.metric("平均涨跌幅", "N/A")
    
    with stat_col3:
        if not valid_records.empty:
            win_rate = (valid_records["涨跌幅(%)"] > 0).sum() / len(valid_records) * 100
            st.metric("胜率", f"{win_rate:.1f}%")
        else:
            st.metric("胜率", "N/A")
    
    with stat_col4:
        all_strategies = stock_hist["strategy"].unique()
        st.metric("涉及战法数", len(all_strategies))
    
    # 显示详细战法列表
    with st.expander("查看所有涉及的战法"):
        strategy_counts = stock_hist.groupby("strategy").size().sort_values(ascending=False)
        for strategy, count in strategy_counts.items():
            st.write(f"• **{strategy}**：选中 {count} 次")
    
    # 显示原始查询结果
    with st.expander("查看数据库原始记录"):
        st.dataframe(
            stock_hist.rename(columns={
                "date": "日期",
                "strategy": "战法",
                "code": "代码"
            }),
            use_container_width=True
        )

elif not stock_code and search_btn:
    st.warning("请先输入股票代码")

# 侧边栏：数据库统计信息
st.sidebar.title("📊 数据库统计")

try:
    # 总记录数
    cursor = conn.cursor()
    total_records = cursor.execute("SELECT COUNT(*) FROM selections").fetchone()[0]
    st.sidebar.metric("总选股记录数", f"{total_records:,}")
    
    # 日期范围
    date_range = cursor.execute(
        "SELECT MIN(date), MAX(date) FROM selections"
    ).fetchone()
    if date_range[0]:
        st.sidebar.write(f"**日期范围:**")
        st.sidebar.write(f"从 {date_range[0]} 到 {date_range[1]}")
    
    # 战法数量
    strategy_count = cursor.execute(
        "SELECT COUNT(DISTINCT strategy) FROM selections"
    ).fetchone()[0]
    st.sidebar.metric("战法数量", strategy_count)
    
    # 股票数量
    stock_count = cursor.execute(
        "SELECT COUNT(DISTINCT code) FROM selections"
    ).fetchone()[0]
    st.sidebar.metric("涉及股票数", stock_count)
    
    # 最活跃战法
    st.sidebar.write("**最活跃战法 (Top 5):**")
    top_strategies = pd.read_sql_query("""
        SELECT strategy, COUNT(*) as count
        FROM selections
        GROUP BY strategy
        ORDER BY count DESC
        LIMIT 5
    """, conn)
    for _, row in top_strategies.iterrows():
        st.sidebar.write(f"• {row['strategy']}: {row['count']} 次")
    
except Exception as e:
    st.sidebar.error(f"获取统计信息失败: {e}")