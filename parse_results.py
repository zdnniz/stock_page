from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

HEADER_RE = re.compile(r"=+ 选股结果 \[(?P<strategy>.+?)\] =+")
DATE_RE = re.compile(r"交易日:\s*(?P<date>\d{4}-\d{2}-\d{2})")

def parse_results_dir(results_dir: str | Path) -> pd.DataFrame:
    """
    扫描 results/YYYYMMDD.txt，解析为长表：
    columns: date, strategy, code
    """
    results_dir = Path(results_dir)
    files = sorted(results_dir.glob("*.txt"))
    rows = []

    for fp in files:
        text = fp.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()

        cur_strategy = None
        cur_date = None

        for line in lines:
            m1 = HEADER_RE.search(line)
            if m1:
                cur_strategy = m1.group("strategy").strip()
                cur_date = None
                continue

            m2 = DATE_RE.search(line)
            if m2:
                cur_date = pd.to_datetime(m2.group("date")).date()
                continue

            # picks 行：你的 logger 会把 picks 用 ", ".join(picks) 打出来
            # 这里用一个比较稳健的策略：
            # - 必须已识别 strategy + date
            # - 行里包含逗号或纯代码列表
            if cur_strategy and cur_date:
                s = line.strip()
                if (not s) or ("无符合条件" in s) or s.startswith("符合条件股票数"):
                    continue

                # 过滤掉明显不是代码列表的行
                # 若你的代码是 A股/美股代码都行：这里先用“逗号分隔”作为主要判断
                if "," in s:
                    codes = [c.strip() for c in s.split(",") if c.strip()]
                    for code in codes:
                        rows.append({"date": cur_date, "strategy": cur_strategy, "code": code})

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates().sort_values(["code", "date", "strategy"])
    return df
