"""
选股结果数据库管理工具
包含：数据库初始化、txt解析导入、常用查询函数
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import tushare as ts


# ============================================
# 1. 数据库初始化
# ============================================

class StockDB:
    """股票选股结果数据库管理类"""
    
    def __init__(self, db_path: str | Path = "stock_selections.db"):
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        
    def connect(self):
        """连接数据库"""
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # 使查询结果可以按列名访问
        return self.conn
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def init_database(self):
        """初始化数据库表和索引"""
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        
        # 1. 创建选股记录表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS selections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            strategy TEXT NOT NULL,
            code TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, strategy, code)
        )
        """)
        
        # 2. 创建股票基本信息表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            code TEXT PRIMARY KEY,
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            market TEXT,
            list_date TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # 3. 创建战法配置历史表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            config_json TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(strategy, created_at)
        )
        """)
        
        # 4. 创建价格快照表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            close REAL,
            high REAL,
            low REAL,
            volume REAL,
            UNIQUE(code, date)
        )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_selections_date ON selections(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_selections_code ON selections(code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_selections_strategy ON selections(strategy)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_selections_date_code ON selections(date, code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_info_industry ON stock_info(industry)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_code_date ON price_snapshots(code, date)")
        
        self.conn.commit()
        print(f"✅ 数据库初始化完成: {self.db_path}")


# ============================================
# 2. TXT解析导入
# ============================================

class TxtImporter:
    """txt文件解析导入工具"""
    
    # 正则表达式
    HEADER_RE = re.compile(r"选股结果\s*\[(?P<strategy>.+?)\]")
    TRADE_DATE_RE = re.compile(r"交易日:\s*(?P<date>\d{4}-\d{2}-\d{2})")
    COUNT_RE = re.compile(r"符合条件股票数:\s*(?P<count>\d+)")
    NO_PICK_KEYWORD = "无符合条件股票"
    LOG_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\s+-\s+\w+\s+-\s+")
    
    @staticmethod
    def parse_txt_file(txt_path: Path) -> List[Tuple[str, str, str]]:
        """
        解析单个txt文件
        返回：[(date, strategy, code), ...]
        """
        results = []
        
        if not txt_path.exists():
            print(f"⚠️  文件不存在: {txt_path}")
            return results
        
        text = txt_path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        
        cur_strategy = None
        cur_trade_date = None
        expecting_picks = False
        
        for raw in lines:
            line = raw.strip()
            # 去除日志前缀
            line = TxtImporter.LOG_PREFIX_RE.sub('', line).strip()
            
            # 匹配战法名称
            mh = TxtImporter.HEADER_RE.search(line)
            if mh:
                cur_strategy = mh.group("strategy").strip()
                cur_trade_date = None
                expecting_picks = False
                continue
            
            # 匹配交易日期
            md = TxtImporter.TRADE_DATE_RE.search(line)
            if md:
                cur_trade_date = md.group("date")
                expecting_picks = False
                continue
            
            # 匹配股票数量
            mc = TxtImporter.COUNT_RE.search(line)
            if mc and cur_strategy and cur_trade_date:
                expecting_picks = True
                continue
            
            # 解析股票代码
            if expecting_picks and cur_strategy and cur_trade_date:
                if not line:
                    continue
                if TxtImporter.NO_PICK_KEYWORD in line:
                    expecting_picks = False
                    continue
                
                if "," in line:
                    codes = [c.strip() for c in line.split(",") if c.strip()]
                else:
                    codes = [line] if line else []
                
                for code in codes:
                    results.append((cur_trade_date, cur_strategy, code))
                expecting_picks = False
        
        return results
    
    @staticmethod
    def import_txt_to_db(txt_path: Path, db: StockDB, verbose: bool = True):
        """将单个txt文件导入数据库"""
        records = TxtImporter.parse_txt_file(txt_path)
        
        if not records:
            if verbose:
                print(f"ℹ️  {txt_path.name}: 无有效记录")
            return 0
        
        cursor = db.conn.cursor()
        inserted = 0
        duplicated = 0
        
        for date, strategy, code in records:
            try:
                cursor.execute("""
                INSERT INTO selections (date, strategy, code)
                VALUES (?, ?, ?)
                """, (date, strategy, code))
                inserted += 1
            except sqlite3.IntegrityError:
                # 重复记录，跳过
                duplicated += 1
        
        db.conn.commit()
        
        if verbose:
            print(f"✅ {txt_path.name}: 导入 {inserted} 条, 跳过重复 {duplicated} 条")
        
        return inserted
    
    @staticmethod
    def import_all_txt(results_dir: Path, db: StockDB):
        """导入results目录下所有txt文件"""
        if not results_dir.exists():
            print(f"❌ 目录不存在: {results_dir}")
            return
        
        txt_files = sorted(results_dir.glob("*.txt"))
        
        if not txt_files:
            print(f"⚠️  未找到txt文件: {results_dir}")
            return
        
        print(f"📂 开始导入 {len(txt_files)} 个txt文件...")
        total_imported = 0
        
        for txt_file in txt_files:
            count = TxtImporter.import_txt_to_db(txt_file, db, verbose=True)
            total_imported += count
        
        print(f"\n🎉 导入完成! 总计导入 {total_imported} 条记录")


# ============================================
# 3. 查询工具函数
# ============================================

class StockQuery:
    """常用查询工具函数"""
    
    def __init__(self, db: StockDB):
        self.db = db
    
    def get_selections_by_date(self, date: str) -> pd.DataFrame:
        """查询某个日期的所有选股结果"""
        query = """
        SELECT date, strategy, code
        FROM selections
        WHERE date = ?
        ORDER BY strategy, code
        """
        return pd.read_sql_query(query, self.db.conn, params=(date,))
    
    def get_selections_by_code(self, code: str) -> pd.DataFrame:
        """查询某只股票的所有选股历史"""
        query = """
        SELECT date, strategy, code
        FROM selections
        WHERE code = ?
        ORDER BY date DESC, strategy
        """
        return pd.read_sql_query(query, self.db.conn, params=(code,))
    
    def get_selections_by_strategy(self, strategy: str) -> pd.DataFrame:
        """查询某个战法选中的所有股票"""
        query = """
        SELECT date, strategy, code
        FROM selections
        WHERE strategy = ?
        ORDER BY date DESC, code
        """
        return pd.read_sql_query(query, self.db.conn, params=(strategy,))
    
    def get_date_range_selections(self, start_date: str, end_date: str) -> pd.DataFrame:
        """查询日期范围内的选股结果"""
        query = """
        SELECT date, strategy, code
        FROM selections
        WHERE date BETWEEN ? AND ?
        ORDER BY date DESC, strategy, code
        """
        return pd.read_sql_query(query, self.db.conn, params=(start_date, end_date))
    
    def get_multi_strategy_stocks(self, min_count: int = 2) -> pd.DataFrame:
        """查询同时被多个战法选中的股票"""
        query = """
        SELECT code, date, GROUP_CONCAT(strategy, ', ') as strategies, COUNT(*) as strategy_count
        FROM selections
        GROUP BY code, date
        HAVING COUNT(*) >= ?
        ORDER BY date DESC, strategy_count DESC
        """
        return pd.read_sql_query(query, self.db.conn, params=(min_count,))
    
    def get_strategy_stats(self) -> pd.DataFrame:
        """统计每个战法的选股次数"""
        query = """
        SELECT strategy, COUNT(*) as count
        FROM selections
        GROUP BY strategy
        ORDER BY count DESC
        """
        return pd.read_sql_query(query, self.db.conn)
    
    def get_stock_stats(self, limit: int = 50) -> pd.DataFrame:
        """统计被选中次数最多的股票"""
        query = """
        SELECT code, COUNT(*) as count
        FROM selections
        GROUP BY code
        ORDER BY count DESC
        LIMIT ?
        """
        return pd.read_sql_query(query, self.db.conn, params=(limit,))
    
    def get_all_dates(self) -> List[str]:
        """获取所有选股日期"""
        query = "SELECT DISTINCT date FROM selections ORDER BY date DESC"
        cursor = self.db.conn.cursor()
        return [row[0] for row in cursor.execute(query)]
    
    def get_all_strategies(self) -> List[str]:
        """获取所有战法名称"""
        query = "SELECT DISTINCT strategy FROM selections ORDER BY strategy"
        cursor = self.db.conn.cursor()
        return [row[0] for row in cursor.execute(query)]
    
    def get_selections_with_info(self, date: str) -> pd.DataFrame:
        """查询选股结果并联表获取股票基本信息"""
        query = """
        SELECT s.date, s.code, si.name, si.industry, si.market, s.strategy
        FROM selections s
        LEFT JOIN stock_info si ON s.code = si.code
        WHERE s.date = ?
        ORDER BY si.industry, s.code
        """
        return pd.read_sql_query(query, self.db.conn, params=(date,))


# ============================================
# 4. 股票信息管理
# ============================================

class StockInfoManager:
    """股票基本信息管理"""
    
    def __init__(self, db: StockDB):
        self.db = db
        self._init_tushare()
    
    def _init_tushare(self):
        """初始化TuShare"""
        import os
        os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
        os.environ["no_proxy"] = os.environ["NO_PROXY"]
        
        # 使用与fetch_kline.py相同的token
        ts.set_token("803773065895874560")
        self.pro = ts.pro_api()
        self.pro._DataApi__token = "803773065895874560"
        self.pro._DataApi__http_url = "http://tushare.top/dataapi"
    
    @staticmethod
    def to_ts_code(code: str) -> str:
        """将6位代码转换为ts_code"""
        code = code.strip()
        if len(code) != 6:
            return code
        if code.startswith(('600', '601', '603', '688')):
            return f"{code}.SH"
        elif code.startswith(('000', '002', '300')):
            return f"{code}.SZ"
        else:
            return f"{code}.SZ"
    
    def update_stock_info(self, codes: List[str]):
        """批量更新股票基本信息"""
        ts_codes = [self.to_ts_code(c) for c in codes]
        ts_code_set = set(ts_codes)
        
        # 获取股票基本信息
        basic = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,industry,market,list_date"
        )
        df = basic[basic["ts_code"].isin(ts_code_set)].copy()
        
        cursor = self.db.conn.cursor()
        updated = 0
        
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT OR REPLACE INTO stock_info (code, ts_code, name, industry, market, list_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                row["symbol"],
                row["ts_code"],
                row["name"],
                row["industry"] if pd.notna(row["industry"]) else "未知",
                row["market"] if pd.notna(row["market"]) else "未知",
                row["list_date"] if pd.notna(row["list_date"]) else None
            ))
            updated += 1
        
        self.db.conn.commit()
        print(f"✅ 更新了 {updated} 只股票的基本信息")
    
    def update_all_stock_info_from_selections(self):
        """更新数据库中所有选股记录涉及的股票信息"""
        query = "SELECT DISTINCT code FROM selections"
        cursor = self.db.conn.cursor()
        codes = [row[0] for row in cursor.execute(query)]
        
        if not codes:
            print("⚠️  selections表中无数据")
            return
        
        print(f"📊 准备更新 {len(codes)} 只股票的基本信息...")
        self.update_stock_info(codes)


# ============================================
# 5. 使用示例
# ============================================

def main():
    """使用示例"""
    
    # 1. 初始化数据库
    print("=" * 50)
    print("步骤 1: 初始化数据库")
    print("=" * 50)
    db = StockDB("stock_selections.db")
    db.connect()
    db.init_database()
    
    # 2. 导入txt文件
    print("\n" + "=" * 50)
    print("步骤 2: 导入txt文件")
    print("=" * 50)
    results_dir = Path("./results")
    TxtImporter.import_all_txt(results_dir, db)
    
    # 3. 更新股票基本信息
    print("\n" + "=" * 50)
    print("步骤 3: 更新股票基本信息")
    print("=" * 50)
    info_manager = StockInfoManager(db)
    info_manager.update_all_stock_info_from_selections()
    
    # 4. 查询示例
    print("\n" + "=" * 50)
    print("步骤 4: 查询示例")
    print("=" * 50)
    query = StockQuery(db)
    
    # 获取所有日期
    dates = query.get_all_dates()
    if dates:
        print(f"\n📅 数据库中有 {len(dates)} 个交易日的数据")
        print(f"   最新日期: {dates[0]}")
        print(f"   最早日期: {dates[-1]}")
        
        # 查询最新日期的数据
        latest_date = dates[0]
        df = query.get_selections_by_date(latest_date)
        print(f"\n📊 {latest_date} 的选股结果 ({len(df)} 条):")
        print(df.head(10))
    
    # 战法统计
    stats = query.get_strategy_stats()
    print("\n📈 战法统计:")
    print(stats)
    
    # 多战法选中的股票
    multi = query.get_multi_strategy_stocks(min_count=2)
    if not multi.empty:
        print(f"\n🎯 同时被多个战法选中的股票 (前10):")
        print(multi.head(10))
    
    db.close()
    print("\n✅ 所有操作完成!")


if __name__ == "__main__":
    main()