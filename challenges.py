import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
from datetime import datetime

# --- CONFIGURATION ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/amman_market"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract(engine):
    print("--- [Extract Stage] ---")
    # ملاحظة لـ Tier 2: في التحميل التدريجي نستخدم "WHERE order_date > last_run"
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict):
    print("--- [Transform Stage] ---")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]
    
    # دمج البيانات الأساسية
    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    
    # الفلاتر الأساسية
    merged = merged[(merged["status"] != 'cancelled') & (merged["quantity"] <= 100)]
    
    # تجميع البيانات
    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()
    
    # حساب المتوسط وقيمة الطلب
    cust_summary["avg_order_value"] = cust_summary["total_revenue"] / cust_summary["total_orders"]
    
    # --- Tier 1: Statistical Outlier Detection ---
    mean_rev = cust_summary['total_revenue'].mean()
    std_rev = cust_summary['total_revenue'].std()
    # تحديد القيم الشاذة (أكثر من 3 انحرافات معيارية)
    cust_summary['is_outlier'] = cust_summary['total_revenue'] > (mean_rev + 3 * std_rev)
    
    # حساب أفضل فئة
    cat_revenue = merged.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = cat_revenue.sort_values(["customer_id", "line_total"], ascending=[True, False]).drop_duplicates("customer_id")
    top_cat = top_cat.rename(columns={"category": "top_category"})[["customer_id", "top_category"]]
    
    # الدمج النهائي
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(top_cat, on="customer_id").merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    return final_df

def validate_and_report(df):
    print("--- [Validation & Tier 1 Reporting Stage] ---")
    
    # الحل الجذري: استخدام دالة bool() و float() و to_dict() 
    # لضمان أن كل البيانات هي Python Native Types
    
    checks = {
        "No Nulls": bool(not df[['customer_id', 'customer_name']].isnull().any().any()),
        "Revenue > 0": bool((df['total_revenue'] > 0).all()),
        "Unique IDs": bool(df['customer_id'].is_unique)
    }
    
    # تحويل بيانات الـ Outliers لقائمة قواميس مع التأكد من نوع الأرقام
    outliers_df = df[df['is_outlier'] == True][['customer_id', 'total_revenue']].copy()
    
    # تحويل الـ Revenue لـ float عادي عشان الـ JSON
    outliers_df['total_revenue'] = outliers_df['total_revenue'].astype(float)
    
    # تحويل الجدول لقائمة (List of Dictionaries)
    outliers_list = outliers_df.to_dict(orient='records')
    
    # بناء التقرير
    quality_report = {
        "etl_timestamp": datetime.now().isoformat(),
        "total_records_checked": int(len(df)), # تحويل لـ int عادي
        "validation_results": checks,
        "outlier_summary": {
            "count": int(len(outliers_list)),
            "details": outliers_list
        }
    }
    
    # الحفظ في الملف
    report_path = os.path.join(OUTPUT_DIR, "quality_report.json")
    with open(report_path, 'w') as f:
        json.dump(quality_report, f, indent=4)
    
    print(f"✅ Quality report generated successfully at: {report_path}")
    
    # إرجاع نتيجة الفحص (هل كل الفحوصات True؟)
    return all(checks.values())

def load(df, engine):
    print("--- [Load Stage] ---")
    # الرفع لقاعدة البيانات
    df.to_sql("customer_analytics_v2", engine, if_exists="replace", index=False)
    # الحفظ CSV
    csv_path = os.path.join(OUTPUT_DIR, "customer_analytics_challenges.csv")
    df.to_csv(csv_path, index=False)
    print(f"✅ Data loaded successfully. Rows: {len(df)}")

def main():
    print("🚀 Running ETL Challenges Mode (Tier 1 Ready)...")
    engine = create_engine(DB_URL)
    
    try:
        # 1. Extraction
        raw_data = extract(engine)
        
        # 2. Transformation (Includes Outlier Detection)
        processed_df = transform(raw_data)
        
        # 3. Validation & JSON Reporting
        if validate_and_report(processed_df):
            # 4. Loading
            load(processed_df, engine)
            print("\n✨ All Challenges (Tier 1) completed successfully!")
        else:
            print("\n❌ ETL stopped due to validation failure.")
            
    except Exception as e:
        print(f"💥 Error occurred: {e}")

if __name__ == "__main__":
    main()


import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
from datetime import datetime

# --- CONFIGURATION ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/amman_market"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_last_run_timestamp(engine):
    """Tier 2: جلب وقت آخر عملية ناجحة من جدول الميتا داتا"""
    query = "SELECT MAX(start_time) FROM etl_metadata WHERE status = 'success'"
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()[0]
        return result if result else '2000-01-01 00:00:00'
    except:
        return '2000-01-01 00:00:00'

def log_etl_run(engine, start_time, rows, status):
    """Tier 2: تسجيل تفاصيل العملية في جدول etl_metadata"""
    end_time = datetime.now()
    query = text("""
        INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
        VALUES (:start, :end, :rows, :status)
    """)
    with engine.connect() as conn:
        conn.execute(query, {"start": start_time, "end": end_time, "rows": rows, "status": status})
        conn.commit()

def extract(engine, last_run_time):
    print(f"--- [Extract Stage] Filtering orders newer than: {last_run_time} ---")
    
    # التحميل التدريجي: نسحب فقط الطلبات الجديدة
    order_query = f"SELECT * FROM orders WHERE order_date > '{last_run_time}'"
    
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql(order_query, engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict):
    print("--- [Transform Stage] ---")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]
    
    if o.empty: return pd.DataFrame()

    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    merged = merged[(merged["status"] != 'cancelled') & (merged["quantity"] <= 100)]
    
    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()
    
    # Tier 1: Outlier Detection
    mean_rev = cust_summary['total_revenue'].mean()
    std_rev = cust_summary['total_revenue'].std()
    cust_summary['is_outlier'] = cust_summary['total_revenue'] > (mean_rev + 3 * std_rev)
    
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    return final_df

def validate_and_report(df):
    if df.empty: return True
    print("--- [Validation & JSON Reporting Stage] ---")
    
    checks = {
        "No Nulls": bool(not df[['customer_id', 'customer_name']].isnull().any().any()),
        "Revenue > 0": bool((df['total_revenue'] > 0).all()),
        "Unique IDs": bool(df['customer_id'].is_unique)
    }
    
    outliers_data = df[df['is_outlier'] == True][['customer_id', 'total_revenue']].to_dict(orient='records')
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": len(df),
        "checks": checks,
        "flagged_outliers": outliers_data
    }
    
    with open('output/quality_report.json', 'w') as f:
        json.dump(report, f, indent=4)
    return all(checks.values())

def load(df, engine):
    if df.empty: return
    # Tier 2: نستخدم 'append' لإضافة البيانات للجدول القديم
    df.to_sql("customer_analytics_v2", engine, if_exists="append", index=False)
    df.to_csv("output/customer_analytics_challenges.csv", index=False)
    print(f"✅ Loaded {len(df)} new rows.")

def main():
    print("🚀 Starting Tier 2 — Incremental ETL Mode...")
    engine = create_engine(DB_URL)
    start_time = datetime.now()
    
    try:
        last_run = get_last_run_timestamp(engine)
        data = extract(engine, last_run)
        processed_df = transform(data)
        
        if validate_and_report(processed_df):
            load(processed_df, engine)
            log_etl_run(engine, start_time, len(processed_df), "success")
            print("\n✨ Incremental Run Completed!")
        else:
            log_etl_run(engine, start_time, 0, "failed")
            
    except Exception as e:
        print(f"💥 Error: {e}")

if __name__ == "__main__":
    main()


import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
import logging
from datetime import datetime

# --- Tier 3: Setup Logging with UTF-8 Support ---
def setup_logging(log_path):
    # Added encoding='utf-8' to handle emojis and special characters on Windows
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("ETL_Framework")

def extract(engine, logger):
    logger.info("Stage 1: Extracting data from PostgreSQL")
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    logger.info(f"Extracted {len(orders)} orders and {len(customers)} customers.")
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict, logger):
    logger.info("Stage 2: Transforming data")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]
    
    if o.empty:
        logger.warning("No orders found to transform.")
        return pd.DataFrame()

    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    
    # Advanced Filters (Tier 0 & Tier 1)
    merged = merged[(merged["status"] != 'cancelled') & (merged["quantity"] <= 100)]
    
    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()
    
    # Tier 1: Outlier Detection (Mean + 3*STD)
    if len(cust_summary) > 1:
        mean_rev = cust_summary['total_revenue'].mean()
        std_rev = cust_summary['total_revenue'].std()
        cust_summary['is_outlier'] = cust_summary['total_revenue'] > (mean_rev + 3 * std_rev)
    else:
        cust_summary['is_outlier'] = False
    
    # Add Customer Names
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    logger.info(f"Transformation complete. Generated {len(final_df)} rows.")
    return final_df

def validate_and_report(df, output_dir, logger):
    logger.info("Stage 3: Validating data and generating JSON report")
    if df.empty: return True
    
    checks = {
        "No_Nulls": bool(not df[['customer_id', 'customer_name']].isnull().any().any()),
        "Revenue_Positive": bool((df['total_revenue'] > 0).all()),
        "Unique_IDs": bool(df['customer_id'].is_unique)
    }
    
    # Prepare Outliers for JSON
    outliers_list = df[df['is_outlier'] == True][['customer_id', 'total_revenue']].to_dict(orient='records')
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": int(len(df)),
        "checks": checks,
        "outliers_found": len(outliers_list),
        "flagged_outliers": outliers_list
    }
    
    report_path = os.path.join(output_dir, 'quality_report_tier3.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=4)
    
    logger.info(f"Quality report saved to {report_path}")
    return all(checks.values())

def load(df, engine, config, logger):
    logger.info(f"Stage 4: Loading data to table: {config['target_table']}")
    # Load to DB
    df.to_sql(config['target_table'], engine, if_exists="replace", index=False)
    
    # Load to CSV
    csv_path = os.path.join(config['output_dir'], config['csv_filename'])
    df.to_csv(csv_path, index=False)
    logger.info(f"Data successfully saved to CSV: {csv_path}")

def main():
    # --- Tier 3: Load Configuration ---
    config_file = "config.json"
    if not os.path.exists(config_file):
        print(f"Error: {config_file} not found!")
        return

    with open(config_file, "r") as f:
        config = json.load(f)

    # Setup Directory and Logger
    os.makedirs(config['output_dir'], exist_ok=True)
    logger = setup_logging(config['log_file'])
    
    logger.info("🚀 Starting Tier 3 Framework Pipeline")
    engine = create_engine(config['db_url'])
    
    try:
        data = extract(engine, logger)
        processed_df = transform(data, logger)
        
        if not processed_df.empty:
            if validate_and_report(processed_df, config['output_dir'], logger):
                load(processed_df, engine, config, logger)
                logger.info("✨ ETL Framework Run Successfully!")
            else:
                logger.error("Validation failed. Pipeline stopped.")
        else:
            logger.warning("No data found to process.")
            
    except Exception as e:
        logger.critical(f"Pipeline crashed with error: {e}")

if __name__ == "__main__":
    main()