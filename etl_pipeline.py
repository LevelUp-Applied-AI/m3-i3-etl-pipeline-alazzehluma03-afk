import pandas as pd
from sqlalchemy import create_engine
import os

def extract(engine):
    print("--- Extracting data from PostgreSQL ---")
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    print(f"Extracted: {len(customers)} customers, {len(products)} products, {len(orders)} orders, {len(order_items)} items.")
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict):
    print("--- Transforming data ---")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]

    # دمج الجداول وحساب القيمة الإجمالية لكل بند
    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    
    # الفلاتر المطلوبة (استبعاد الملغي والكميات المشبوهة)
    merged = merged[merged["status"] != 'cancelled']
    merged = merged[merged["quantity"] <= 100]

    # تجميع البيانات على مستوى العميل
    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()

    cust_summary["avg_order_value"] = cust_summary["total_revenue"] / cust_summary["total_orders"]

    # حساب الفئة الأكثر مبيعاً لكل عميل
    cat_revenue = merged.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = cat_revenue.sort_values(["customer_id", "line_total"], ascending=[True, False]).drop_duplicates("customer_id")
    top_cat = top_cat.rename(columns={"category": "top_category"})[["customer_id", "top_category"]]

    # الدمج النهائي مع معالجة اسم العمود (سواء كان name أو customer_name)
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(top_cat, on="customer_id").merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    return final_df[["customer_id", "customer_name", "total_orders", "total_revenue", "avg_order_value", "top_category"]]

def validate(df):
    print("--- Validating data quality ---")
    checks = {
        "No Nulls": not df[['customer_id', 'customer_name']].isnull().any().any(),
        "Revenue > 0": (df['total_revenue'] > 0).all(),
        "Unique IDs": df['customer_id'].is_unique,
        "Orders > 0": (df['total_orders'] > 0).all()
    }
    for name, result in checks.items():
        print(f"{name}: {'PASS' if result else 'FAIL'}")
        if not result: raise ValueError(f"Check failed: {name}")
    return checks

def load(df, engine, csv_path):
    print(f"--- Loading data to database and {csv_path} ---")
    # الرفع لقاعدة البيانات
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    # الحفظ بصيغة CSV
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"Successfully loaded {len(df)} rows.")

def main():
    print("🚀 Starting ETL Pipeline...")
    DB_URL = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(DB_URL)
    
    data = extract(engine)
    transformed_df = transform(data)
    validate(transformed_df)
    load(transformed_df, engine, "output/customer_analytics.csv")
    
    print("\n✅ ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()