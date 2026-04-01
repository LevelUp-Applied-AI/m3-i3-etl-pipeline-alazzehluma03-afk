import pandas as pd
import pytest
from etl_pipeline import transform, validate

def test_transform_filters_cancelled():
    """اختبار استبعاد الطلبات الملغية"""
    # تجهيز بيانات وهمية فيها طلب ملغي
    df_orders = pd.DataFrame({'order_id': [1], 'customer_id': [101], 'status': ['cancelled']})
    df_items = pd.DataFrame({'order_id': [1], 'product_id': [1], 'quantity': [1]})
    df_products = pd.DataFrame({'product_id': [1], 'category': ['Tech'], 'unit_price': [100]})
    df_customers = pd.DataFrame({'customer_id': [101], 'name': ['Majd']})
    
    data = {
        "customers": df_customers, 
        "products": df_products, 
        "orders": df_orders, 
        "order_items": df_items
    }
    
    result = transform(data)
    # النتيجة لازم تكون فارغة لأننا استبعدنا الطلب الملغي
    assert len(result) == 0

def test_transform_filters_suspicious_quantity():
    """اختبار استبعاد الكميات المشبوهة (> 100)"""
    df_orders = pd.DataFrame({'order_id': [1], 'customer_id': [101], 'status': ['shipped']})
    df_items = pd.DataFrame({'order_id': [1], 'product_id': [1], 'quantity': [101]}) # كمية كبيرة
    df_products = pd.DataFrame({'product_id': [1], 'category': ['Tech'], 'unit_price': [10]})
    df_customers = pd.DataFrame({'customer_id': [101], 'name': ['Majd']})
    
    data = {
        "customers": df_customers, 
        "products": df_products, 
        "orders": df_orders, 
        "order_items": df_items
    }
    
    result = transform(data)
    # النتيجة لازم تكون فارغة لأن الكمية 101 مستبعدة
    assert len(result) == 0

def test_validate_catches_nulls():
    """اختبار أن التحقق يرفض القيم الفارغة"""
    # بيانات فيها قيمة فارغة (None) في اسم العميل
    df_invalid = pd.DataFrame({
        'customer_id': [1], 
        'customer_name': [None], 
        'total_orders': [1], 
        'total_revenue': [100],
        'avg_order_value': [100],
        'top_category': ['Tech']
    })
    
    # نتوقع أن ترفع الدالة خطأ من نوع ValueError
    with pytest.raises(ValueError):
        validate(df_invalid)