from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# ========================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN MODULE
# ========================================================
# Trỏ đến thư mục chứa 3 file script: etl_pipeline.py, enrich_geo.py, update_dw.py
# Nếu dùng Docker, đường dẫn thường là /opt/airflow/dags/scripts
# Bạn hãy sửa đường dẫn này cho đúng với máy của bạn
sys.path.append(r'/opt/airflow/dags/scripts') 

# ========================================================
# 2. IMPORT CÁC HÀM XỬ LÝ (TỪ CODE CỦA BẠN)
# ========================================================
try:
    from etl_pipeline import run_etl
    from enrich_geo import fetch_and_update_geo
    from update_dw import run_dw_update
except ImportError as e:
    print(f"Lỗi Import: {e}")
    # Placeholder function để không crash DAG nếu chưa có file
    def run_etl(): print("ETL Function Missing")
    def fetch_and_update_geo(): print("Geo Function Missing")
    def run_dw_update(): print("DW Update Function Missing")

# ========================================================
# 3. CẤU HÌNH DAG (LỊCH TRÌNH)
# ========================================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), # Nếu lỗi, thử lại sau 5 phút
}

with DAG(
    'delivery_core_etl_pipeline',      # Tên hiển thị trên Airflow
    default_args=default_args,
    description='Pipeline cốt lõi: Clean -> Geo Enrich -> Warehouse Update',
    schedule_interval='0 7 * * *',     # Chạy 7:00 sáng hàng ngày
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'core-etl'],
) as dag:

    # --- TASK 1: LÀM SẠCH DỮ LIỆU (STAGING) ---
    # Chạy file etl_pipeline.py: Đọc CSV, xử lý Null, Deduplicate, nạp vào delivery_food
    t1_clean = PythonOperator(
        task_id='1_clean_staging_data',
        python_callable=run_etl
    )

    # --- TASK 2: LÀM GIÀU DỮ LIỆU ĐỊA LÝ ---
    # Chạy file enrich_geo.py: Tìm City/State cho các Store mới
    t2_geo = PythonOperator(
        task_id='2_enrich_geography',
        python_callable=fetch_and_update_geo
    )

    # --- TASK 3: CẬP NHẬT KHO DỮ LIỆU (DATA WAREHOUSE) ---
    # Chạy file update_dw.py: Chuyển dữ liệu từ delivery_food sang delivery_dw
    t3_dw = PythonOperator(
        task_id='3_update_data_warehouse',
        python_callable=run_dw_update
    )

    # ========================================================
    # 4. ĐIỀU PHỐI LUỒNG CHẠY (DEPENDENCIES)
    # ========================================================
    # Chạy tuần tự: Phải sạch xong mới được điền địa chỉ, có địa chỉ rồi mới đổ vào kho
    
    t1_clean >> t2_geo >> t3_dw