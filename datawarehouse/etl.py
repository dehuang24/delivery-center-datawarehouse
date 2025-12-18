import pandas as pd
import os
import csv
from dateutil.parser import parse
import mysql.connector
import numpy as np
from sqlalchemy import create_engine, text

# Kết nối tới database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123456",
  database="delivery_food"
)

cursor = mydb.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS delivery_food")

folder_path = r'C:\Users\Duc Hoang\Desktop\data_source'

if not os.path.exists(folder_path):
    print(f"Thu muc '{folder_path}' khong ton tai.")
    exit(1) 

csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

print(csv_files)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

for file_name in csv_files:
    full_path = os.path.join(folder_path, file_name)
    
    # Xử lý Encoding
    try:
        df = pd.read_csv(full_path, encoding='utf-8')
    except:
        df = pd.read_csv(full_path, encoding='latin1')

    print(df.head(10))

    # IN KẾT QUẢ
    print(f"╔══════════════════════════════════════════════════════════════╗")
    print(f"║ FILE: {file_name:<45} ║")
    print(f"║ Kích thước: {df.shape[0]} dòng x {df.shape[1]} cột {' ':<28} ║")
    print(f"╚══════════════════════════════════════════════════════════════╝")
    
    # In 5 dòng đầu tiên
    print(df.head(10))
    print("\n" + "="*80 + "\n") # Dòng kẻ ngăn cách cho dễ nhìn

    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

for file_name in csv_files:
    print(f"--- {file_name} ---")
    full_path = os.path.join(folder_path, file_name)
    
    try:
        df = pd.read_csv(full_path, encoding='utf-8')
    except:
        df = pd.read_csv(full_path, encoding='latin1')

    for col in df.columns:
        first_idx = df[col].first_valid_index()
        
        if first_idx is not None:
            val = df[col].loc[first_idx]
            print(f"   {col:<30} : {type(val).__name__}")
        else:
            print(f"   {col:<30} : TOÀN BỘ LÀ NULL")
    print("\n")


def infer_mysql_type(value):
    if pd.isna(value) or value == '':
        return "TEXT" 

    try:
        int(value)
        return "BIGINT" # Dùng BIGINT để bao quát cả số lớn
    except ValueError:
        pass

    try:
        float(value)
        return "DOUBLE"
    except ValueError:
        pass

    if isinstance(value, str) and len(value) > 8 and any(c.isdigit() for c in value):
        try:
            parse(value) 
            return "DATETIME"
        except (ValueError, TypeError):
            pass

    return "TEXT"

for file_name in csv_files:
    table_name = file_name.replace('.csv', '') 
    full_path = os.path.join(folder_path, file_name)
    
    try:
        df = pd.read_csv(full_path, encoding='utf-8')
    except:
        df = pd.read_csv(full_path, encoding='latin1')
    
    sql_columns = []
    
    print(f"--- Đang phân tích cấu trúc bảng: {table_name.upper()} ---")

    for col in df.columns:
        first_valid_idx = df[col].first_valid_index()
        
        if first_valid_idx is not None:
            sample_val = df[col].loc[first_valid_idx]
            mysql_type = infer_mysql_type(sample_val)
        else:
            mysql_type = "TEXT"
            
        sql_columns.append(f"`{col}` {mysql_type}")
        print(f"   -> Cột '{col}': mẫu '{sample_val}' => Type: {mysql_type}")
        
    column_str = ", ".join(sql_columns)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_str});"
    
    try:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")      
        cursor.execute(create_table_sql)
        print(f"Đã tạo bảng `{table_name}` thành công!\n")
    except mysql.connector.Error as err:
        print(f"Lỗi SQL: {err}\n")

engine = create_engine('mysql+pymysql://root:123456@localhost/delivery_food')

for file_name in csv_files:
    table_name = file_name.replace('.csv', '')
    full_path = os.path.join(folder_path, file_name)
    
    try:
        df = pd.read_csv(full_path, encoding='utf-8')
    except:
        df = pd.read_csv(full_path, encoding='latin1')

    print(f"Đang xử lý bảng: {table_name} ({len(df)} dòng)...")

    date_cols = [c for c in df.columns if 'moment' in c or 'date' in c or 'created' in c]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')

    float_cols = [c for c in df.columns if any(x in c for x in ['lat', 'lng', 'amount', 'fee', 'cost', 'time'])]
    for col in float_cols:
         df[col] = pd.to_numeric(df[col], errors='coerce')

    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=1000)
        
        print(f"[OK] Đã insert data cho bảng {table_name}\n")
        
    except Exception as e:
        print(f"[LỖI] Bảng {table_name}: {e}\n")

print("Hoàn tất nạp dữ liệu !!!")

connection = engine.connect()

TABLE_JOBS = {
    'orders': {
        'mode': 'keep',
        'cols': [
            'order_id', 'store_id', 'channel_id', 'order_status', 
            'order_amount', 'order_delivery_fee', 'order_delivery_cost',
            'order_moment_created', 'order_moment_collected', 'order_moment_finished'
        ],
        'dedup_cols': ['order_id'],
        'replace_values': {
            'order_delivery_cost': { None: 0 }
        },
        'conditional_cols': {
            'order_amount_value': {
                'source': 'order_amount',
                'rules': [
                    ('<', 200, 'Low'),
                    ('<', 1000, 'Medium'),
                    ('<', 2000, 'High')
                ],
                'else_value': 'Very High'
            }
        }
    },

    'deliveries': {
        'mode': 'skip',
        'replace_values': {
            'driver_id': { None: -1 } # Thay Null bằng -1
        },
        'conditional_cols': {
            'delivery_range': {
                'source': 'delivery_distance_meters',
                'rules': [
                    ('<', 5000,  'Small'),
                    ('<', 10000, 'Medium'),
                    ('<', 20000, 'Large')
                ],
                'else_value': 'Very Large'
            }
        }
    },
    
    'payments': {
        'mode': 'drop',
        'cols': ['payment_amount', 'payment_fee']
    },
    
    'stores': {
        'mode': 'drop',
        'cols': ['store_plan_price']
    },

    'hubs': {
        'mode': 'skip',       
        'dedup_cols': [],     
        'replace_values': {
            'hub_state': {
                'RS': 'Rio Grande do Sul',
                'RJ': 'Rio de Janeiro',
                'SP': 'São Paulo',
                'PR': 'Paraná',         # Thêm dự phòng nếu có
                'MG': 'Minas Gerais',   # Thêm dự phòng
                'DF': 'Distrito Federal'
            }
        }
    }
}


def clean_table_columns(table_name, job_config):
    mode = job_config.get('mode', 'skip')
    
    if mode == 'skip':
        return

    target_cols = set(job_config.get('cols', []))
    
    try:
        current_cols = set(pd.read_sql(f"SELECT * FROM `{table_name}` LIMIT 0", connection).columns)
        cols_to_drop = []
        
        if mode == 'keep':
            cols_to_drop = list(current_cols - target_cols)
        elif mode == 'drop':
            cols_to_drop = list(target_cols.intersection(current_cols))
            
        if cols_to_drop:
            print(f"Tìm thấy {len(cols_to_drop)} cột cần xóa: {cols_to_drop}")
            drop_cmd = [f"DROP COLUMN `{col}`" for col in cols_to_drop]
            sql = f"ALTER TABLE `{table_name}` {', '.join(drop_cmd)};"
            connection.execute(text(sql))
            print(f"Đã xử lý xong!")
        else:
            print(f"Bảng đã chuẩn.")

    except Exception as e:
        print(f"Lỗi: {e}")
    
    print("-" * 30)

for table, config in TABLE_JOBS.items():
    clean_table_columns(table, config)    

def remove_duplicates(table_name, job_config):
    dedup_cols = job_config.get('dedup_cols', [])
    
    if not dedup_cols:
        return
    
    cols_str = ", ".join([f"`{c}`" for c in dedup_cols])
    
    try:
        count_sql = f"""
        SELECT COUNT(*) - COUNT(DISTINCT {cols_str}) as duplicate_count 
        FROM `{table_name}`
        """
        dup_count = connection.execute(text(count_sql)).scalar()
        
        if dup_count == 0:
            print(f"Bảng {table_name} sạch, không có dòng trùng lặp")
            return
        print(f"Phát hiện {dup_count} dòng trùng lặp")

        delete_sql = f"""
        DELETE t1 FROM `{table_name}` t1
        INNER JOIN `{table_name}` t2 
        WHERE 
            t1.row_id > t2.row_id 
            AND t1.`{dedup_cols[0]}` = t2.`{dedup_cols[0]}`
        """

        temp_table = f"{table_name}_dedup_temp" #Bảng tạm chứa
        connection.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        
        # Chọn tất cả cột, GROUP BY theo khóa chính để loại bỏ trùng
        create_temp_sql = f"""
        CREATE TABLE {temp_table} AS
        SELECT * FROM `{table_name}` GROUP BY {cols_str};
        """
        connection.execute(text(create_temp_sql))
        
        # Kiểm tra lại số lượng cho chắc ăn
        new_count = connection.execute(text(f"SELECT COUNT(*) FROM {temp_table}")).scalar()
        
        # Tráo đổi bảng (Swap): Xóa bảng gốc, đổi tên bảng tạm thành bảng gốc
        connection.execute(text(f"DROP TABLE `{table_name}`"))
        connection.execute(text(f"RENAME TABLE {temp_table} TO `{table_name}`"))
        
        print(f"Đã xóa trùng lặp! Bảng {table_name} hiện còn {new_count} dòng.")

    except Exception as e:
        print(f"Lỗi: {e}")
    print("-" * 30)

for table, config in TABLE_JOBS.items():
    remove_duplicates(table, config)

def replace_values(table_name, job_config):
    replace_map = job_config.get('replace_values', {})
    
    if not replace_map:
        return
    
    with engine.begin() as conn:
        for col_name, mapping in replace_map.items():
            try:
                conn.execute(text(f"SELECT `{col_name}` FROM `{table_name}` LIMIT 1"))
            except:
                print(f"Cột '{col_name}' không tồn tại. Bỏ qua.")
                continue

            for old_val, new_val in mapping.items():
                try:
                    # --- LOGIC THÔNG MINH Ở ĐÂY ---
                    if old_val is None:
                        # Trường hợp 1: Xử lý Null (Dùng IS NULL)
                        sql = f"UPDATE `{table_name}` SET `{col_name}` = :new_val WHERE `{col_name}` IS NULL"
                        params = {'new_val': new_val}
                        log_msg = f"Null -> {new_val}"
                    else:
                        # Trường hợp 2: Thay thế giá trị thường (Dùng = )
                        sql = f"UPDATE `{table_name}` SET `{col_name}` = :new_val WHERE `{col_name}` = :old_val"
                        params = {'new_val': new_val, 'old_val': old_val}
                        log_msg = f"'{old_val}' -> '{new_val}'"

                    result = conn.execute(text(sql), params)
                    
                    if result.rowcount > 0:
                        print(f"Đã thay thế {log_msg} ({result.rowcount} dòng) tại cột {col_name}")
                        
                except Exception as e:
                    print(f"Lỗi cột {col_name}: {e}")
                
    print("-" * 30)

for table, config in TABLE_JOBS.items():
    replace_values(table, config)     

def add_conditional_columns(table_name, job_config):
    cond_config = job_config.get('conditional_cols', {})
    
    if not cond_config:
        return

    with engine.begin() as conn:
        for new_col, logic in cond_config.items():
            source_col = logic['source']
            else_val = logic['else_value']
            
            try:
                conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `{new_col}` VARCHAR(50)"))
                print(f"   -> Đã thêm cột mới: {new_col}")
            except Exception:

            case_parts = []
            for op, val, output in logic['rules']:
                case_parts.append(f"WHEN `{source_col}` {op} {val} THEN '{output}'")
    
            sql = f"""
            UPDATE `{table_name}` 
            SET `{new_col}` = CASE 
                {' '.join(case_parts)} 
                ELSE '{else_val}' 
            END
            """
                       
            try:
                result = conn.execute(text(sql))
                if result.rowcount > 0:
                    print(f"Đã cập nhật giá trị cho cột '{new_col}' ({result.rowcount} dòng)")
            except Exception as e:
                print(f"Lỗi khi tính toán cột '{new_col}': {e}")
    
    print("-" * 30)

print("--- START PIPELINE ---")

for table, config in TABLE_JOBS.items():
    add_conditional_columns(table, config)

def fix_store_locations():
    with engine.begin() as conn:
        sql = """
        UPDATE stores s
        JOIN hubs h ON s.hub_id = h.hub_id
        SET 
            s.store_latitude = h.hub_latitude,
            s.store_longitude = h.hub_longitude
        WHERE s.store_latitude IS NULL OR s.store_longitude IS NULL
        """
        result = conn.execute(text(sql))
        print(f"Đã sửa tọa độ cho {result.rowcount} cửa hàng.")

for table, config in TABLE_JOBS.items():
    if table == 'stores':
        fix_store_locations()
