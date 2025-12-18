import pandas as pd
import time
from sqlalchemy import create_engine, text
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# 1. KẾT NỐI MYSQL
engine = create_engine('mysql+pymysql://root:123456@localhost/delivery_food')

def get_location_info_osm_full():
    geolocator = Nominatim(user_agent="my_delivery_analytics_project_v1")

    reverse_geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1.1)

    query = """
    SELECT store_id, store_latitude, store_longitude 
    FROM stores 
    WHERE (store_city IS NULL OR store_state IS NULL)
      AND store_latitude IS NOT NULL 
    """
    df = pd.read_sql(query, engine)
    
    total_rows = len(df)

    print(f"-> Tìm thấy tổng cộng {total_rows} cửa hàng cần xử lý.")

    if total_rows == 0:
        print("Tất cả dữ liệu đã đầy đủ. Không cần chạy.")
        return

    count = 0
    with engine.begin() as conn:
        for index, row in df.iterrows():
            store_id = row['store_id']
            lat = row['store_latitude']
            lon = row['store_longitude']
            coord_str = f"{lat}, {lon}"

            try:
                location = reverse_geocode(coord_str, language='en')
                
                if location:
                    address_data = location.raw.get('address', {})
                    
                    city = (address_data.get('city') or 
                            address_data.get('town') or 
                            address_data.get('village') or 
                            address_data.get('municipality') or 
                            address_data.get('county'))
                            
                    state = address_data.get('state')

                    sql = """
                    UPDATE stores 
                    SET store_city = :city, store_state = :state 
                    WHERE store_id = :sid
                    """
                    conn.execute(text(sql), {'city': city, 'state': state, 'sid': store_id})
                    
                    # In tiến độ (Progress) để bạn đỡ sốt ruột
                    count += 1
                    print(f"[{count}/{total_rows}] ID {store_id}: {city}, {state}")
                
                else:
                    print(f"[{count}/{total_rows}] Không tìm thấy địa chỉ cho ID {store_id}")

            except Exception as e:
                print(f"[{count}/{total_rows}] Lỗi tại ID {store_id}: {e}")

    print("\nHOÀN TẤT CẬP NHẬT TOÀN BỘ!")

if __name__ == "__main__":
    get_location_info_osm_full()