import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics

# ========================================================
# BƯỚC 1: LẤY DỮ LIỆU TỔNG HỢP THEO NGÀY
# ========================================================
print("--- ĐANG TẢI DỮ LIỆU (AGGREGATE DAILY) ---")
engine = create_engine('mysql+pymysql://root:123456@localhost/delivery_dw')

# Query: Gom nhóm theo ngày (Bỏ qua giờ)
# Lấy từ DW vì DW đã chuẩn hóa theo ngày rồi
sql = """
SELECT 
    d.date_key as ds, 
    COUNT(f.id_order) as y 
FROM fact_luong_don_dat_hang f
JOIN dim_date d ON f.id_date = d.date_key
GROUP BY d.date_key
ORDER BY d.date_key
"""

df = pd.read_sql(sql, engine)
df['ds'] = pd.to_datetime(df['ds'])

# --- BƯỚC QUAN TRỌNG: LỌC NHIỄU (CLEAN OUTLIERS) ---
# Nếu có những ngày đơn hàng quá thấp (ví dụ < 100 đơn do lỗi app/nghỉ lễ đóng cửa)
# Prophet sẽ bị "học sai". Ta cần loại bỏ chúng.
mean_val = df['y'].mean()
print(f"Trung bình đơn/ngày: {mean_val:.0f}")

# Lọc bỏ các ngày có lượng đơn < 10% mức trung bình (Giả sử là lỗi)
# Bạn có thể điều chỉnh số này tùy thực tế
df = df[df['y'] > (mean_val * 0.1)] 

print(f"✅ Dữ liệu sạch: {len(df)} ngày.")

# ========================================================
# BƯỚC 2: HUẤN LUYỆN MODEL (CẤU HÌNH CHO DAILY)
# ========================================================
print("\n--- HUẤN LUYỆN MODEL ---")

model = Prophet(
    daily_seasonality=False,   # Tắt daily (vì đang dự báo theo ngày)
    weekly_seasonality=True,   # Bật weekly (Rất quan trọng)
    yearly_seasonality=True,   # Bật yearly (Mùa vụ năm)
    
    # Tinh chỉnh độ nhạy:
    changepoint_prior_scale=0.1, # Tăng nhẹ để bắt trend tốt hơn
    seasonality_prior_scale=10.0 # Tăng để bắt nhịp cuối tuần tốt hơn
)

model.add_country_holidays(country_name='BR')
model.fit(df)

# ========================================================
# BƯỚC 3: ĐÁNH GIÁ ĐỘ CHÍNH XÁC (CROSS-VALIDATION)
print("\n--- ĐANG KIỂM TRA ĐỘ CHÍNH XÁC (CV) ---")

try:
    # SỬA TẠI ĐÂY: Giảm thời gian xuống để vừa với dữ liệu 4 tháng
    df_cv = cross_validation(
        model, 
        initial='60 days',  # Học 2 tháng đầu
        period='15 days',   # Cách 2 tuần test 1 lần
        horizon='15 days'   # Chỉ dự báo xa 2 tuần
    )
    
    df_p = performance_metrics(df_cv)
    metrics = df_p[['horizon', 'mae', 'mape']].mean()

    print("\n--- KẾT QUẢ ĐÁNH GIÁ MỚI ---")
    print(f"MAE (Sai số tuyệt đối): {metrics['mae']:.2f} đơn")
    print(f"MAPE (Sai số phần trăm): {metrics['mape']:.2%}")
    
    if metrics['mape'] < 0.15:
        print("=> ĐÁNH GIÁ: TỐT (Sai số < 15%)")
    else:
        print("=> ĐÁNH GIÁ: CẦN CẢI THIỆN THÊM (Do dữ liệu quá ngắn)")

except Exception as e:
    print(f"Vẫn lỗi: {e}")
    # In ra thời gian thực tế để kiểm tra
    print(f"Dữ liệu thực tế dài: {(df['ds'].max() - df['ds'].min()).days} ngày")

# ========================================================
# BƯỚC 4: DỰ BÁO 30 NGÀY TỚI (FULL DATA)
# ========================================================
# Khi dự báo tương lai thực tế, ta dùng toàn bộ 100% dữ liệu để học
# nên vẫn có thể dự báo xa 30 ngày được.
future = model.make_future_dataframe(periods=30) 
forecast = model.predict(future)

# Vẽ biểu đồ
fig1 = model.plot(forecast)
plt.title("Dự báo Lượng Đơn (Final Model)")
plt.show()

fig2 = model.plot_components(forecast)
plt.show()

# ========================================================
# BƯỚC 5: XUẤT RA BẢNG KẾT QUẢ DỰ BÁO ĐẸP
# ========================================================

# Lấy 2 cột quan trọng nhất từ kết quả dự báo
forecast_clean = forecast[['ds', 'yhat']].copy()

# Làm tròn số dự báo (yhat) để dễ nhìn (ví dụ: làm tròn thành số nguyên)
forecast_clean['yhat'] = forecast_clean['yhat'].round(0).astype(int)

# Đổi tên cột cho chuyên nghiệp hơn (Optional)
forecast_clean.rename(columns={
    'ds': 'Thời gian dự báo', 
    'yhat': 'Lượng đơn dự kiến'
}, inplace=True)

# Lọc chỉ lấy phần tương lai (30 ngày tới) để báo cáo
# (Vì dataframe 'forecast' chứa cả quá khứ lẫn tương lai)
last_date_in_history = df['ds'].max()
future_forecast = forecast_clean[forecast_clean['Thời gian dự báo'] > last_date_in_history]

print("\n--- BẢNG DỰ BÁO LƯỢNG ĐƠN (30 NGÀY TỚI) ---")
print(future_forecast.head(10)) # In 10 dòng đầu tiên của tương lai

# (Tùy chọn) Xuất ra Excel để gửi Sếp
# future_forecast.to_excel('ket_qua_du_bao.xlsx', index=False)
# print("\nĐã xuất file Excel: ket_qua_du_bao.xlsx")