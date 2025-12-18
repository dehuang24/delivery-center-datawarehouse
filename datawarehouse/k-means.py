import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# Kết nối database
engine = create_engine('mysql+pymysql://root:123456@localhost/delivery_food')

# Chỉ lấy những cửa hàng có tọa độ đầy đủ
query = """
SELECT store_id, store_name, store_latitude, store_longitude 
FROM stores 
WHERE store_latitude IS NOT NULL AND store_longitude IS NOT NULL
"""
df = pd.read_sql(query, engine)

# Lấy 2 cột tọa độ để đưa vào thuật toán (X)
# KMeans cần dữ liệu dạng số, không cần ID hay tên
X = df[['store_latitude', 'store_longitude']]

print(f"Dữ liệu sẵn sàng: {len(df)} cửa hàng.")

# ==========================================
# 2. TÌM K TỐI ƯU (ELBOW & SILHOUETTE)
# ==========================================
print("--- ĐANG TÍNH TOÁN K TỐI ƯU ---")

inertia = []        # Để vẽ Elbow (Càng thấp càng tốt, nhưng phải ở điểm gãy)
silhouette_avg = [] # Để vẽ Silhouette (Càng cao càng tốt)
K_range = range(2, 11) # Thử phân từ 2 đến 10 cụm

for k in K_range:
    # Khởi tạo thuật toán K-Means
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(X)
    
    # Lưu chỉ số Inertia (Tổng bình phương khoảng cách đến tâm)
    inertia.append(kmeans.inertia_)
    
    # Tính Silhouette Score (Độ tách biệt của các cụm)
    score = silhouette_score(X, cluster_labels)
    silhouette_avg.append(score)

# --- VẼ 2 BIỂU ĐỒ ---
plt.figure(figsize=(15, 5))

# Biểu đồ 1: Elbow Method
plt.subplot(1, 2, 1)
plt.plot(K_range, inertia, 'bo-')
plt.title('Elbow Method')
plt.xlabel('Số lượng cụm (k)')
plt.ylabel('Inertia (Độ biến thiên)')
plt.grid(True)

# Biểu đồ 2: Silhouette Score
plt.subplot(1, 2, 2)
plt.plot(K_range, silhouette_avg, 'ro-')
plt.title('Chỉ số Silhouette')
plt.xlabel('Số lượng cụm (k)')
plt.ylabel('Silhouette Score')
plt.grid(True)

plt.tight_layout()
plt.show() 

# ==========================================
# 3. CHẠY K-MEANS VÀ HIỂN THỊ KẾT QUẢ
# ==========================================
# Hướng dẫn: Nhìn biểu đồ trên, tìm điểm "Gãy" của Elbow và điểm Cao của Silhouette
# Giả sử ta chọn k=4 hoặc k=5 (Bạn có thể sửa số này sau khi xem biểu đồ)
k_selected = 4

print(f"\n--- TIẾN HÀNH PHÂN CỤM VỚI K={k_selected} ---")

kmeans_final = KMeans(n_clusters=k_selected, random_state=42, n_init=10)
df['cluster'] = kmeans_final.fit_predict(X)

# Lấy tọa độ tâm cụm
centers = kmeans_final.cluster_centers_

# --- VẼ BIỂU ĐỒ PHÂN BỐ (SCATTER PLOT) ---
plt.figure(figsize=(10, 8))
sns.scatterplot(
    data=df, 
    x='store_longitude', 
    y='store_latitude', 
    hue='cluster', 
    palette='viridis', 
    s=50, 
    alpha=0.7
)

# Vẽ các tâm cụm (Dấu X màu đỏ)
plt.scatter(
    centers[:, 1], centers[:, 0], 
    c='red', s=200, marker='X', label='Centroids'
)

plt.title(f'Bản đồ phân cụm các cửa hàng (K={k_selected})')
plt.xlabel('Kinh độ')
plt.ylabel('Vĩ độ')
plt.legend()
plt.show()

# (Tùy chọn) In ra vài mẫu để xem
print(df[['store_name', 'cluster']].head(10))