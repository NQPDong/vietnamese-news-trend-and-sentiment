import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Cấu hình trang
st.set_page_config(
    page_title="Lakehouse Media Trend & Sentiment Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tải dữ liệu từ tầng Gold
@st.cache_data
def load_gold_data():
    trend_path = os.path.join("data", "lakehouse", "gold", "xu_huong_ngay.parquet")
    alert_path = os.path.join("data", "lakehouse", "gold", "canh_bao_dot_bien.parquet")
    
    if not os.path.exists(trend_path) or not os.path.exists(alert_path):
        st.error("Không tìm thấy dữ liệu tầng Gold. Vui lòng chạy job ETL trước!")
        return None, None
        
    df_trend = pd.read_parquet(trend_path)
    df_alert = pd.read_parquet(alert_path)
    
    df_trend["crawled_date"] = pd.to_datetime(df_trend["crawled_date"].astype(str))
    df_alert["crawled_date"] = pd.to_datetime(df_alert["crawled_date"].astype(str))
    
    # Chuẩn hóa tên cột phục vụ hiển thị
    if "z_score_volume" in df_alert.columns and "z_score" not in df_alert.columns:
        df_alert["z_score"] = df_alert["z_score_volume"]
    if "tb_so_bai_chuyen_muc" in df_alert.columns and "trung_binh_lich_su" not in df_alert.columns:
        df_alert["trung_binh_lich_su"] = df_alert["tb_so_bai_chuyen_muc"]
    if "do_lech_chuan_so_bai" in df_alert.columns and "do_lech_chuan" not in df_alert.columns:
        df_alert["do_lech_chuan"] = df_alert["do_lech_chuan_so_bai"]
    
    return df_trend, df_alert

df_trend, df_alert = load_gold_data()

if df_trend is not None and df_alert is not None:
    # --- SIDEBAR: BỘ LỌC TƯƠNG TÁC ---
    st.sidebar.title("🔍 Bộ lọc Dữ liệu")
    st.sidebar.markdown("*Hệ thống Lakehouse Tin tức Tiếng Việt*")
    
    min_date = df_trend["crawled_date"].min().date()
    max_date = df_trend["crawled_date"].max().date()
    
    date_range = st.sidebar.date_input(
        "Khoảng thời gian khảo sát:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Lọc danh sách nguồn báo
    all_sources = ["Tất cả"] + sorted(list(df_trend["source"].unique()))
    selected_source = st.sidebar.selectbox("Nguồn báo chí:", all_sources)
    
    # Lọc danh sách chủ đề
    all_topics = ["Tất cả"] + sorted(list(df_trend["topic"].unique()))
    selected_topic = st.sidebar.selectbox("Chuyên mục / Chủ đề:", all_topics)
    
    # Áp dụng bộ lọc
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        mask = (df_trend["crawled_date"].dt.date >= start_d) & (df_trend["crawled_date"].dt.date <= end_d)
        filtered_trend = df_trend[mask]
        filtered_alert = df_alert[(df_alert["crawled_date"].dt.date >= start_d) & (df_alert["crawled_date"].dt.date <= end_d)]
    else:
        filtered_trend = df_trend.copy()
        filtered_alert = df_alert.copy()
        
    if selected_source != "Tất cả":
        filtered_trend = filtered_trend[filtered_trend["source"] == selected_source]
        
    if selected_topic != "Tất cả":
        filtered_trend = filtered_trend[filtered_trend["topic"] == selected_topic]
        filtered_alert = filtered_alert[filtered_alert["topic"] == selected_topic]

    # --- TIÊU ĐỀ CHÍNH ---
    st.title("📈 HỆ THỐNG THEO DÕI XU HƯỚNG & CẢM XÚC TIN TỨC TIẾNG VIỆT")
    st.caption("Kiến trúc Big Data Lakehouse (HDFS 3 DataNodes • Apache Spark 3.5 MLlib • Parquet)")

    # --- HÀNG KPI CARDS ---
    total_articles = filtered_trend["tong_so_bai"].sum()
    total_pos = filtered_trend["so_bai_tich_cuc"].sum()
    total_neu = filtered_trend["so_bai_trung_tinh"].sum()
    total_neg = filtered_trend["so_bai_tieu_cuc"].sum()
    
    pos_rate = (total_pos / total_articles * 100) if total_articles > 0 else 0
    neu_rate = (total_neu / total_articles * 100) if total_articles > 0 else 0
    neg_rate = (total_neg / total_articles * 100) if total_articles > 0 else 0
    
    alert_count = len(filtered_alert[filtered_alert["muc_do_canh_bao"] != "Bình thường"])

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    kpi1.metric("Tổng số bài báo", f"{total_articles:,}")
    kpi2.metric("Tích cực (Positive)", f"{pos_rate:.1f}%", f"{total_pos:,} bài")
    kpi3.metric("Trung tính (Neutral)", f"{neu_rate:.1f}%", f"{total_neu:,} bài")
    kpi4.metric("Tiêu cực (Negative)", f"{neg_rate:.1f}%", f"{total_neg:,} bài", delta_color="inverse")
    kpi5.metric("Cảnh báo Đột biến (Z-Score)", f"{alert_count} sự kiện", delta="Crisis Alert" if alert_count > 0 else "Ổn định", delta_color="inverse" if alert_count > 0 else "normal")

    st.markdown("---")

    # --- TABS NỘI DUNG ---
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Biến động Xu hướng theo Ngày",
        "🎭 Phân tích Cảm xúc Báo chí",
        "🚨 Cảnh báo Đột biến Truyền thông (Z-Score)",
        "🏗️ Kiến trúc Lakehouse & Báo cáo"
    ])

    with tab1:
        st.subheader("Dòng thời gian biến động lượng tin bài (Time Series)")
        daily_summary = filtered_trend.groupby("crawled_date")["tong_so_bai"].sum().reset_index()
        
        fig_trend = px.area(
            daily_summary,
            x="crawled_date",
            y="tong_so_bai",
            labels={"crawled_date": "Ngày", "tong_so_bai": "Số lượng bài báo"},
            title="Số lượng bài viết xuất bản theo ngày (Tháng 06/2022)",
            color_discrete_sequence=["#1f77b4"]
        )
        fig_trend.update_layout(xaxis_title="Ngày", yaxis_title="Số bài báo", hovermode="x unified")
        st.plotly_chart(fig_trend, use_container_width=True)

        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Top Chuyên mục phổ biến nhất")
            topic_summary = filtered_trend.groupby("topic")["tong_so_bai"].sum().reset_index().sort_values("tong_so_bai", ascending=False).head(10)
            fig_topic = px.bar(topic_summary, x="tong_so_bai", y="topic", orientation="h", color="tong_so_bai", color_continuous_scale="Blues", labels={"tong_so_bai": "Số bài", "topic": "Chủ đề"})
            fig_topic.update_layout(yaxis={"autorange": "reversed"})
            st.plotly_chart(fig_topic, use_container_width=True)
            
        with col_right:
            st.subheader("Top Nguồn báo có lượng tin lớn nhất")
            source_summary = filtered_trend.groupby("source")["tong_so_bai"].sum().reset_index().sort_values("tong_so_bai", ascending=False).head(10)
            fig_source = px.bar(source_summary, x="source", y="tong_so_bai", color="tong_so_bai", color_continuous_scale="Teal", labels={"tong_so_bai": "Số bài", "source": "Nguồn báo"})
            st.plotly_chart(fig_source, use_container_width=True)

    with tab2:
        st.subheader("Phân tích cơ cấu và biến động cảm xúc truyền thông")
        c1, c2 = st.columns([1, 2])
        
        with c1:
            sentiment_df = pd.DataFrame({
                "Cảm xúc": ["Tích cực", "Trung tính", "Tiêu cực"],
                "Số lượng": [total_pos, total_neu, total_neg]
            })
            fig_pie = px.pie(
                sentiment_df,
                values="Số lượng",
                names="Cảm xúc",
                color="Cảm xúc",
                color_discrete_map={"Tích cực": "#28a745", "Trung tính": "#ffc107", "Tiêu cực": "#dc3545"},
                hole=0.45,
                title="Tỷ lệ Cảm xúc Tổng thể"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with c2:
            daily_sentiment = filtered_trend.groupby("crawled_date")[["so_bai_tich_cuc", "so_bai_trung_tinh", "so_bai_tieu_cuc"]].sum().reset_index()
            fig_sent_line = go.Figure()
            fig_sent_line.add_trace(go.Scatter(x=daily_sentiment["crawled_date"], y=daily_sentiment["so_bai_tich_cuc"], mode="lines+markers", name="Tích cực", line=dict(color="#28a745", width=2)))
            fig_sent_line.add_trace(go.Scatter(x=daily_sentiment["crawled_date"], y=daily_sentiment["so_bai_trung_tinh"], mode="lines+markers", name="Trung tính", line=dict(color="#ffc107", width=2)))
            fig_sent_line.add_trace(go.Scatter(x=daily_sentiment["crawled_date"], y=daily_sentiment["so_bai_tieu_cuc"], mode="lines+markers", name="Tiêu cực", line=dict(color="#dc3545", width=2)))
            fig_sent_line.update_layout(title="Xu hướng cảm xúc theo từng ngày", xaxis_title="Ngày", yaxis_title="Số lượng bài", hovermode="x unified")
            st.plotly_chart(fig_sent_line, use_container_width=True)

    with tab3:
        st.subheader("Phát hiện Đột biến Dữ liệu Báo chí (Anomaly Detection)")
        st.info("Công thức Z-score: Điểm đột biến được tính toán dựa trên mức độ sai lệch so với giá trị trung bình lịch sử của chủ đề. $|Z| > 2.0$ biểu thị bùng nổ truyền thông.")
        
        alerts_only = filtered_alert[filtered_alert["muc_do_canh_bao"] != "Bình thường"].sort_values("z_score", ascending=False)
        
        if len(alerts_only) > 0:
            styler = alerts_only[[
                "crawled_date", "topic", "so_bai_ngay", "trung_binh_lich_su", 
                "do_lech_chuan", "z_score", "muc_do_canh_bao"
            ]].style
            map_fn = getattr(styler, "map", getattr(styler, "applymap", None))
            if map_fn:
                styled_df = map_fn(
                    lambda v: "background-color: #f8d7da; color: #721c24; font-weight: bold;" if v == "Khủng hoảng - Đột biến nghiêm trọng" 
                    else ("background-color: #fff3cd; color: #856404; font-weight: bold;" if v == "Cảnh báo đột biến" else ""),
                    subset=["muc_do_canh_bao"]
                )
            else:
                styled_df = alerts_only
            st.dataframe(styled_df, use_container_width=True)
            
            # Biểu đồ phân tán Z-score
            fig_scatter = px.scatter(
                alerts_only,
                x="crawled_date",
                y="z_score",
                size="so_bai_ngay",
                color="muc_do_canh_bao",
                hover_data=["topic", "so_bai_ngay"],
                color_discrete_map={"Khủng hoảng - Đột biến nghiêm trọng": "#dc3545", "Cảnh báo đột biến": "#ffc107"},
                title="Bản đồ các sự kiện Đột biến Truyền thông theo Z-Score"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.success("Không có chủ đề nào chạm ngưỡng cảnh báo đột biến trong bộ lọc hiện tại.")

    with tab4:
        st.subheader("Kiến trúc Hệ thống Lakehouse HDFS & Công nghệ")
        st.markdown("""
        ```text
        ┌──────────────────┐
        │  News Dataset    │ 59.168 bài báo thô (Tháng 06/2022)
        └────────┬─────────┘
                 ▼
        ┌──────────────────┐
        │   HDFS Bronze    │ /lakehouse/bd05_tin_tuc/bronze/news_2021_2022.parquet
        │ (3 DataNodes)    │ Replication = 3, Snappy compressed
        └────────┬─────────┘
                 ▼ (PySpark Data Cleaning & Deduplication)
        ┌──────────────────┐
        │   HDFS Silver    │ /lakehouse/bd05_tin_tuc/silver/news_clean.parquet/
        │                  │ 51.621 bài sạch, 24 phân vùng theo ngày (crawled_date)
        └────────┬─────────┘
                 ▼ (Spark MLlib Sentiment Classification Pipeline)
        ┌──────────────────┐
        │ ML Sentiment     │ /lakehouse/bd05_tin_tuc/models/sentiment_model
        │                  │ LogisticRegression + TF-IDF (pyvi ViTokenizer)
        └────────┬─────────┘
                 ▼ (Aggregation & Z-Score Window Functions)
        ┌──────────────────┐
        │    HDFS Gold     │ xu_huong_ngay.parquet & canh_bao_dot_bien.parquet
        └────────┬─────────┘
                 ▼
        ┌──────────────────┐
        │ Streamlit GUI    │ Dashboard hiển thị thời gian thực
        └──────────────────┘
        ```
        """)
        st.caption("Đồ án môn Dữ liệu lớn (Big Data) • Đại học • Nhóm 4 thành viên")
