import streamlit as st 
import pandas as pd
import clickhouse_connect
import plotly.express as px
import plotly.graph_objects as go
import time
import json
import os

# --- CONFIGURATION ---
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'admin'
CLICKHOUSE_PASS = '123'
METRICS_PATH = "/models/metrics.json"

# Page configuration (Wide mode by default)
st.set_page_config(page_title="Social Sentiment BI", layout="wide", page_icon="📊")

# --- CUSTOM CSS (Unified visual style) ---
st.markdown("""
<style>
    /* KPI Cards */
    div[data-testid="metric-container"] {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    /* Headers */
    h1, h2, h3 {
        color: #FAFAFA;
    }
    /* HR lines */
    hr {
        border-color: #444;
    }
</style>
""", unsafe_allow_html=True)

# --- CONNECTION TO DWH ---
@st.cache_resource
def get_client():
    try:
        return clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASS
        )
    except Exception as e:
        return None
    
def get_distinct_values(table, column):
    """Fetches unique values for filters from ClickHouse"""
    client = get_client()
    if client:
        try:
            df = client.query_df(f"SELECT DISTINCT {column} FROM {table} LIMIT 100")
            return df[column].tolist()
        except:
            return []
    return []

# --- DATA LOADING ---
def load_data(limit=2000000):
    client = get_client()
    if not client:
        return pd.DataFrame()
    
    # Load raw facts + dimensions
    query = f"""
    SELECT 
        f.sentiment_class,
        f.sentiment_score,
        f.created_at,
        f.text,
        f.word_count,
        f.hashtags_count,
        s.source_name,
        t.topic_name
    FROM social_dwh.fact_sentiment f
    JOIN social_dwh.dim_source s ON f.source_id = s.source_id
    JOIN social_dwh.dim_topic t ON f.topic_id = t.topic_id
    ORDER BY f.created_at DESC
    LIMIT {limit}
    """
    try:
        df = client.query_df(query)
        df['created_at'] = pd.to_datetime(df['created_at'])
        return df
    except Exception:
        return pd.DataFrame()

# --- LOAD ML METRICS ---
def load_ml_metrics():
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH, "r") as f:
            return json.load(f)
    return {}

#  SIDEBAR: CONTROLS (Drill-Down & Roll-Up)
st.sidebar.title("🎛 Control Panel")

# 1. Data loading
with st.spinner("Loading data from DWH..."):
    df = load_data()

if df.empty:
    st.sidebar.error("🔴 No connection to DWH or database is empty.")
    st.stop()

# 2. Roll-Up (Time Grain)
st.sidebar.subheader("📅 Roll-Up (Time Grain)")
time_grain_map = {"Minute": "min", "Hour": "H", "Day": "D", "Week": "W"}
selected_grain_label = st.sidebar.selectbox("Time aggregation", list(time_grain_map.keys()), index=1)
grain_code = time_grain_map[selected_grain_label]

# 3. Drill-Down (Filters)
st.sidebar.subheader("🔍 Drill-Down (Filters)")
# Source
sources = ["All sources"] + list(df['source_name'].unique())
selected_source = st.sidebar.selectbox("Source", sources)
# Topic
topics = ["All topics"] + list(df['topic_name'].unique())
selected_topic = st.sidebar.selectbox("Topic", topics)

# Apply filters
df_filtered = df.copy()
if selected_source != "All sources":
    df_filtered = df_filtered[df_filtered['source_name'] == selected_source]
if selected_topic != "All topics":
    df_filtered = df_filtered[df_filtered['topic_name'] == selected_topic]

# Reload button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data (Live)"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.info(f"Loaded records: **{len(df_filtered)}**")

#  MAIN WORKSPACE
st.title("📡 Social Sentiment Analytics Platform")

# Tabs (BI Structure)
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Overview", 
    "🔍 Diagnostics", 
    "🤖 ML Model", 
    "🛠 SQL Console (Ad-hoc)"
])

# TAB 1: DESCRIPTIVE ANALYTICS & KPI
with tab1:
    st.markdown("### Situation Overview (Descriptive Analytics)")
    
    # 1. KPI BLOCK
    total_msgs = len(df_filtered)
    if total_msgs > 0:
        pos_rate = len(df_filtered[df_filtered['sentiment_class'] == 'positive']) / total_msgs
        neg_rate = len(df_filtered[df_filtered['sentiment_class'] == 'negative']) / total_msgs
        avg_score = df_filtered['sentiment_score'].mean()
        # Engagement = text length + hashtag weight
        engagement = df_filtered['word_count'].mean() + (df_filtered['hashtags_count'].mean() * 3)
    else:
        pos_rate, neg_rate, avg_score, engagement = 0, 0, 0, 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Messages Volume", f"{total_msgs}")
    col2.metric("Positive Rate", f"{pos_rate:.1%}", delta_color="normal")
    col3.metric("Negative Rate (Risk)", f"{neg_rate:.1%}", delta="-Risk", delta_color="inverse")
    col4.metric("Avg Engagement", f"{engagement:.1f}", help="Composite engagement metric")

    st.markdown("---")

    # 2. TIME SERIES
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader(f"Sentiment Dynamics (Roll-up: {selected_grain_label})")
        df_time = df_filtered.set_index('created_at').resample(grain_code)['sentiment_class'].value_counts().unstack().fillna(0)
        df_time = df_time.reset_index()
        
        if not df_time.empty:
            df_melt = df_time.melt(id_vars=[df_time.columns[0]], var_name='Sentiment', value_name='Count')
            fig_line = px.area(df_melt, x='created_at', y='Count', color='Sentiment',
                               color_discrete_map={'positive': '#00CC96', 'negative': '#EF553B', 'neutral': '#636EFA'},
                               title="Activity Trend by Sentiment Class")
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("Not enough data to plot.")

    with c2:
        st.subheader("Distribution")
        fig_pie = px.pie(df_filtered, names='sentiment_class', 
                         color='sentiment_class',
                         color_discrete_map={'positive': '#00CC96', 'negative': '#EF553B', 'neutral': '#636EFA'},
                         hole=0.4, title="Class Shares")
        st.plotly_chart(fig_pie, use_container_width=True)

    # 3. TOP TOPICS
    st.subheader(" Trending Topics")
    top_topics = df_filtered['topic_name'].value_counts().head(10).reset_index()
    top_topics.columns = ['Topic', 'Count']
    fig_bar = px.bar(top_topics, x='Count', y='Topic', orientation='h', 
                     color='Count', color_continuous_scale='Viridis')
    st.plotly_chart(fig_bar, use_container_width=True)

# TAB 2: DIAGNOSTIC ANALYTICS
with tab2:
    st.markdown("### Root Cause Analysis (Diagnostic Analytics)")
    
    neg_df = df_filtered[df_filtered['sentiment_class'] == 'negative']
    
    if neg_df.empty:
        st.success(" No negative messages detected! Great performance.")
    else:
        st.warning(f" Found {len(neg_df)} negative messages. Detailed analysis below.")
        
        d1, d2 = st.columns(2)
        
        # Most negative topics
        with d1:
            st.markdown("#####  Most Negative Topics")
            neg_by_topic = neg_df['topic_name'].value_counts().head(5).reset_index()
            neg_by_topic.columns = ['Topic', 'Negatives']
            fig_neg_top = px.bar(neg_by_topic, x='Topic', y='Negatives', color='Negatives', color_continuous_scale='Reds')
            st.plotly_chart(fig_neg_top, use_container_width=True)
            
        # Sources of negativity
        with d2:
            st.markdown("#####  Sources of Toxicity")
            neg_by_source = neg_df['source_name'].value_counts().head(5).reset_index()
            neg_by_source.columns = ['Source', 'Negatives']
            fig_neg_src = px.pie(neg_by_source, names='Source', values='Negatives', hole=0.5, color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_neg_src, use_container_width=True)

        # Drill-down texts
        st.markdown("#####  Examples of Negative Messages (Drill-down)")
        st.dataframe(
            neg_df[['created_at', 'source_name', 'topic_name', 'text']]
            .sort_values(by='created_at', ascending=False)
            .head(10), 
            use_container_width=True
        )

# TAB 3: ML MODEL EVALUATION
with tab3:
    metrics = load_ml_metrics()
    
    if not metrics:
        st.error(" Metrics not found. Run `docker exec -it spark-app python train_model.py`")
    else:
        st.markdown("###  Test Results (Logistic Regression)")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Accuracy", f"{metrics.get('accuracy', 0):.4f}")
        m2.metric("Precision", f"{metrics.get('precision', 0):.4f}")
        m3.metric("Recall", f"{metrics.get('recall', 0):.4f}")
        m4.metric("F1-Score", f"{metrics.get('f1', 0):.4f}")
        
        st.markdown("---")
        
        col_radar, col_matrix = st.columns([1, 1])
        
        with col_radar:
            st.markdown("#### 🕸 Metrics Radar Chart")
            vals = [metrics.get('accuracy',0), metrics.get('precision',0), metrics.get('recall',0), metrics.get('f1',0)]
            fig_r = go.Figure(data=go.Scatterpolar(r=vals, theta=['Accuracy','Precision','Recall','F1'], fill='toself', name='Model V1'))
            fig_r.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), showlegend=False)
            st.plotly_chart(fig_r, use_container_width=True)
            
        with col_matrix:
            st.markdown("####  Confusion Matrix (Real Data)")
            
            cm_real = metrics.get("confusion_matrix")
            
            if cm_real:
                labels = ["Negative", "Positive", "Neutral"]
                
                fig_cm = px.imshow(cm_real, 
                                   x=labels, 
                                   y=labels, 
                                   labels=dict(x="Predicted Class", y="True Class", color="Count"),
                                   text_auto=True, 
                                   color_continuous_scale='Blues')
                st.plotly_chart(fig_cm, use_container_width=True)
            else:
                st.warning("Confusion matrix missing in metrics file.")

        st.info("Metrics computed on test set (20% of Kaggle Dataset).")

# TAB 4: SQL CONSOLE
with tab4:
    st.markdown("### 🛠 Visual SQL Builder (Ad-Hoc Analysis)")
    st.caption("Construct complex queries without writing SQL code.")
    
    # --- 1. ПАНЕЛЬ ФИЛЬТРОВ ---
    with st.container():
        col_f1, col_f2, col_f3 = st.columns(3)
        
        # Фильтр дат
        with col_f1:
            date_range = st.date_input(" Date Range", [])
        
        # Фильтр источников (Dynamic fetch)
        with col_f2:
            avail_sources = get_distinct_values("social_dwh.dim_source", "source_name")
            sel_sources = st.multiselect(" Data Source", avail_sources)
            
        # Фильтр тональности
        with col_f3:
            sel_sentiments = st.multiselect(" Sentiment", ["positive", "negative", "neutral"])

        col_f4, col_f5 = st.columns(2)
        
        # Фильтр тем
        with col_f4:
            avail_topics = get_distinct_values("social_dwh.dim_topic", "topic_name")
            sel_topics = st.multiselect("🏷 Topic / Category", avail_topics)
            
        # Поиск по тексту
        with col_f5:
            keyword = st.text_input("🔎 Keyword Search (Text Content)", placeholder="e.g. 'crisis', 'hack', 'good'")

    st.markdown("---")

    # --- 2. ГЕНЕРАЦИЯ SQL ---
    if st.button("🚀 Generate & Run Query", type="primary"):
        # Базовый запрос
        base_sql = """
        SELECT 
            f.created_at, s.source_name, t.topic_name, f.sentiment_class, f.sentiment_score, f.text
        FROM social_dwh.fact_sentiment f
        JOIN social_dwh.dim_source s ON f.source_id = s.source_id
        JOIN social_dwh.dim_topic t ON f.topic_id = t.topic_id
        """
        
        conditions = []
        
        # Логика условий
        if len(date_range) == 2:
            conditions.append(f"f.created_at BETWEEN '{date_range[0]} 00:00:00' AND '{date_range[1]} 23:59:59'")
        
        if sel_sources:
            src_list = "', '".join(sel_sources)
            conditions.append(f"s.source_name IN ('{src_list}')")
            
        if sel_sentiments:
            sent_list = "', '".join(sel_sentiments)
            conditions.append(f"f.sentiment_class IN ('{sent_list}')")
            
        if sel_topics:
            topic_list = "', '".join(sel_topics)
            conditions.append(f"t.topic_name IN ('{topic_list}')")
            
        if keyword:
            # ClickHouse ILIKE (case-insensitive)
            conditions.append(f"f.text ILIKE '%{keyword}%'")

        # Сборка финального SQL
        if conditions:
            final_sql = base_sql + " WHERE " + " AND ".join(conditions)
        else:
            final_sql = base_sql
            
        final_sql += " ORDER BY f.created_at DESC LIMIT 1000"

        # --- 3. ВЫПОЛНЕНИЕ ---
        st.markdown("##### Generated SQL:")
        st.code(final_sql, language="sql")
        
        client = get_client()
        if client:
            try:
                with st.spinner("Executing query in ClickHouse..."):
                    start_t = time.time()
                    res_df = client.query_df(final_sql)
                    duration = time.time() - start_t
                
                if not res_df.empty:
                    st.success(f" Found {len(res_df)} records in {duration:.3f}s")
                    st.dataframe(res_df, use_container_width=True)
                else:
                    st.warning(" No records found matching these criteria.")
                    
            except Exception as e:
                st.error(f"SQL Error: {e}")
        else:
            st.error("Database connection lost.")

    # --- 4. ADVANCED MODE ---
    with st.expander("Advanced: Write Raw SQL"):
        raw_sql = st.text_area("Enter custom SQL", "SELECT count(*) FROM social_dwh.fact_sentiment")
        if st.button("Run Raw SQL"):
            c = get_client()
            if c: st.dataframe(c.query_df(raw_sql))