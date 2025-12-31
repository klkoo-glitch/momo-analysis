import streamlit as st
import pandas as pd
import os
import shutil
import plotly.graph_objects as go
from datetime import datetime, timedelta

# 1. 페이지 설정
st.set_page_config(page_title="모모유부 통합 분석 시스템", layout="wide")
st.title("📊 모모유부 지점별 정밀 성과 분석 (고객 데이터 분석.v1)")

# 파일 경로
file_path = r'C:\Users\Administrator\OneDrive\바탕 화면\python_study\지점별 샘플러스 데이터_2025.12.29.xlsx'
DUPLICATE_LIMIT = 30 # 중복 결제 기준 30분

@st.cache_data(ttl=600)
def load_and_process_ultimate_data():
    if not os.path.exists(file_path): 
        return None, "FILE_NOT_FOUND"
    
    temp_path = "temp_analysis_ultimate.xlsx"
    try:
        shutil.copyfile(file_path, temp_path)
        excel = pd.ExcelFile(temp_path)
        combined_data = []
        
        def unify_name(x):
            txt = str(x)
            if '강남구청' in txt: return '강남구청'
            if '기흥' in txt: return '기흥'
            if '여의도' in txt or '브라이튼' in txt: return '여의도'
            if '목동' in txt: return '목동'
            if '원주' in txt: return '원주'
            if '강남' in txt: return '강남'
            return "기타"

        for sheet in excel.sheet_names:
            if any(x in sheet for x in ['요약', '공식']): continue
            df_sheet = pd.read_excel(temp_path, sheet_name=sheet, skiprows=3)
            if df_sheet.empty: continue
            
            is_shifted = df_sheet['가맹점명'].astype(str).str.match(r'\d{4}[-./]\d{2}[-./]\d{2}')
            
            normal = df_sheet[~is_shifted].copy()
            if not normal.empty:
                req = ['카드번호', '거래금액', '거래일자', '거래시간', '가맹점명', '거래유형']
                cols = [c for c in req if c in normal.columns]
                tmp = normal[cols].copy()
                tmp['가맹점명'] = tmp['가맹점명'].apply(unify_name)
                combined_data.append(tmp)
            
            shifted = df_sheet[is_shifted].copy()
            if not shifted.empty:
                sh = pd.DataFrame()
                sh['카드번호'] = shifted['체크']
                sh['거래금액'] = shifted['봉사료']
                sh['거래일자'] = shifted['가맹점명']
                sh['거래시간'] = shifted['발급사']
                sh['거래유형'] = shifted['카드번호'] 
                sh['가맹점명'] = unify_name(sheet)
                combined_data.append(sh)
        
        full_df = pd.concat(combined_data, sort=False).reset_index(drop=True)
        full_df['거래금액'] = pd.to_numeric(full_df['거래금액'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        full_df['datetime'] = pd.to_datetime(
            full_df['거래일자'].astype(str).str.split(' ').str[0] + ' ' + 
            full_df['거래시간'].astype(str).fillna('00:00:00'), 
            errors='coerce'
        )
        full_df = full_df.dropna(subset=['datetime', '카드번호'])
        full_df = full_df.sort_values(['가맹점명', '카드번호', 'datetime'])
        
        # 30분 중복 제거
        full_df['time_diff'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].diff().dt.total_seconds() / 60.0
        full_df = full_df[~((full_df['time_diff'] <= DUPLICATE_LIMIT) & (full_df['time_diff'].notnull()))]
        
        full_df['visit_no'] = full_df.groupby(['가맹점명', '카드번호']).cumcount() + 1
        full_df['first_v'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].transform('min')
        full_df['last_v'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].transform('max')
        full_df['total_v_all'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].transform('count')
        
        second_v = full_df[full_df['visit_no'] == 2][['가맹점명', '카드번호', 'datetime']]
        second_v.columns = ['가맹점명', '카드번호', 'second_date']
        full_df = full_df.merge(second_v, on=['가맹점명', '카드번호'], how='left')
        
        full_df['연월'] = full_df['datetime'].dt.strftime('%Y-%m')
        return full_df, "SUCCESS"
    except Exception as e: return None, str(e)

def draw_highlight_bar(df, x_col, y_col, title, is_pct=False):
    if df.empty or y_col not in df.columns: return
    plot_df = df.copy().reset_index(drop=True)
    colors = ['#D3D3D3'] * len(plot_df)
    max_val = plot_df[y_col].max()
    min_val = plot_df[y_col].min()
    for i, val in enumerate(plot_df[y_col]):
        if val == max_val and val > 0: colors[i] = '#1f77b4'
        elif val == min_val and val > 0: colors[i] = '#d62728'
    fig = go.Figure(data=[go.Bar(x=plot_df[x_col], y=plot_df[y_col], marker_color=colors,
        text=[f"{v:.1f}%" if is_pct else f"{int(v):,}" for v in plot_df[y_col]], textposition='auto')])
    fig.update_layout(title=title, height=330, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

df_main, status = load_and_process_ultimate_data()

if status == "SUCCESS" and df_main is not None:
    st.sidebar.header("⚙️ 분석 설정")
    all_months = sorted(df_main['연월'].unique(), reverse=True)
    selected_month = st.sidebar.selectbox("📅 비교 대상 연월 선택", all_months)
    stores = [s for s in sorted(df_main['가맹점명'].unique()) if s != "기타"]
    data_end_date = df_main['datetime'].max()

    st.header(f"⚖️ {selected_month} 지점별 전지표 통합 비교")
    
    comp_list = []
    for s_name in stores:
        s_data = df_main[df_main['가맹점명'] == s_name]
        m_data = s_data[s_data['연월'] == selected_month].copy()
        if m_data.empty: continue
            
        m_data['net_sales'] = m_data.apply(lambda x: -x['거래금액'] if str(x.get('거래유형', '')) == '취소' else x['거래금액'], axis=1)
        v_ids = m_data['카드번호'].unique()
        total_v = len(v_ids)
        new_v_ids = m_data[m_data['first_v'].dt.strftime('%Y-%m') == selected_month]['카드번호'].unique()
        new_v = len(new_v_ids)
        ret_v = total_v - new_v
        
        if new_v > 0:
            new_full = s_data[s_data['카드번호'].isin(new_v_ids)].groupby('카드번호').first()
            o_conv = round(len(new_full[new_full['total_v_all'] >= 2]) / new_v * 100, 1)
            c_2m = round(len(new_full[(new_full['second_date'].notnull()) & (new_full['second_date'] <= new_full['first_v'] + timedelta(days=60))]) / new_v * 100, 1)
            c_3m = round(len(new_full[(new_full['second_date'].notnull()) & (new_full['second_date'] <= new_full['first_v'] + timedelta(days=90))]) / new_v * 100, 1)
            l_conv = round(len(new_full[new_full['total_v_all'] >= 4]) / new_v * 100, 1)
        else: o_conv = c_2m = c_3m = l_conv = 0.0
        
        v_stats = s_data[s_data['카드번호'].isin(v_ids)].groupby('카드번호').first()
        loyal_v_count = len(v_stats[v_stats['total_v_all'] >= 4])
        ret_pool = v_stats[v_stats['total_v_all'] >= 2]
        
        # [수정] 매출액 선두 + 요청하신 방문자 흐름 순서 + 단위 기호 제거
        comp_list.append({
            "지점": s_name, 
            "매출액": int(m_data['net_sales'].sum()),
            "전체방문자수": total_v,
            "신규방문자": new_v,
            "신규비율": round(new_v/total_v*100, 1) if total_v > 0 else 0.0,
            "재방문자": ret_v,
            "재방문 비율": round(ret_v/total_v*100, 1) if total_v > 0 else 0.0,
            "충성고객": loyal_v_count,
            "충성고객비율": round(loyal_v_count/total_v*100, 1) if total_v > 0 else 0.0,
            "전체전환율": o_conv, 
            "2개월전환율": c_2m, 
            "3개월전환율": c_3m, 
            "충성고객전환율": l_conv,
            "방문빈도": round(ret_pool['total_v_all'].mean(), 1) if not ret_pool.empty else 1.0,
            "유지기간": round((ret_pool['last_v'] - ret_pool['first_v']).dt.days.mean(), 1) if not ret_pool.empty else 0.0,
            "이탈율": round(len(ret_pool[ret_pool['last_v'] <= data_end_date - timedelta(days=90)]) / len(ret_pool) * 100, 1) if not ret_pool.empty else 0.0
        })
    
    comp_df = pd.DataFrame(comp_list)

    st.subheader("📈 성과 비율 지표")
    r1, r2, r3 = st.columns(3)
    with r1: draw_highlight_bar(comp_df, "지점", "전체전환율", "💡 전체 전환율", is_pct=True)
    with r2: draw_highlight_bar(comp_df, "지점", "3개월전환율", "📅 3개월 내 전환율", is_pct=True)
    with r3: draw_highlight_bar(comp_df, "지점", "충성고객비율", "💎 충성고객 비율", is_pct=True)

    st.subheader("👥 고객 규모 비교")
    c1, c2, c3 = st.columns(3)
    with c1: draw_highlight_bar(comp_df, "지점", "신규방문자", "🆕 신규 방문자 수")
    with c2: draw_highlight_bar(comp_df, "지점", "재방문자", "🔄 재방문자 수")
    with c3: draw_highlight_bar(comp_df, "지점", "충성고객", "💎 충성고객 수")

    # 상단 통합 표
    display_comp = comp_df.copy()
    display_comp['매출액'] = display_comp['매출액'].apply(lambda x: f"{x:,}원")
    pct_format_cols = ["신규비율", "재방문 비율", "충성고객비율", "전체전환율", "2개월전환율", "3개월전환율", "충성고객전환율", "이탈율"]
    for col in pct_format_cols:
        display_comp[col] = display_comp[col].apply(lambda x: f"{x}%")
    st.table(display_comp)

    st.divider()

    # 2. 개별 매장 상세 리포트
    selected_store = st.sidebar.selectbox("🏠 상세 매장 선택", stores)
    st.header(f"🔍 {selected_store} 상세 분석 리포트")
    s_df = df_main[df_main['가맹점명'] == selected_store]
    
    monthly_summary = []
    s_months = sorted(s_df['연월'].unique(), reverse=True)
    
    for m in s_months:
        m_df = s_df[s_df['연월'] == m].copy()
        m_df['net_sales'] = m_df.apply(lambda x: -x['거래금액'] if str(x.get('거래유형', '')) == '취소' else x['거래금액'], axis=1)
        v_ids = m_df['카드번호'].unique()
        total_v = len(v_ids)
        new_v_ids = m_df[m_df['first_v'].dt.strftime('%Y-%m') == m]['카드번호'].unique()
        new_v = len(new_v_ids)
        ret_v = total_v - new_v
        
        if new_v > 0:
            new_full = s_df[s_df['카드번호'].isin(new_v_ids)].groupby('카드번호').first()
            o_conv = round(len(new_full[new_full['total_v_all'] >= 2]) / new_v * 100, 1)
            c_2m = round(len(new_full[(new_full['second_date'].notnull()) & (new_full['second_date'] <= new_full['first_v'] + timedelta(days=60))]) / new_v * 100, 1)
            c_3m = round(len(new_full[(new_full['second_date'].notnull()) & (new_full['second_date'] <= new_full['first_v'] + timedelta(days=90))]) / new_v * 100, 1)
            l_conv = round(len(new_full[new_full['total_v_all'] >= 4]) / new_v * 100, 1)
        else: o_conv = c_2m = c_3m = l_conv = 0.0

        v_stats = s_df[s_df['카드번호'].isin(v_ids)].groupby('카드번호').first()
        loyal_v = len(v_stats[v_stats['total_v_all'] >= 4])
        ret_pool = v_stats[v_stats['total_v_all'] >= 2]
        
        monthly_summary.append({
            "연월": m, 
            "매출액": int(m_df['net_sales'].sum()),
            "전체방문자수": total_v, 
            "신규방문자": new_v, 
            "신규비율": round(new_v/total_v*100, 1) if total_v > 0 else 0.0,
            "재방문자": ret_v,
            "재방문 비율": round(ret_v/total_v*100, 1) if total_v > 0 else 0.0,
            "충성고객": loyal_v,
            "충성고객비율": round(loyal_v/total_v*100, 1) if total_v > 0 else 0.0,
            "전체전환율": o_conv, "2개월전환율": c_2m, "3개월전환율": c_3m, "충성고객전환율": l_conv,
            "방문빈도": round(ret_pool['total_v_all'].mean(), 1) if not ret_pool.empty else 1.0,
            "유지기간": round((ret_pool['last_v'] - ret_pool['first_v']).dt.days.mean(), 1) if not ret_pool.empty else 0.0,
            "이탈율": round(len(ret_pool[ret_pool['last_v'] <= data_end_date - timedelta(days=90)]) / len(ret_pool) * 100, 1) if not ret_pool.empty else 0.0
        })

    summary_df = pd.DataFrame(monthly_summary)
    st.subheader(f"📈 {selected_store} 추이 그래프")
    st.line_chart(summary_df.sort_values("연월").set_index("연월")[["전체방문자수", "신규방문자", "재방문자"]])
    
    display_df = summary_df.copy()
    display_df['매출액'] = display_df['매출액'].apply(lambda x: f"{x:,}원")
    for col in pct_format_cols:
        display_df[col] = display_df[col].apply(lambda x: f"{x}%")
    st.dataframe(display_df, use_container_width=True)

else:
    st.error(f"오류: {status}")