import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from psycopg2.extras import RealDictCursor

def create_connection():
    return psycopg2.connect(
        host='your_greenplum_host',
        database='your_database',
        user='your_username',
        password='your_password',
        port='5432'
    )

def get_production_data(conn, start_date, end_date, data_type='actual'):
    query = """
    SELECT 
        inch,
        line,
        nano,
        date_trunc('month', month)::date as month,
        production_amount
    FROM semiconductor_production
    WHERE month BETWEEN %s AND %s
    AND data_type = %s
    ORDER BY inch, line, nano, month
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (start_date, end_date, data_type))
        return cur.fetchall()

def process_data(data):
    df = pd.DataFrame(data)
    
    # 전체 합계 계산
    total_by_inch = df.groupby(['inch', 'line', 'month'])['production_amount'].sum().reset_index()
    total_by_inch['nano'] = '전체'
    
    # 라인별 합계 계산
    total_by_line = df.groupby(['inch', 'line', 'month'])['production_amount'].sum().reset_index()
    total_by_line['nano'] = '전체'
    
    # 모든 데이터 합치기
    result_df = pd.concat([total_by_inch, total_by_line, df])
    
    # 피벗 테이블 생성
    pivot_df = result_df.pivot_table(
        index=['inch', 'line', 'nano'],
        columns='month',
        values='production_amount',
        aggfunc='first'
    ).reset_index()
    
    # 정렬
    pivot_df['sort_order'] = (
        pivot_df['inch'].map({'12인치': 0, '8인치': 1}) * 1000 +
        pivot_df['nano'].map({'전체': 0}).fillna(1) * 100
    )
    pivot_df = pivot_df.sort_values('sort_order').drop('sort_order', axis=1)
    
    return pivot_df

def main():
    st.set_page_config(page_title="반도체 생산 현황", layout="wide")
    st.title("반도체 생산 현황 대시보드")
    
    # 기간 선택
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("시작일", pd.to_datetime("2024-01-01"))
    with col2:
        end_date = st.date_input("종료일", pd.to_datetime("2024-06-30"))
    
    try:
        conn = create_connection()
        
        # 실제 데이터와 예측 데이터 조회
        actual_data = get_production_data(conn, start_date, end_date, 'actual')
        predicted_data = get_production_data(conn, start_date, end_date, 'predicted')
        
        # 데이터 처리
        actual_df = process_data(actual_data)
        predicted_df = process_data(predicted_data)
        
        # 필터링 옵션
        st.sidebar.header("필터링 옵션")
        inch_filter = st.sidebar.multiselect(
            "인치 선택",
            actual_df['inch'].unique(),
            default=actual_df['inch'].unique()
        )
        line_filter = st.sidebar.multiselect(
            "라인 선택",
            actual_df['line'].unique(),
            default=actual_df['line'].unique()
        )
        
        # 필터링 적용
        actual_filtered = actual_df[
            actual_df['inch'].isin(inch_filter) &
            actual_df['line'].isin(line_filter)
        ]
        predicted_filtered = predicted_df[
            predicted_df['inch'].isin(inch_filter) &
            predicted_df['line'].isin(line_filter)
        ]
        
        # 데이터 표시
        st.header("실제 생산량")
        st.dataframe(
            actual_filtered.style.format({col: "{:.1f}" for col in actual_filtered.columns if isinstance(col, pd.Timestamp)}),
            use_container_width=True
        )
        
        st.header("예측 생산량")
        st.dataframe(
            predicted_filtered.style.format({col: "{:.1f}" for col in predicted_filtered.columns if isinstance(col, pd.Timestamp)}),
            use_container_width=True
        )
        
        # 차트 데이터 준비
        numeric_columns = [col for col in actual_filtered.columns if isinstance(col, pd.Timestamp)]
        chart_data = pd.DataFrame({
            '월': numeric_columns * 2,
            '구분': ['실제'] * len(numeric_columns) + ['예측'] * len(numeric_columns),
            '평균 생산량': (
                [actual_filtered[col].mean() for col in numeric_columns] +
                [predicted_filtered[col].mean() for col in numeric_columns]
            )
        })
        
        # 차트 표시
        st.header("생산량 추이 비교")
        fig = px.line(
            chart_data,
            x='월',
            y='평균 생산량',
            color='구분',
            title='월별 평균 생산량 비교'
        )
        
        fig.update_layout(
            height=500,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"오류가 발생했습니다: {str(e)}")
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
