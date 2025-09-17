import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import joblib
from sqlalchemy import create_engine
import os
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.tsa.statespace.sarimax import SARIMAX

# String koneksi database
engine = create_engine("postgresql://postgres:admin@localhost:5433/db_perumda")

# Path untuk menyimpan hasil forecast (format pickle)
CACHE_PATH = "cache/forecast.pkl"
MODEL_PATH = "models/"

# Fungsi untuk mengambil data dari database
@st.cache_data
def load_data():
    query = """
        SELECT t.*, w.year, w.month, p.wilayah
        FROM fact_transaksi t
        JOIN dim_waktu w ON t.id_waktu = w.id_waktu
        JOIN dim_pelanggan p ON t.kodepelanggan = p.kodepelanggan
    """
    return pd.read_sql(query, engine)

# Fungsi untuk agregasi data keseluruhan
def aggregate_overall(data):
    return data.groupby(['year', 'month']).agg({
        'tagihan': 'sum',
        'pemakaian': 'sum',
        'denda': 'sum',
        'jumlahbayar': 'sum'
    }).reset_index()

# Fungsi untuk agregasi data per wilayah
def aggregate_by_region(data):
    return data.groupby(['year', 'month', 'wilayah']).agg({
        'tagihan': 'sum',
        'pemakaian': 'sum',
        'denda': 'sum',
        'jumlahbayar': 'sum'
    }).reset_index()

# Fungsi untuk melatih ulang model SARIMAX
def train_sarimax(data):
    # Keseluruhan
    agg_overall = aggregate_overall(data)
    agg_overall['date'] = pd.to_datetime(agg_overall[['year', 'month']].assign(day=1))
    agg_overall.set_index('date', inplace=True)

    model_overall = SARIMAX(
        agg_overall['jumlahbayar'],
        order=(2, 1, 1),
        seasonal_order=(1, 1, 1, 12),
        exog=agg_overall[['denda', 'tagihan']]
    ).fit()
    joblib.dump(model_overall, os.path.join(MODEL_PATH, "sarimax_overall.pkl"))

    # Per wilayah
    agg_by_region = aggregate_by_region(data)
    agg_by_region['date'] = pd.to_datetime(agg_by_region[['year', 'month']].assign(day=1))
    agg_by_region.set_index('date', inplace=True)

    for wilayah in ['PS', 'SN', 'UT']:
        region_data = agg_by_region[agg_by_region['wilayah'] == wilayah]
        model_region = SARIMAX(
            region_data['jumlahbayar'],
            order=(2, 1, 1),
            seasonal_order=(1, 1, 1, 12),
            exog=region_data[['denda', 'tagihan']]
        ).fit()
        joblib.dump(model_region, os.path.join(MODEL_PATH, f"sarimax_{wilayah}.pkl"))

# Fungsi untuk melakukan forecasting SARIMA
def sarimax_forecast(data, model_path, steps=6):
    model = joblib.load(model_path)
    last_exog = data[['denda', 'tagihan']].iloc[-steps:]
    forecast = model.forecast(steps=steps, exog=last_exog)
    return forecast

def plot_overall(data, forecast):
    fig = go.Figure()

    # Data historis
    fig.add_trace(go.Scatter(
        x=data.index[-12:], 
        y=data['jumlahbayar'][-12:], 
        mode='lines', 
        name='Actual'
    ))

    # Gabungkan titik terakhir dengan prediksi
    future_dates = pd.date_range(start=data.index[-1], periods=7, freq='MS')
    combined_values = np.concatenate([[data['jumlahbayar'].iloc[-1]], forecast])

    # Forecast SARIMAX
    fig.add_trace(go.Scatter(
        x=future_dates, 
        y=combined_values, 
        mode='lines', 
        name='Forecast', 
        line=dict(dash='dot', color='red')
    ))

    fig.update_layout(title="Prediksi Pendapatan Keseluruhan", xaxis_title="Waktu", yaxis_title="Jumlah Bayar")
    
    return fig

def plot_all_regions(data, forecasts):
    fig = go.Figure()
    wilayahs = ['PS', 'SN', 'UT']

    # Plot data historis
    for wilayah in wilayahs:
        region_data = data[data['wilayah'] == wilayah]
        fig.add_trace(go.Scatter(
            x=region_data.index[-12:], 
            y=region_data['jumlahbayar'][-12:], 
            mode='lines', 
            name=f'Actual {wilayah}'
        ))

        # Gabungkan titik terakhir dengan prediksi SARIMAX
        future_dates = pd.date_range(start=region_data.index[-1], periods=7, freq='MS')
        combined_values = np.concatenate([[region_data['jumlahbayar'].iloc[-1]], forecasts[wilayah]])

        fig.add_trace(go.Scatter(
            x=future_dates, 
            y=combined_values, 
            mode='lines', 
            name=f'Forecast {wilayah}',
            line=dict(dash='dot')
        ))

    fig.update_layout(title="Prediksi Pendapatan Per Wilayah", xaxis_title="Waktu", yaxis_title="Jumlah Bayar")

    return fig

# Fungsi untuk menyimpan hasil forecasting
def save_forecast(forecast):
    # Pastikan folder cache ada
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    joblib.dump(forecast, CACHE_PATH)


# Fungsi untuk load forecasting yang sudah tersimpan
def load_cached_forecast():
    if not os.path.exists(CACHE_PATH):
        return None  
    return joblib.load(CACHE_PATH)

def show_forecasting():
    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Prediksi Pendapatan</h2>", unsafe_allow_html=True)

    col1, col2 = st.columns([1.5,8])

    with col1:
        if st.button("Perbarui Model"):
            data = load_data()
            train_sarimax(data)
            st.success("Model berhasil dilatih ulang!")
            st.rerun()
            st.cache_data.clear() 

    with col2:
        if st.button("Mulai Prediksi"):
            data = load_data()

            agg_overall = aggregate_overall(data)
            agg_overall['date'] = pd.to_datetime(agg_overall[['year', 'month']].assign(day=1))
            agg_overall.set_index('date', inplace=True)

            agg_by_region = aggregate_by_region(data)
            agg_by_region['date'] = pd.to_datetime(agg_by_region[['year', 'month']].assign(day=1))
            agg_by_region.set_index('date', inplace=True)

            forecast_overall = sarimax_forecast(agg_overall, os.path.join(MODEL_PATH, "sarimax_overall.pkl"))
            forecast_ps = sarimax_forecast(agg_by_region[agg_by_region['wilayah'] == 'PS'], os.path.join(MODEL_PATH, "sarimax_PS.pkl"))
            forecast_sn = sarimax_forecast(agg_by_region[agg_by_region['wilayah'] == 'SN'], os.path.join(MODEL_PATH, "sarimax_SN.pkl"))
            forecast_ut = sarimax_forecast(agg_by_region[agg_by_region['wilayah'] == 'UT'], os.path.join(MODEL_PATH, "sarimax_UT.pkl"))

            forecast_results = {
                'overall': forecast_overall,
                'region': {'PS': forecast_ps, 'SN': forecast_sn, 'UT': forecast_ut}
            }
            save_forecast(forecast_results)

        else:
            forecast_results = load_cached_forecast()

    if forecast_results:
        data = load_data()

        agg_overall = aggregate_overall(data)
        agg_overall['date'] = pd.to_datetime(agg_overall[['year', 'month']].assign(day=1))
        agg_overall.set_index('date', inplace=True)

        agg_by_region = aggregate_by_region(data)
        agg_by_region['date'] = pd.to_datetime(agg_by_region[['year', 'month']].assign(day=1))
        agg_by_region.set_index('date', inplace=True)

        col1, col2 = st.columns(2)

        with col1:
            st.plotly_chart(plot_overall(agg_overall, forecast_results['overall']))

        with col2:
            st.plotly_chart(plot_all_regions(agg_by_region, forecast_results['region']))
