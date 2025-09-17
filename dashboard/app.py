# app.py
import streamlit as st
import pandas as pd
import plotly.express as px

# -------- Data Dummy -------- #
# dim_pelanggan
dim_pelanggan = pd.DataFrame({
    'id_pelanggan': [1, 2, 3],
    'nama_pelanggan': ['Aldi', 'Budi', 'Cici'],
    'kota': ['Jakarta', 'Bandung', 'Surabaya']
})

# dim_barang
dim_barang = pd.DataFrame({
    'id_barang': [101, 102, 103],
    'nama_barang': ['Pulpen', 'Pensil', 'Penghapus'],
    'kategori': ['ATK', 'ATK', 'ATK'],
    'harga': [3000, 2000, 1500]
})

# dim_waktu
dim_waktu = pd.DataFrame({
    'id_waktu': [1, 2, 3],
    'tanggal': pd.to_datetime(['2024-01-10', '2024-02-15', '2024-03-05']),
    'bulan': [1, 2, 3],
    'tahun': [2024, 2024, 2024]
})

# fact_transaksi
fact_transaksi = pd.DataFrame({
    'id_transaksi': [1001, 1002, 1003],
    'id_pelanggan': [1, 2, 3],
    'id_barang': [101, 102, 103],
    'id_waktu': [1, 2, 3],
    'jumlah': [2, 3, 4]
})

# Hitung total_harga
fact_transaksi = fact_transaksi.merge(dim_barang[['id_barang', 'harga']], on='id_barang', how='left')
fact_transaksi['total_harga'] = fact_transaksi['jumlah'] * fact_transaksi['harga']

# Gabungkan semua data
df_all = (
    fact_transaksi
    .merge(dim_pelanggan, on='id_pelanggan')
    .merge(dim_barang[['id_barang', 'nama_barang', 'kategori']], on='id_barang')
    .merge(dim_waktu, on='id_waktu')
)

# -------- Tampilan Streamlit -------- #
st.set_page_config(layout="wide")
st.markdown("# 📊 Dashboard Penjualan Barang Alat Tulis")
st.markdown("Visualisasi data dummy transaksi yang terdiri dari pelanggan, barang, dan waktu.")

# Expand untuk melihat data
with st.expander("🧾 Lihat Data Gabungan"):
    st.dataframe(df_all)

# -------- Visualisasi -------- #
col1, col2 = st.columns(2)

with col1:
    st.subheader("📅 Jumlah Transaksi per Bulan")
    
    # Gabungkan bulan dan tahun jadi datetime
    trx_per_bulan = (
        df_all
        .groupby(['bulan', 'tahun'])['jumlah']
        .sum()
        .reset_index()
    )
    
    trx_per_bulan['periode'] = pd.to_datetime(
        trx_per_bulan.rename(columns={'tahun': 'year', 'bulan': 'month'})[['year', 'month']].assign(day=1)
    )
    
    fig1 = px.line(trx_per_bulan, x='periode', y='jumlah', markers=True,
                   labels={'jumlah': 'Jumlah Transaksi', 'periode': 'Periode'},
                   title="Jumlah Transaksi per Bulan")
    
    fig1.update_xaxes(dtick="M1", tickformat="%b-%Y")
    st.plotly_chart(fig1, use_container_width=True)


with col2:
    st.subheader("🛒 Total Penjualan per Kategori")
    penjualan_per_kategori = df_all.groupby('kategori')['total_harga'].sum().reset_index()
    fig2 = px.pie(penjualan_per_kategori, names='kategori', values='total_harga',
                  title="Total Penjualan per Kategori")
    st.plotly_chart(fig2, use_container_width=True)
