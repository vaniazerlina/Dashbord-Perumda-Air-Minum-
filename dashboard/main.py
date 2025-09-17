import sys
import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go 
import proses 

# Tambahkan path ke folder utama agar bisa mengakses ETL/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

Image.MAX_IMAGE_PIXELS = None

# Koneksi ke database test_perumda
DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
engine = create_engine(DATABASE_URL)

# Fungsi untuk mengambil data pelanggan
def load_data():
    dim_pelanggan = pd.read_sql("SELECT * FROM dim_pelanggan", engine)
    fact_transaksi = pd.read_sql("SELECT * FROM fact_transaksi", engine)
    dim_goltarif = pd.read_sql("SELECT * FROM dim_goltarif", engine)
    dim_waktu = pd.read_sql("SELECT * FROM dim_waktu", engine)
    dim_jenispengaduan = pd.read_sql("SELECT * FROM dim_jenispengaduan", engine)
    fact_pengaduan = pd.read_sql("SELECT * FROM fact_pengaduan", engine)
    dim_realisasi = pd.read_sql("SELECT * FROM dim_realisasi", engine)
    fact_pemutusan = pd.read_sql("SELECT * FROM fact_pemutusan", engine)
    fact_sbbaru = pd.read_sql("SELECT * FROM fact_sbbaru", engine)

    return dim_pelanggan, fact_transaksi, dim_goltarif, dim_waktu, dim_jenispengaduan, fact_pengaduan, dim_realisasi, fact_pemutusan, fact_sbbaru

@st.cache_data
def load_data_cached():
    return load_data()

def show_dashboard_pelanggan():
    dim_pelanggan, _, _, _, _, _, _, _, _ = load_data_cached()

    # Cek apakah data pelanggan kosong
    if dim_pelanggan.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Sidebar untuk memilih status pelanggan
    st.sidebar.markdown("**Pilih Status Pelanggan**")
    status_options =dim_pelanggan['status'].unique()
    status_selected = {}
    for status in status_options:
        status_selected[status] = st.sidebar.checkbox(status, value=True)
    pilihan_status = [s for s, v in status_selected.items() if v]

    # Sidebar untuk memilih wilayah
    st.sidebar.markdown("**Pilih Wilayah**")
    wilayah_options = dim_pelanggan['wilayah'].unique()
    wilayah_selected = {}
    for wilayah in wilayah_options:
        wilayah_selected[wilayah] = st.sidebar.checkbox(wilayah, value=True)
    pilihan_wilayah = [w for w, v in wilayah_selected.items() if v]

    # Filter data berdasarkan status dan wilayah yang dipilih
    dim_pelanggan = dim_pelanggan[dim_pelanggan['status'].isin(pilihan_status) & dim_pelanggan['wilayah'].isin(pilihan_wilayah)]
    
    # Total jumlah pelanggan setelah filter
    total_pelanggan = dim_pelanggan['kodepelanggan'].nunique()
    
    # Jumlah wilayah setelah filter
    wilayah_manajemen = dim_pelanggan['wilayah'].nunique()
    
    # Jumlah pelanggan berdasarkan status setelah filter
    status_counts = dim_pelanggan['status'].value_counts()
    status_labels = {"A": "Aktif", "N": "Nonaktif", "P": "Proses"}
    status_counts.index = [status_labels.get(s, s) for s in status_counts.index]

    # Jumlah pelanggan berdasarkan wilayah setelah filter
    pelanggan_per_wilayah = dim_pelanggan['wilayah'].value_counts()
    wilayah_labels = {"UT": "Utara", "PS": "Pusat", "SN": "Selatan"}
    pelanggan_per_wilayah.index = [wilayah_labels.get(w, w) for w in pelanggan_per_wilayah.index]

    # Layout Judul dan Navigasi
    col1, col2, col3 = st.columns([1, 8, 1])

    with col1:
        if st.button("⬅", key="prev_dashboard"):
            st.session_state["slide"] = "sbbaru"
            st.rerun()

    with col2:
        st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pelanggan</h2>", unsafe_allow_html=True)

    with col3:
        if st.button("➡", key="next_dashboard"):
            st.session_state["slide"] = "pemakaian_air"
            st.rerun()
    
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pelanggan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pelanggan:,}</p>", 
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Jumlah Wilayah</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{wilayah_manajemen}</p>", 
            unsafe_allow_html=True
        )

    col1, col2 = st.columns(2)

    with col1:
        fig = px.pie(
            names=status_counts.index, 
            values=status_counts.values,
            hole=0.3,
            color_discrete_sequence=px.colors.sequential.Blues_r
        )

        fig.update_traces(
            textinfo="label+percent",
            textfont_size=12
        )

        fig.update_layout(
            title="Distribusi Status Pelanggan",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#000000"),
            legend=dict(yanchor="middle", y=0.5, xanchor="right", x=1)  # Posisikan legend lebih dekat ke pie chart
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            x=pelanggan_per_wilayah.index, 
            y=pelanggan_per_wilayah.values,
            labels={"x": "Wilayah", "y": "Jumlah Pelanggan"},
            color_discrete_sequence=px.colors.sequential.Blues_r
        )

        fig.update_traces(texttemplate='%{y}', textposition="outside")

        fig.update_layout(
            title="Jumlah Pelanggan per Wilayah",
            xaxis_title="Wilayah",
            yaxis_title="Jumlah Pelanggan",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#000000"),
            xaxis=dict(tickangle=0)  # Tidak memiringkan label wilayah
        )

        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_pemakaian_air():
    _, fact_transaksi, dim_goltarif, dim_waktu, _, _, _, _, _ = load_data_cached()
    
    # Cek apakah data fact_transaksi kosong
    if fact_transaksi.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Gabungkan dengan dimensi golongan tarif dan waktu
    df = fact_transaksi.merge(dim_goltarif, on="kodegoltarif", how="left")
    df = df.merge(dim_waktu, on="id_waktu", how="left")

    st.markdown(
    """<style>
    label[data-baseweb="radio"] div {font-size: 14px !important; font-weight: bold;}
    </style>""",
    unsafe_allow_html=True,
    )

    pilihan_waktu = st.sidebar.radio("**Pilih Rentang Waktu**", ["Semua Data", "Pilih Tahun"])


    if pilihan_waktu == "Semua Data":
        tahun_pilihan = sorted(df["year"].unique())
        tahun_awal, tahun_akhir = min(tahun_pilihan), max(tahun_pilihan)
    else:
        tahun_awal, tahun_akhir = st.sidebar.select_slider("Rentang Tahun", options=sorted(df["year"].unique()), value=(min(df["year"]), max(df["year"])))
    
    df_filtered = df[(df["year"] >= tahun_awal) & (df["year"] <= tahun_akhir)]

    # Hitung metrik utama
    total_pemakaian = df_filtered["pemakaian"].sum()
    jumlah_pelanggan = df_filtered["kodepelanggan"].nunique()  # Menghitung jumlah pelanggan unik
    rata_rata_pemakaian = total_pemakaian / jumlah_pelanggan if jumlah_pelanggan > 0 else 0

    # Hitung total pemakaian per tahun
    pemakaian_per_tahun = df_filtered.groupby("year")["pemakaian"].sum().reset_index()
    pemakaian_per_tahun["year"] = pemakaian_per_tahun["year"].astype(str)  

    # Hitung jumlah pelanggan unik per tahun
    pelanggan_per_tahun = df_filtered.groupby("year")["kodepelanggan"].nunique().reset_index()
    pelanggan_per_tahun["year"] = pelanggan_per_tahun["year"].astype(str)  

    # Gabungkan data pemakaian dan jumlah pelanggan per tahun
    rata_rata_per_tahun = pemakaian_per_tahun.merge(pelanggan_per_tahun, on="year")

    # Hitung rata-rata pemakaian per pelanggan per tahun
    rata_rata_per_tahun["rata_rata_pemakaian"] = (
        rata_rata_per_tahun["pemakaian"] / rata_rata_per_tahun["kodepelanggan"]
    )

    # Gunakan hanya kolom yang dibutuhkan untuk visualisasi
    rata_rata_per_tahun = rata_rata_per_tahun[["year", "rata_rata_pemakaian"]] 

    pemakaian_per_goltarif = df_filtered.groupby("kodegoltarif")["pemakaian"].sum().reset_index()
    pemakaian_per_bulan = df_filtered.groupby("month")["pemakaian"].sum().reset_index()

    col1, col2, col3 = st.columns([1, 8, 1])
    
    with col1:
        if st.button("⬅", key="prev_dashboard"):
            st.session_state["slide"] = "pelanggan"
            st.rerun()
    
    with col2:
        st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pemakaian Air</h2>", unsafe_allow_html=True)
    
    with col3:
        if st.button("➡", key="next_dashboard"):
            st.session_state["slide"] = "pendapatan"
            st.rerun()
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pemakaian Air</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pemakaian:,.0f} m³</p>", 
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Rata-rata Pemakaian Air</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{rata_rata_pemakaian:.2f} m³</p>", 
            unsafe_allow_html=True
        )

    col1, col2 = st.columns([4, 4])

    with col1:
        if len(pemakaian_per_tahun) < 5:
            pemakaian_per_tahun = pemakaian_per_tahun.tail(5)
        fig = px.bar(
        pemakaian_per_tahun, 
        x="year", 
        y="pemakaian", 
        text="pemakaian",
        labels={"year": "Tahun", "pemakaian": "Total Pemakaian (m³)"},
        color_discrete_sequence=["#0073E6"]
        )
        fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig.update_layout(
            title="Total Pemakaian Air per Tahun",
            xaxis_title="Tahun",
            yaxis_title="Total Pemakaian (m³)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(tickmode="array", tickvals=pemakaian_per_tahun["year"]),
            yaxis=dict(range=[0, pemakaian_per_tahun["pemakaian"].max() * 1.1]) if pemakaian_per_tahun["pemakaian"].min() > 0 else None
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if len(rata_rata_per_tahun) > 5:
            rata_rata_per_tahun = rata_rata_per_tahun.tail(5)

        # Buat plot garis untuk rata-rata pemakaian per tahun
        fig = px.line(
            rata_rata_per_tahun, 
            x="year", 
            y="rata_rata_pemakaian",  # Gunakan kolom yang sudah diperbaiki
            text="rata_rata_pemakaian",
            markers=True,
            labels={"year": "Tahun", "rata_rata_pemakaian": "Rata-rata Pemakaian (m³)"},
            line_shape="linear",
            color_discrete_sequence=["#0073E6"]  # Warna biru khas
        )

        fig.update_traces(texttemplate='%{text:.2f}', textposition="top center")

        fig.update_layout(
            title="Rata-rata Pemakaian Air per Tahun",
            xaxis_title="Tahun",
            yaxis_title="Rata-rata Pemakaian (m³)",
            plot_bgcolor="rgba(0,0,0,0)",  # Background transparan
            xaxis=dict(tickmode="array", tickvals=rata_rata_per_tahun["year"]),
            yaxis=dict(range=[0, rata_rata_per_tahun["rata_rata_pemakaian"].max() * 1.1]) 
            if rata_rata_per_tahun["rata_rata_pemakaian"].min() > 0 else None
        )

        st.plotly_chart(fig, use_container_width=True)


    col1, col2 = st.columns([4, 4])

    with col1:
        fig = go.Figure()
        blue_shades = px.colors.sequential.Blues[2:]  # Skip warna putih dan biru pucat
        
        for i, year in enumerate(sorted(df_filtered["year"].unique())):
            df_year = df_filtered[df_filtered["year"] == year].groupby("month")["pemakaian"].sum().reset_index()
            fig.add_trace(go.Scatter(
                x=df_year["month"], 
                y=df_year["pemakaian"], 
                mode="lines+markers+text",
                text=df_year["pemakaian"].apply(lambda x: f"{x:,.0f}"),
                textposition="top center",
                name=str(year),
                line=dict(color=blue_shades[i % len(blue_shades)])  # Ambil warna biru berbeda tiap tahun
            ))

        fig.update_layout(
            title="Pemakaian Air per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Pemakaian Air (m³)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                tickmode="array",
                tickvals=list(range(1, 13)),
                ticktext=["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]
            ),
            font=dict(color="#003366")
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            pemakaian_per_goltarif, 
            y="kodegoltarif",  
            x="pemakaian", 
            text="pemakaian",
            labels={"kodegoltarif": "Kode Golongan Tarif", "pemakaian": "Total Pemakaian (m³)"},
            color_discrete_sequence=["#0073E6"],  # Warna biru khas
            orientation="h" 
        )

        fig.update_traces(texttemplate='%{text:,.0f}', textposition="outside")

        fig.update_layout(
            title="Pemakaian Air per Golongan Tarif",
            xaxis_title="Total Pemakaian (m³)",
            yaxis_title="Kode Golongan Tarif",
            plot_bgcolor="rgba(0,0,0,0)",  # Background transparan
            yaxis=dict(showgrid = False, categoryorder="total ascending"),  # Urutkan dari besar ke kecil
            font=dict(color="#003366")  # Warna teks biru tua
        )

        st.plotly_chart(fig, use_container_width=True)
        
def show_dashboard_pendapatan():
    dim_pelanggan, fact_transaksi, _, dim_waktu, _, _, _, _, _ = load_data_cached()
    
    # Cek apakah data fact_transaksi kosong
    if fact_transaksi.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Gabungkan fact_transaksi dengan dim_waktu dan dim_pelanggan
    fact_transaksi = fact_transaksi.merge(dim_waktu, on="id_waktu", how="left")
    fact_transaksi = fact_transaksi.merge(dim_pelanggan, on="kodepelanggan", how="left")
    
    st.markdown(
    """<style>
    label[data-baseweb="radio"] div {font-size: 14px !important; font-weight: bold;}
    </style>""",
    unsafe_allow_html=True,
    )
    
    pilihan_waktu = st.sidebar.radio("**Pilih Rentang Waktu**", ["Semua Data", "Pilih Tahun"])
    
    if pilihan_waktu == "Pilih Tahun":
        tahun_min, tahun_max = st.sidebar.select_slider(
            "", 
            options=sorted(fact_transaksi["year"].unique()),
            value=(fact_transaksi["year"].min(), fact_transaksi["year"].max())
        )
        fact_transaksi = fact_transaksi[
            (fact_transaksi["year"] >= tahun_min) & (fact_transaksi["year"] <= tahun_max)
        ]
    
    # **FILTER BERDASARKAN WILAYAH**
    pilihan_wilayah = st.sidebar.radio("**Pilih Wilayah**", ["Semua Wilayah", "Pilih Wilayah"])
    
    if pilihan_wilayah == "Pilih Wilayah":
        wilayah_terpilih = []
        col1, col2, col3 = st.sidebar.columns(3)
        with col1:
            if st.checkbox("PS", value=True):
                wilayah_terpilih.append("PS")
        with col2:
            if st.checkbox("SN", value=True):
                wilayah_terpilih.append("SN")
        with col3:
            if st.checkbox("UT", value=True):
                wilayah_terpilih.append("UT")
        
        if wilayah_terpilih:
            fact_transaksi = fact_transaksi[fact_transaksi["wilayah"].isin(wilayah_terpilih)]
    
    # **PERHITUNGAN TOTAL DIPINDAH KE SINI, SETELAH DATA DIFILTER**
    total_tagihan = fact_transaksi['tagihan'].sum()
    total_denda = fact_transaksi['denda'].sum()
    total_pendapatan = total_tagihan + total_denda

    # Hitung pendapatan berdasarkan tahun
    pendapatan_per_tahun = fact_transaksi.groupby("year")["jumlahbayar"].sum().reset_index()
    pendapatan_per_tahun["year"] = pendapatan_per_tahun["year"].astype(str)
    tagihan_per_tahun = fact_transaksi.groupby("year")["tagihan"].sum().reset_index()
    denda_per_tahun = fact_transaksi.groupby("year")["denda"].sum().reset_index()

    # Hitung pendapatan berdasarkan bulan untuk rentang tahun yang dipilih
    tagihan_per_bulan = fact_transaksi.groupby(["year", "month"])["tagihan"].sum().reset_index()
    denda_per_bulan = fact_transaksi.groupby(["year", "month"])["denda"].sum().reset_index()
    pendapatan_per_bulan = fact_transaksi.groupby(["year", "month"])["jumlahbayar"].sum().reset_index()

    # Hitung pendapatan berdasarkan golongan tarif untuk rentang tahun yang dipilih
    denda_per_goltarif = fact_transaksi.groupby("kodegoltarif")["denda"].sum().reset_index()
    pendapatan_per_goltarif = fact_transaksi.groupby("kodegoltarif")["jumlahbayar"].sum().reset_index()
    pendapatan_per_wilayah = fact_transaksi.groupby("wilayah")["jumlahbayar"].sum().reset_index()
    
    col1, col2, col3 = st.columns([1, 8, 1])
    
    with col1:
        if st.button("⬅", key="prev_dashboard"):
            st.session_state["slide"] = "pemakaian_air"
            st.rerun()
    
    with col2:
        st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pendapatan</h2>", unsafe_allow_html=True)
    
    with col3:
        if st.button("➡", key="next_dashboard"):
            st.session_state["slide"] = "pengaduan"
            st.rerun()
    
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pendapatan dari Tagihan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>Rp {total_tagihan:,.0f}</p>", 
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pendapatan dari Denda</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>Rp {total_denda:,.0f}</p>", 
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pendapatan Keseluruhan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>Rp {total_pendapatan:,.0f}</p>", 
            unsafe_allow_html=True
        )

    col1, col2, col3 = st.columns(3)

    with col1:
        fig = px.bar(
            tagihan_per_tahun, 
            x="year", 
            y="tagihan", 
            text_auto=".2s",
            color_discrete_sequence=["#6A80B9"]
        )
        fig.update_layout(
            title=("Tagihan per Tahun"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Tagihan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            denda_per_tahun, 
            x="year", 
            y="denda", 
            text_auto=".2s",
            color_discrete_sequence=["#155E95"]
        )
        fig.update_layout(
            title=("Denda per Tahun"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Denda",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        fig = px.bar(
            pendapatan_per_tahun, 
            x="year", 
            y="jumlahbayar", 
            text_auto=".2s",
            color_discrete_sequence=["#091057"]
        )
        fig.update_layout(
            title=("Pendapatan per Tahun"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Pendapatan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Mapping angka bulan ke nama bulan
    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
        7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
    }

    # Ganti angka bulan dengan nama bulan
    tagihan_per_bulan["month"] = tagihan_per_bulan["month"].map(month_map)
    denda_per_bulan["month"] = denda_per_bulan["month"].map(month_map)
    pendapatan_per_bulan["month"] = pendapatan_per_bulan["month"].map(month_map)

    blue_shades = px.colors.sequential.Blues[2:]  # Skip warna putih dan biru pucat

    col1, col2, col3 = st.columns(3)

    with col1:
        fig = go.Figure()
        years = sorted(tagihan_per_bulan["year"].unique())  # Urutkan tahun
        for i, year in enumerate(years):
            df_year = tagihan_per_bulan[tagihan_per_bulan["year"] == year]
            fig.add_trace(go.Scatter(
                x=df_year["month"], 
                y=df_year["tagihan"], 
                mode="lines+markers+text",
                text=df_year["tagihan"].apply(lambda x: f"{x:,.0f}"),
                textposition="top center",
                name=str(year),
                line=dict(color=blue_shades[i % len(blue_shades)])  # Ambil warna biru secara siklikal
            ))
        fig.update_layout(
            title="Tagihan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Tagihan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(categoryorder="array", categoryarray=list(month_map.values())),
            yaxis=dict(range=[0, denda_per_bulan["denda"].max()])
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = go.Figure()
        years = sorted(denda_per_bulan["year"].unique())
        for i, year in enumerate(years):
            df_year = denda_per_bulan[denda_per_bulan["year"] == year]
            fig.add_trace(go.Scatter(
                x=df_year["month"], 
                y=df_year["denda"], 
                mode="lines+markers+text",
                text=df_year["denda"].apply(lambda x: f"{x:,.0f}"),
                textposition="top center",
                name=str(year),
                line=dict(color=blue_shades[i % len(blue_shades)])
            ))
        fig.update_layout(
            title="Denda per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Denda",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(categoryorder="array", categoryarray=list(month_map.values())),
            yaxis=dict(range=[0, denda_per_bulan["denda"].max()])
        )
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        fig = go.Figure()
        years = sorted(pendapatan_per_bulan["year"].unique())
        for i, year in enumerate(years):
            df_year = pendapatan_per_bulan[pendapatan_per_bulan["year"] == year]
            fig.add_trace(go.Scatter(
                x=df_year["month"], 
                y=df_year["jumlahbayar"], 
                mode="lines+markers+text",
                text=df_year["jumlahbayar"].apply(lambda x: f"{x:,.0f}"),
                textposition="top center",
                name=str(year),
                line=dict(color=blue_shades[i % len(blue_shades)])
            ))
        fig.update_layout(
            title="Pendapatan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Pendapatan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(categoryorder="array", categoryarray=list(month_map.values())),
            yaxis=dict(range=[0, denda_per_bulan["denda"].max()]),
        )
        st.plotly_chart(fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        fig = px.bar(
            denda_per_goltarif, 
            y="kodegoltarif",  # Kategori di sumbu Y agar grafik horizontal
            x="denda",  # Nilai di sumbu X
            text_auto=True,
            color_discrete_sequence=["#155E95"],
            orientation="h"  # Mengubah orientasi menjadi horizontal
        )
        fig.update_traces(textposition="outside")  # Menampilkan angka di luar batang
        fig.update_layout(
            title="Denda per Golongan Tarif",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Denda",
            yaxis_title="Golongan Tarif",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(showgrid=False),  # Hilangkan grid untuk tampilan lebih bersih
            yaxis=dict(categoryorder="total ascending")  # Urutkan dari besar ke kecil
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            pendapatan_per_goltarif, 
            y="kodegoltarif",  # Kategori di sumbu Y agar grafik horizontal
            x="jumlahbayar",  # Nilai di sumbu X
            text_auto=True,
            color_discrete_sequence=["#091057"],
            orientation="h"  # Mengubah orientasi menjadi horizontal
        )
        fig.update_traces(textposition="outside")  # Menampilkan angka di luar batang
        fig.update_layout(
            title="Pendapatan per Golongan Tarif",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Pendapatan",
            yaxis_title="Golongan Tarif",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(showgrid=False),  # Hilangkan grid
            yaxis=dict(categoryorder="total ascending")  # Urutkan dari besar ke kecil
        )
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        fig = px.bar(
            pendapatan_per_wilayah,
            x="wilayah",  # Sumbu X: Wilayah
            y="jumlahbayar",  # Sumbu Y: Total Pembayaran
            text="jumlahbayar",  # Menampilkan nilai pada batang
            labels={"wilayah": "Wilayah", "jumlahbayar": "Total Pendapatan"},
            color_discrete_sequence=["dodgerblue"]  # Warna batang biru khas
        )

        # Format angka pada label
        fig.update_traces(texttemplate='%{text:,.0f}', textposition="outside")

        # Mengatur tampilan layout
        fig.update_layout(
            title="Pendapatan per Wilayah",
            xaxis_title="Wilayah",
            yaxis_title="Total Pendapatan",
            plot_bgcolor="rgba(0,0,0,0)",  # Background transparan
            yaxis=dict(gridcolor="#CCCCCC"),  # Warna grid abu-abu
            font=dict(color="#003366")  # Warna teks biru tua
        )

        # Menampilkan chart di Streamlit
        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_pengaduan():
    dim_pelanggan, _, _, dim_waktu, dim_jenispengaduan, fact_pengaduan, _, _, _ = load_data_cached()

    # Cek apakah data fact_pengaduan kosong
    if fact_pengaduan.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return

    fact_pengaduan = fact_pengaduan.merge(dim_waktu, on="id_waktu", how="left")
    fact_pengaduan = fact_pengaduan.merge(dim_jenispengaduan, on="id_jenispengaduan", how="left")
    fact_pengaduan = fact_pengaduan.merge(dim_pelanggan, left_on="idpelanggan", right_on="kodepelanggan", how="left")
    fact_pengaduan["year"] = fact_pengaduan["year"].astype(str)  # Pastikan tahun dalam bentuk string

    col1, col2, col3 = st.columns([1, 8, 1])

    with col1:
        if st.button("⬅", key="prev_dashboard"):
            st.session_state["slide"] = "pendapatan"
            st.rerun()

    with col2:
        st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pengaduan Pelanggan</h2>", unsafe_allow_html=True)

    with col3:
        if st.button("➡", key="next_dashboard"):
            st.session_state["slide"] = "pemutusan"
            st.rerun()

    # Pilih Rentang Tahun
    pilihan_waktu = st.sidebar.radio("**Pilih Rentang Waktu**", ["Semua Data", "Pilih Tahun"])

    # Dapatkan semua tahun yang ada di dataset
    tahun_options = sorted(fact_pengaduan["year"].unique())  # Tahun sebagai string
    tahun_min, tahun_max = tahun_options[0], tahun_options[-1]  # Default semua tahun

    fact_pengaduan_filtered = fact_pengaduan.copy()  # Untuk menyimpan data setelah filter

    if pilihan_waktu == "Pilih Tahun":
        tahun_min, tahun_max = st.sidebar.select_slider(
            "**Pilih Rentang Tahun**", 
            options=tahun_options,
            value=(tahun_options[0], tahun_options[-1])
        )
        fact_pengaduan_filtered = fact_pengaduan[
            (fact_pengaduan["year"] >= tahun_min) & (fact_pengaduan["year"] <= tahun_max)
        ]

    # **Hitung ulang total pengaduan setelah filter**
    total_pengaduan = len(fact_pengaduan_filtered)

    # Baris 1: Total Pengaduan (Card) di satu kolom
    col1, col2 = st.columns([1, 2])  # Kolom pertama lebih kecil untuk card, kolom kedua lebih besar untuk grafik

    with col1:
        # Card Total Pengaduan Keseluruhan (sudah dipengaruhi filter tahun)
        st.markdown(
            "<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pengaduan Keseluruhan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pengaduan:,}</p>", 
            unsafe_allow_html=True
        )

    with col2:
        # Grafik Total Pengaduan per Jenis (Bar Chart)
        pengaduan_jenis = fact_pengaduan_filtered.groupby("jenis_pengaduan").size().reset_index(name="total_pengaduan")
        fig = px.bar(
            pengaduan_jenis,
            x="jenis_pengaduan",
            y="total_pengaduan",
            text_auto=True,
            color_discrete_sequence=["dodgerblue"]
        )
        fig.update_layout(
            title=("Total Pengaduan per Jenis"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Jenis Pengaduan",
            yaxis_title="Jumlah Pengaduan",
            font=dict(color="#003366"),
            height=520,
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Baris 2: Total Pengaduan Berdasarkan Wilayah (Pie Chart) diletakkan di bawah card
    with col1:
        pengaduan_wilayah = fact_pengaduan_filtered.groupby("wilayah").size().reset_index(name="total_pengaduan")
        fig = px.pie(
            pengaduan_wilayah,
            names="wilayah",
            values="total_pengaduan",
            hole=0.3,
            color_discrete_sequence=px.colors.sequential.Blues
        )
        fig.update_traces(
            textinfo="label+percent",
            textfont_size=12
        )
        fig.update_layout(
            title=("Total Pengaduan Berdasarkan Wilayah"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)


    # Baris 3: Line Chart (Pengaduan per Bulan dan Tahun)
    col1, col2 = st.columns(2)

    # Mapping angka bulan ke nama bulan
    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
        7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
    }

    # Hitung total pengaduan per bulan dan tahun
    pengaduan_bulanan = fact_pengaduan_filtered.groupby(["year", "month"]).size().reset_index(name="total_pengaduan")

    # Ganti angka bulan dengan nama bulan
    pengaduan_bulanan["month"] = pengaduan_bulanan["month"].map(month_map)

    blue_shades = px.colors.sequential.Blues[2:]  # Skip warna putih dan biru pucat

    with col1:
        fig = px.line(
            pengaduan_bulanan,
            x="month",
            y="total_pengaduan",
            color="year",
            markers=True,
            text="total_pengaduan",
            color_discrete_sequence=blue_shades  # Gunakan daftar warna yang sudah diperbaiki
        )
        fig.update_layout(
            title="Total Pengaduan per Bulan",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Bulan",
            yaxis_title="Jumlah Pengaduan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        pengaduan_tahunan = fact_pengaduan_filtered.groupby("year").size().reset_index(name="total_pengaduan")
        fig = px.bar(
            pengaduan_tahunan,
            x="year",
            y="total_pengaduan",
            text="total_pengaduan",
            color_discrete_sequence=["dodgerblue"]
        )
        fig.update_layout(
            title=("Total Pengaduan per Tahun"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Jumlah Pengaduan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_pemutusan():
    dim_pelanggan, fact_transaksi, dim_goltarif, dim_waktu, _, _, dim_realisasi, fact_pemutusan,_ = load_data_cached()

    # Cek apakah data fact_pemutusan kosong
    if fact_pemutusan.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Merge awal (pastikan kolom waktu sudah ada)
    fact_pemutusan = fact_pemutusan.merge(dim_waktu, on="id_waktu", how="left")
    fact_pemutusan = fact_pemutusan.merge(dim_realisasi, on="id_realisasi", how="left")
    fact_pemutusan = fact_pemutusan.merge(dim_pelanggan, on="kodepelanggan", how="left")

    fact_pemutusan["year"] = fact_pemutusan["year"].astype(str)
    fact_pemutusan["month"] = pd.to_numeric(fact_pemutusan["month"])  # Pastikan bulan dalam bentuk angka

    # Pilih Rentang Tahun
    pilihan_waktu = st.sidebar.radio("**Pilih Rentang Waktu**", ["Semua Data", "Pilih Tahun"])

    # Dapatkan semua tahun yang ada di dataset
    tahun_options = sorted(fact_pemutusan["year"].unique())  # Tahun sebagai string
    tahun_min, tahun_max = tahun_options[0], tahun_options[-1]

    fact_pemutusan_filtered = fact_pemutusan.copy()

    if pilihan_waktu == "Pilih Tahun":
        tahun_min, tahun_max = st.sidebar.select_slider(
            "**Pilih Rentang Tahun**", 
            options=tahun_options,
            value=(tahun_options[0], tahun_options[-1])
        )
        fact_pemutusan_filtered = fact_pemutusan_filtered[
            (fact_pemutusan_filtered["year"] >= tahun_min) & (fact_pemutusan_filtered["year"] <= tahun_max)
        ]

    # Total Pemutusan Sambungan
    total_pemutusan = fact_pemutusan_filtered[["kodepelanggan", "id_waktu"]].shape[0]

    # Pemutusan per Bulan (Tanpa Merge Ulang)
    pemutusan_bulanan = (
        fact_pemutusan_filtered
        .groupby(["year", "month"])["kodepelanggan"]
        .nunique()
        .reset_index()
    )

    # Pemutusan berdasarkan Golongan Tarif
    pemutusan_goltarif = (
        fact_pemutusan_filtered
        .merge(fact_transaksi, on="kodepelanggan")
        .merge(dim_goltarif, on="kodegoltarif")
        .groupby("kodegoltarif")["kodepelanggan"]
        .nunique()
        .reset_index()
    )

    # Pemutusan berdasarkan Wilayah
    pemutusan_wilayah = (
        fact_pemutusan_filtered
        .groupby("wilayah")["kodepelanggan"]
        .nunique()
        .reset_index()
    )

    # Realisasi Pemutusan per Bulan
    realisasi_bulanan = (
        fact_pemutusan_filtered
        .groupby("jenis_realisasi")["id_pemutusan"]
        .nunique()
        .reset_index()
    )

    # Layout Dashboard
    col1, col2, col3 = st.columns([1, 8, 1])

    with col1:
        if st.button("⬅", key="prev_dashboard"):
            st.session_state["slide"] = "pengaduan"
            st.rerun()

    with col2:
        st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pemutusan Sambungan</h2>", unsafe_allow_html=True)

    with col3:
        if st.button("➡", key="next_dashboard"):
            st.session_state["slide"] = "sbbaru"
            st.rerun()
        
    # Layout utama
    col1, col2 = st.columns([3, 7])  

    with col1:
        # Card Total Pemutusan Sambungan
        st.markdown(
            "<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pemutusan Sambungan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pemutusan:,}</p>", 
            unsafe_allow_html=True
        )

        all_jenis_realisasi = dim_realisasi["jenis_realisasi"].unique()
        realisasi_bulanan = (
            pd.DataFrame({"jenis_realisasi": all_jenis_realisasi})
            .merge(realisasi_bulanan, on="jenis_realisasi", how="left")
            .fillna(0)
        )
        fig = px.bar(
            realisasi_bulanan,
            x="jenis_realisasi",
            y="id_pemutusan",
            text="id_pemutusan",
            color_discrete_sequence=["dodgerblue"]
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            title=("Realisasi Pemutusan per Bulan"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Bulan",
            yaxis_title="Jumlah Realisasi Pemutusan",
            font=dict(color="#003366")
        )
        st.plotly_chart(fig, use_container_width=True)

        

    with col2:
        # Grafik Jumlah Pemutusan per Bulan
        fig = go.Figure()
        years = sorted(pemutusan_bulanan["year"].unique())  # Urutkan tahun
        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun", 
            7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
        }
        pemutusan_bulanan["month"] = pemutusan_bulanan["month"].map(month_map)
        blue_shades = px.colors.sequential.Blues[2:]

        for i, year in enumerate(years):
            df_year = pemutusan_bulanan[pemutusan_bulanan["year"] == year]
            fig.add_trace(go.Scatter(
                x=df_year["month"], 
                y=df_year["kodepelanggan"], 
                mode="lines+markers+text",
                text=df_year["kodepelanggan"].apply(lambda x: f"{x:,.0f}"),
                textposition="top center",
                name=str(year),
                line=dict(color=blue_shades[i % len(blue_shades)])
            ))

        fig.update_layout(
            title="Jumlah Pemutusan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Jumlah Pemutusan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(categoryorder="array", categoryarray=list(month_map.values())),
            height=520
        )
        st.plotly_chart(fig, use_container_width=True)


    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            pemutusan_wilayah,
            names="wilayah",
            values="kodepelanggan",
            hole=0.3,
            color_discrete_sequence=px.colors.sequential.Blues
        )
        fig.update_traces(
            textinfo="label+percent",
            textfont_size=12
        )
        fig.update_layout(
            title=("Pemutusan berdasarkan Wilayah"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#003366")
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Grafik Pemutusan berdasarkan Golongan Tarif dalam bentuk horizontal
        fig = px.bar(
            pemutusan_goltarif,
            y="kodegoltarif",  # Kategori di sumbu Y agar grafik horizontal
            x="kodepelanggan",  # Nilai pemutusan di sumbu X
            text="kodepelanggan",
            color_discrete_sequence=["dodgerblue"],
            orientation="h"  # Ubah orientasi grafik menjadi horizontal
        )

        # Menyesuaikan teks agar berada di luar batang grafik
        fig.update_traces(textposition="outside")

        # Mengatur tampilan layout agar lebih mirip dengan gambar
        fig.update_layout(
            title="Pemutusan berdasarkan Golongan Tarif",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Jumlah Pemutusan",
            yaxis_title="Golongan Tarif",
            font=dict(color="#003366"),
            xaxis=dict(showgrid=False),  # Hilangkan grid untuk tampilan lebih bersih
            yaxis=dict(categoryorder="total ascending")  # Urutkan dari terbesar ke terkecil
        )

        # Tampilkan grafik di Streamlit
        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_sbbaru():
    _, _, _, dim_waktu, _, _, dim_realisasi, _, fact_sbbaru = load_data_cached()

    # Cek apakah data fact_sbbaru kosong
    if fact_sbbaru.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Merge data
    fact_sbbaru = fact_sbbaru.merge(dim_waktu, on="id_waktu", how="left")
    fact_sbbaru = fact_sbbaru.merge(dim_realisasi, on="id_realisasi", how="left")
    
    fact_sbbaru["year"] = fact_sbbaru["year"].astype(str)
    fact_sbbaru["month"] = pd.to_numeric(fact_sbbaru["month"])
    
    # Pilih Rentang Tahun
    pilihan_waktu = st.sidebar.radio("**Pilih Rentang Waktu**", ["Semua Data", "Pilih Tahun"])
    tahun_options = sorted(fact_sbbaru["year"].unique())
    
    fact_sbbaru_filtered = fact_sbbaru.copy()
    
    if pilihan_waktu == "Pilih Tahun":
        tahun_min, tahun_max = st.sidebar.select_slider(
            "**Pilih Rentang Tahun**", 
            options=tahun_options,
            value=(tahun_options[0], tahun_options[-1])
        )
        fact_sbbaru_filtered = fact_sbbaru_filtered[
            (fact_sbbaru_filtered["year"] >= tahun_min) & (fact_sbbaru_filtered["year"] <= tahun_max)
        ]
    
    # Data Agregasi
    total_pendaftaran = fact_sbbaru_filtered.shape[0]
    pendaftaran_bulanan = fact_sbbaru_filtered.groupby(["year", "month"]).agg({"kodecpelanggan": "nunique"}).reset_index()
    pendaftaran_wilayah = fact_sbbaru_filtered.groupby("wilayah").agg({"kodecpelanggan": "nunique"}).reset_index()
    biaya_bulanan = fact_sbbaru_filtered.groupby(["year", "month"]).agg({"jumlah": "sum"}).reset_index()
    pendaftaran_realisasi = fact_sbbaru_filtered.groupby("jenis_realisasi").size().reset_index(name="total_pendaftaran")
    
    # Layout Dashboard
    col1, col2, col3 = st.columns([1, 8, 1])
    
    with col1:
        if st.button("⬅", key="prev_dashboard"):
            st.session_state["slide"] = "pemutusan"
            st.rerun()

    with col2:
        st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pendaftaran Sambungan Baru</h2>", unsafe_allow_html=True)
    
    with col3:
        if st.button("➡", key="next_dashboard"):
            st.session_state["slide"] = "pelanggan"
            st.rerun()
    
    col1, col2 = st.columns([3, 7])
    
    with col1:
        st.markdown("<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pendaftaran Sambungan Baru</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pendaftaran:,}</p>", unsafe_allow_html=True)
        
        fig = px.pie(
            pendaftaran_wilayah,
            names="wilayah",
            values="kodecpelanggan",
            hole=0.3,
            color_discrete_sequence=px.colors.sequential.Blues
        )
        fig.update_layout(title="Pendaftaran Berdasarkan Wilayah")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun", 7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"}
        pendaftaran_bulanan["month"] = pendaftaran_bulanan["month"].map(month_map)
        biaya_bulanan["month"] = biaya_bulanan["month"].map(month_map)
        
        blue_shades = px.colors.sequential.Blues[2:]

        fig = px.line(
            pendaftaran_bulanan,
            x="month",
            y="kodecpelanggan",
            color="year",
            markers=True,
            text="kodecpelanggan",
            color_discrete_sequence=blue_shades
        )
        fig.update_layout(title="Jumlah Pendaftaran per Bulan", xaxis_title="Bulan", yaxis_title="Jumlah Pendaftaran")
        st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(
            biaya_bulanan,
            x="month",
            y="jumlah",
            color="year",
            markers=True,
            text="jumlah",
            color_discrete_sequence=blue_shades
        )
        fig.update_layout(title="Total Biaya Pemasangan per Bulan", xaxis_title="Bulan", yaxis_title="Total Biaya Pemasangan")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            x=pendaftaran_realisasi["jenis_realisasi"],
            y=pendaftaran_realisasi["total_pendaftaran"],
            text_auto=True,
            color_discrete_sequence=["#0073E6"]
        )
        fig.update_layout(title="Jumlah Pendaftaran Berdasarkan Tindakan Realisasi", xaxis_title="Jenis Realisasi", yaxis_title="Total Pendaftaran")
        st.plotly_chart(fig, use_container_width=True)

# Load dan resize gambar
logo_path = "dashboard/logo_perumda.png"

if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)
else:
    st.sidebar.error("Gambar tidak ditemukan! Periksa kembali path-nya.")

st.sidebar.markdown("---")  # Garis pemisah untuk memperjelas menu

st.sidebar.title("Menu")

if "page" not in st.session_state:
    st.session_state["page"] = "Dashboard"
    st.session_state["slide"] = "pelanggan"

col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button("Dashboard", key="dashboard_button"):
        st.session_state["page"] = "Dashboard"
        st.rerun() # Refresh halaman setelah mengganti tab

with col2:
    if st.button("Proses Data", key="proses_data_button"):
        st.session_state["page"] = "Proses Data"
        st.rerun()  # Refresh halaman setelah mengganti tab

if st.session_state["page"] == "Dashboard":
    if st.session_state["slide"] == "pelanggan":
        show_dashboard_pelanggan()
    elif st.session_state["slide"] == "pemakaian_air":
        show_dashboard_pemakaian_air()
    elif st.session_state["slide"] == "pendapatan":
        show_dashboard_pendapatan()
    elif st.session_state["slide"] == "pengaduan":
        show_dashboard_pengaduan()
    elif st.session_state["slide"] == "pemutusan":
        show_dashboard_pemutusan()
    elif st.session_state["slide"] == "sbbaru":
        show_dashboard_sbbaru()
elif st.session_state["page"] == "Proses Data":
    proses.show_proses_etl()  # Memanggil UI dari proses.py