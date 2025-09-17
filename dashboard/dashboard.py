import sys
import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go 
import proses 
from tabel import *
from forecast import *

# path ke folder utama 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

Image.MAX_IMAGE_PIXELS = None

@st.cache_resource
def get_db_connection():
    """Mengembalikan koneksi database yang hanya dibuat sekali."""
    DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
    return create_engine(DATABASE_URL)
engine = get_db_connection()

# Fungsi untuk mengambil data 
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
    dim_pelanggan, _, _, dim_waktu, _, _, dim_realisasi, _, fact_sbbaru = load_data_cached()

    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pelanggan</h2>", unsafe_allow_html=True)

    if dim_pelanggan.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Merge fact_sbbaru dengan dim_waktu dan dim_realisasi
    fact_sbbaru = fact_sbbaru.merge(dim_waktu, on="id_waktu", how="left")
    fact_sbbaru = fact_sbbaru.merge(dim_realisasi, on="id_realisasi", how="left")

    # Sidebar untuk memilih rentang tahun
    st.sidebar.markdown("**Pilih Rentang Tahun**")
    tahun_awal, tahun_akhir = st.sidebar.select_slider(
        "Rentang Tahun", 
        options=sorted(fact_sbbaru["year"].unique()), 
        value=(fact_sbbaru["year"].min(), fact_sbbaru["year"].max())
    )

    # Dictionary mapping untuk Status Pelanggan
    status_mapping = {
        "A": "Aktif",
        "N": "Nonaktif",
        "P": "Proses"
    }

    # Dictionary mapping untuk Wilayah
    wilayah_mapping = {
        "UT": "Utara",
        "SN": "Selatan",
        "PS": "Pusat"
    }

    # Sidebar untuk memilih Status Pelanggan
    st.sidebar.markdown("**Pilih Status Pelanggan**")
    status_options = dim_pelanggan["status"].unique()

    col_status1, col_status2 = st.sidebar.columns(2)  
    status_selected = {}

    for i, status in enumerate(status_options):
        label = status_mapping.get(status, status)  # Gunakan label dari mapping
        with [col_status1, col_status2][i % 2]:  # Bagi ke dalam 2 kolom
            status_selected[status] = st.checkbox(label, value=True)

    # Simpan status yang dipilih
    pilihan_status = [s for s, v in status_selected.items() if v]

    # Sidebar untuk memilih Wilayah
    st.sidebar.markdown("**Pilih Wilayah**")
    wilayah_options = dim_pelanggan["wilayah"].unique()

    col_wilayah1, col_wilayah2 = st.sidebar.columns(2)  
    wilayah_selected = {}

    for i, wilayah in enumerate(wilayah_options):
        label = wilayah_mapping.get(wilayah, wilayah)  # Gunakan label dari mapping
        with [col_wilayah1, col_wilayah2][i % 2]:  # Bagi ke dalam 2 kolom
            wilayah_selected[wilayah] = st.checkbox(label, value=True)

    # Simpan wilayah yang dipilih
    pilihan_wilayah = [w for w, v in wilayah_selected.items() if v]

    # Cek jika tidak ada wilayah atau status yang dipilih
    if not pilihan_wilayah or not pilihan_status:
        if not pilihan_wilayah and not pilihan_status:
            st.warning("Silakan pilih **status pelanggan** dan **wilayah** terlebih dahulu di sidebar.")
        elif not pilihan_wilayah:
            st.warning("Silakan pilih **wilayah** terlebih dahulu di sidebar.")
        elif not pilihan_status:
            st.warning("Silakan pilih **status pelanggan** terlebih dahulu di sidebar.")
        
        return  

    # Filter data hanya untuk pelanggan yang bertambah (jenis_realisasi == "Y")
    pelanggan_bertambah = fact_sbbaru[fact_sbbaru["jenis_realisasi"] == "Y"]

    # Urutkan tahun yang dipilih
    tahun_terpilih = sorted(pelanggan_bertambah["year"].unique())
    tahun_terakhir = tahun_terpilih[-1]  

    # Filter data berdasarkan status, wilayah, dan rentang tahun yang dipilih
    dim_pelanggan = dim_pelanggan[
        dim_pelanggan['status'].isin(pilihan_status) & 
        dim_pelanggan['wilayah'].isin(pilihan_wilayah)
    ]

    fact_sbbaru = fact_sbbaru[
        (fact_sbbaru["year"] >= tahun_awal) & 
        (fact_sbbaru["year"] <= tahun_akhir)
    ]

    # Total jumlah pelanggan setelah filter
    total_pelanggan = dim_pelanggan['kodepelanggan'].nunique()

    # Jumlah wilayah setelah filter
    wilayah_manajemen = dim_pelanggan['wilayah'].nunique()

    # Jumlah pelanggan berdasarkan status setelah filter
    status_labels = {"A": "Aktif", "N": "Nonaktif", "P": "Proses"}
    status_counts = dim_pelanggan['status'].value_counts()
    status_counts.index = [status_labels.get(s, s) for s in status_counts.index]

    # Jumlah pelanggan berdasarkan wilayah setelah filter
    wilayah_labels = {"UT": "Utara", "PS": "Pusat", "SN": "Selatan"}
    pelanggan_per_wilayah = dim_pelanggan['wilayah'].value_counts()
    pelanggan_per_wilayah.index = [wilayah_labels.get(w, w) for w in pelanggan_per_wilayah.index]

    # Kolom untuk total pelanggan dan jumlah wilayah
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pelanggan</h5>", 
            unsafe_allow_html=True
        )
        total_pelanggan = dim_pelanggan['kodepelanggan'].nunique()
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pelanggan:,}</p>", 
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Jumlah Wilayah</h5>", 
            unsafe_allow_html=True
        )
        wilayah_manajemen = dim_pelanggan['wilayah'].nunique()
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{wilayah_manajemen}</p>", 
            unsafe_allow_html=True
        )

    col1, col2, col3 = st.columns([2,3,5])

    with col1:
        status_counts = dim_pelanggan['status'].value_counts()
        status_labels = {"A": "Aktif", "N": "Nonaktif", "P": "Proses"}
        status_counts.index = [status_labels.get(s, s) for s in status_counts.index]

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
            legend=dict(yanchor="top", y=1, xanchor="right", x=1)
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        pelanggan_per_wilayah = dim_pelanggan['wilayah'].value_counts()
        wilayah_labels = {"UT": "Utara", "PS": "Pusat", "SN": "Selatan"}
        pelanggan_per_wilayah.index = [wilayah_labels.get(w, w) for w in pelanggan_per_wilayah.index]

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
            xaxis=dict(tickangle=0)
        )

        st.plotly_chart(fig, use_container_width=True)

    # Pilihan palet warna yang kontras
    color_palette = [c for c in px.colors.qualitative.Dark24]

    with col3:
        fig = go.Figure()

        # Filter data pelanggan bertambah berdasarkan rentang tahun & wilayah
        pelanggan_bertambah = fact_sbbaru[
            (fact_sbbaru["jenis_realisasi"] == "Y") & 
            (fact_sbbaru["year"] >= tahun_awal) & (fact_sbbaru["year"] <= tahun_akhir)
        ]

        # Filter berdasarkan wilayah jika ada pilihan wilayah
        if pilihan_wilayah:
            pelanggan_bertambah = pelanggan_bertambah[pelanggan_bertambah["wilayah"].isin(pilihan_wilayah)]

        # Pastikan data tahun tersedia setelah filter
        tahun_terpilih = sorted(pelanggan_bertambah["year"].unique())
        tahun_terakhir = tahun_terpilih[-1] if tahun_terpilih else None  

        # Mapping angka bulan ke nama bulan
        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
            7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
        }
        pelanggan_bertambah["month"] = pelanggan_bertambah["month"].map(month_map)

        # Pastikan semua bulan tersedia dalam urutan yang benar
        all_months = list(month_map.values())

        # Jika tidak ada data setelah filter
        if pelanggan_bertambah.empty:
            st.warning("Tidak ada data pelanggan bertambah untuk rentang tahun dan wilayah yang dipilih.")
        else:
            # Looping setiap tahun
            for i, year in enumerate(tahun_terpilih):
                df_year = pelanggan_bertambah[pelanggan_bertambah["year"] == year]

                # Jika tidak ada wilayah yang dipilih, jumlahkan semua wilayah
                wilayah_list = pilihan_wilayah if pilihan_wilayah else ["Semua Wilayah"]
                
                for j, wilayah in enumerate(wilayah_list):
                    # Filter data berdasarkan wilayah
                    if wilayah == "Semua Wilayah":
                        df_wilayah = df_year.groupby("month")["kodecpelanggan"].count()
                    else:
                        df_wilayah = df_year[df_year["wilayah"] == wilayah].groupby("month")["kodecpelanggan"].count()

                    # Pastikan semua bulan tersedia
                    df_wilayah = df_wilayah.reindex(all_months, fill_value=0).reset_index()

                    # Tambahkan data ke grafik
                    fig.add_trace(go.Scatter(
                        x=df_wilayah["month"],
                        y=df_wilayah["kodecpelanggan"],
                        mode="lines+markers+text",
                        text=df_wilayah["kodecpelanggan"].apply(lambda x: "{:,}".format(int(x))),
                        textposition="top center",
                        name=f"{year} - {wilayah}",
                        legendgroup=str(year),
                        line=dict(
                            color=color_palette[j % len(color_palette)],
                            width=2
                        ),
                        hoverinfo="x+y+name",
                        visible=True if year == tahun_terakhir else "legendonly"
                    ))

            # Layout Grafik
            fig.update_layout(
                title="Jumlah Pelanggan Baru per Bulan",
                xaxis_title="Bulan",
                yaxis_title="Jumlah Pelanggan Bertambah",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(
                    tickmode="array",
                    tickvals=all_months,
                    ticktext=all_months
                ),
                legend_title="Tahun - Wilayah",
                font=dict(color="#003366"),
                height=450
            )

            # Menampilkan Grafik
            st.plotly_chart(fig, use_container_width=True)

def show_dashboard_pemakaian_air():
    # Load data
    dim_pelanggan, fact_transaksi, dim_goltarif, dim_waktu, _, _, _, _, _ = load_data_cached()

    # Cek apakah data fact_transaksi kosong
    if fact_transaksi.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return

    # Gabungkan dengan dimensi golongan tarif dan waktu
    df = fact_transaksi.merge(dim_goltarif, on="kodegoltarif", how="left")
    df = df.merge(dim_waktu, on="id_waktu", how="left")

    # Rentang Tahun dari sidebar
    tahun_awal, tahun_akhir = st.sidebar.select_slider(
        "Rentang Tahun", 
        options=sorted(df["year"].unique()), 
        value=(min(df["year"]), max(df["year"]))
    )
    
    df_filtered = df[(df["year"] >= tahun_awal) & (df["year"] <= tahun_akhir)]

    # Hitung metrik utama
    total_pemakaian = df_filtered["pemakaian"].sum()
    jumlah_pelanggan = df_filtered["kodepelanggan"].nunique()  
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

    # Judul Dashboard
    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pemakaian Air</h2>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])

    with col1:
        # Menampilkan Total Pemakaian Air
        st.markdown(f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pemakaian Air</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pemakaian:,.0f} m³</p>", unsafe_allow_html=True)

    with col2:
        # Menampilkan Rata-rata Pemakaian Air
        st.markdown(f"<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Rata-rata Pemakaian Air</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{rata_rata_pemakaian:.2f} m³</p>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([4, 4, 4])

    with col1:
        fig = go.Figure()

        # Bar chart: Total pemakaian air
        fig.add_trace(go.Bar(
            x=pemakaian_per_tahun["year"], 
            y=pemakaian_per_tahun["pemakaian"], 
            text=pemakaian_per_tahun["pemakaian"],
            texttemplate='%{text:,.0f}', 
            textposition='outside',
            name="Total (m³)",
            marker_color=px.colors.sequential.Blues_r[1]
        ))

        # Line chart: Rata-rata pemakaian
        fig.add_trace(go.Scatter(
            x=rata_rata_per_tahun["year"], 
            y=rata_rata_per_tahun["rata_rata_pemakaian"], 
            text=rata_rata_per_tahun["rata_rata_pemakaian"],
            texttemplate='%{text:.2f}', 
            textposition="top center",
            mode='lines+markers+text',
            name="Rata-rata (m³)",
            marker=dict(color="#FF5733", size=8),
            line=dict(color="#FF5733", width=2),
            textfont=dict(color="white")
        ))

        # Layout konfigurasi
        fig.update_layout(
            title=dict(
                text="Total dan Rata-rata Pemakaian Air<br>per Tahun",
                x=0.0,  
                xanchor="left",  
                y=0.90,
                font=dict(size=16)
            ),
            xaxis_title="Tahun",
            yaxis_title="Pemakaian (m³)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(tickmode="array", tickvals=pemakaian_per_tahun["year"]),
            yaxis=dict(range=[0, max(pemakaian_per_tahun["pemakaian"].max(), rata_rata_per_tahun["rata_rata_pemakaian"].max()) * 1.1]),
            legend=dict(
                x=1.0,  
                xanchor="right",
                y=1.15,
                orientation="v",
                font=dict(size=10),
                bgcolor="rgba(0,0,0,0)" 
            )
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = go.Figure()

        df_wilayah = fact_transaksi.merge(dim_pelanggan, on="kodepelanggan", how="left")
        df_filtered = df_wilayah.merge(dim_waktu, on="id_waktu", how="left")

        # Filter berdasarkan tahun yang dipilih
        df_filtered = df_filtered[(df_filtered["year"] >= tahun_awal) & (df_filtered["year"] <= tahun_akhir)]

        # Warna yang kontras, tanpa merah
        color_palette = [c for c in px.colors.qualitative.Dark24 if c.lower() != "#e31a1c"]

        tahun_terpilih = sorted(df_filtered["year"].unique())
        tahun_terakhir = tahun_terpilih[-1]
        wilayah_list = df_filtered["wilayah"].unique()

        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
            7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
        }

        for i, year in enumerate(tahun_terpilih):
            df_year = df_filtered[df_filtered["year"] == year].groupby(["month", "wilayah"])["pemakaian"].sum().reset_index()

            for j, wilayah in enumerate(wilayah_list):
                df_wilayah = df_year[df_year["wilayah"] == wilayah]
                if df_wilayah.empty:
                    continue

                df_wilayah = df_wilayah.sort_values("month")

                # Hitung persentase perubahan
                df_wilayah["change"] = df_wilayah["pemakaian"].pct_change() * 100
                df_wilayah["change"] = df_wilayah["change"].fillna(0)

                # Menambahkan label tahun hanya di titik terakhir
                text_labels = [f"{year}" if k == len(df_wilayah) - 1 else "" for k in range(len(df_wilayah))]

                fig.add_trace(go.Scatter(
                    x=df_wilayah["month"].apply(lambda x: month_map.get(x, x)),
                    y=df_wilayah["pemakaian"],
                    mode="lines+markers+text",
                    name=f"{year} - {wilayah}",
                    legendgroup=str(year),
                    line=dict(color=color_palette[j % len(color_palette)], width=2),
                    hovertemplate=" (%{x},"
                                " %{y:.2f} m³)<br>"
                                "change: %{customdata:.1f}%",
                    customdata=df_wilayah["change"],
                    visible=True if year == tahun_terakhir else "legendonly",
                    text=text_labels,
                    textposition="top right"
                ))

        fig.update_layout(
            title=dict(
                text="Total Pemakaian Air (m³) per Bulan<br> Berdasarkan Wilayah",
                x=0.0,
                xanchor="left",
                y=0.90,
                font=dict(size=16)
            ),
            xaxis_title="Bulan",
            yaxis_title="Pemakaian Air (m³)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                tickmode="array",
                tickvals=list(month_map.values()),
                ticktext=list(month_map.values())
            ),
            legend_title="Tahun - Wilayah",
            font=dict(color="#003366")
        )

        st.plotly_chart(fig, use_container_width=True)


    with col3:
        df_wilayah = fact_transaksi.merge(dim_pelanggan, on="kodepelanggan", how="left")
        df_filtered = df_wilayah.merge(dim_waktu, on="id_waktu", how="left")

        # Filter berdasarkan tahun yang dipilih
        df_filtered = df_filtered[(df_filtered["year"] >= tahun_awal) & (df_filtered["year"] <= tahun_akhir)]


        # Pastikan ada kolom 'year'
        if "year" not in df_filtered.columns:
            df_filtered["year"] = pd.to_datetime(df_filtered["tanggal"]).dt.year

        # Grouping berdasarkan golongan tarif & wilayah
        pemakaian_per_goltarif_wilayah = df_filtered.groupby(["year", "kodegoltarif", "wilayah"])["pemakaian"].sum().reset_index()

        # Warna biru dengan lompatan warna
        color_palette = px.colors.sequential.Blues_r[::2]

        fig = go.Figure()

        # Loop setiap tahun
        for i, year in enumerate(sorted(df_filtered["year"].unique())):
            df_year = pemakaian_per_goltarif_wilayah[pemakaian_per_goltarif_wilayah["year"] == year]

            # Loop setiap wilayah
            for j, wilayah in enumerate(df_year["wilayah"].unique()):
                df_wilayah = df_year[df_year["wilayah"] == wilayah]

                fig.add_trace(go.Bar(
                    x=df_wilayah["pemakaian"], 
                    y=df_wilayah["kodegoltarif"],  
                    name=f"{wilayah} - {year}",
                    legendgroup=str(year),
                    marker_color=color_palette[j % len(color_palette)],
                    hoverinfo="x+y+name",
                    orientation="h",
                    text=df_wilayah["pemakaian"].map('{:,.0f}'.format),
                    textposition="outside",
                    visible=True if year == sorted(df_filtered["year"].unique())[-1] else "legendonly"
                ))

        fig.update_layout(
            title=dict(
                text="Pemakaian Air<br>per Golongan Tarif dan Wilayah",
                x=0.0,  
                xanchor="left",  
                y=0.90,
                font=dict(size=16)
            ),
            xaxis_title="Total Pemakaian (m³)",
            yaxis_title="Kode Golongan Tarif",
            barmode="stack",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(showgrid=False, categoryorder="total ascending"),
            font=dict(color="#003366"),
            legend_title="Wilayah dan Tahun"
        )

        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_pendapatan():
    dim_pelanggan, fact_transaksi, _, dim_waktu, _, _, _, _, _ = load_data_cached()
    
    # Judul Dashboard
    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pendapatan</h2>", unsafe_allow_html=True)

    # Cek apakah ada data yang bisa ditampilkan
    if fact_transaksi.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return
    
    # Gabungkan data transaksi dengan waktu dan pelanggan
    fact_transaksi = fact_transaksi.merge(dim_waktu, on="id_waktu", how="left")
    fact_transaksi = fact_transaksi.merge(dim_pelanggan, on="kodepelanggan", how="left")
    
    # Filter berdasarkan rentang tahun
    tahun_min, tahun_max = st.sidebar.select_slider(
        "Pilih Rentang Tahun",
        options=sorted(fact_transaksi["year"].unique()),
        value=(fact_transaksi["year"].min(), fact_transaksi["year"].max())
    )

    fact_transaksi = fact_transaksi[
        (fact_transaksi["year"] >= tahun_min) & (fact_transaksi["year"] <= tahun_max)
    ]
    
    # Filter berdasarkan wilayah
    pilihan_wilayah = st.sidebar.radio("**Pilih Wilayah**", ["Semua Wilayah", "Pilih Wilayah"])

    if pilihan_wilayah == "Pilih Wilayah":
        wilayah_terpilih = []
        
        # Checkbox untuk memilih wilayah
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.checkbox("Pusat", value=True):
                wilayah_terpilih.append("PS")
        with col2:
            if st.checkbox("Selatan", value=True):
                wilayah_terpilih.append("SN")
        with col1:
            if st.checkbox("Utara", value=True):
                wilayah_terpilih.append("UT")
        
        if wilayah_terpilih:
            fact_transaksi = fact_transaksi[fact_transaksi["wilayah"].isin(wilayah_terpilih)]
    
    # Cek jika tidak ada wilayah yang dipilih (kecuali pilihan "Semua Wilayah" dianggap valid)
    if pilihan_wilayah == "Pilih Wilayah" and not wilayah_terpilih:
        st.warning("Silakan pilih **wilayah** terlebih dahulu di sidebar.")
        return


    # Hitung total pendapatan
    total_tagihan = fact_transaksi['tagihan'].sum()
    total_denda = fact_transaksi['denda'].sum()
    total_pendapatan = total_tagihan + total_denda

    # Agregasi data berdasarkan kategori tertentu
    group_by_columns_tahun = ["year"] if pilihan_wilayah == "Semua Wilayah" else ["year", "wilayah"]
    group_by_columns_bulan = ["year", "month"] if pilihan_wilayah == "Semua Wilayah" else ["year", "month", "wilayah"]
    group_by_columns_goltarif = ["kodegoltarif"] if pilihan_wilayah == "Semua Wilayah" else ["kodegoltarif", "wilayah"]

    # Agregasi data berdasarkan filter
    tagihan_per_tahun = fact_transaksi.groupby(group_by_columns_tahun)["tagihan"].sum().reset_index()
    denda_per_tahun = fact_transaksi.groupby(group_by_columns_tahun)["denda"].sum().reset_index()
    pendapatan_per_tahun = fact_transaksi.groupby(group_by_columns_tahun)["jumlahbayar"].sum().reset_index()

    tagihan_per_bulan = fact_transaksi.groupby(group_by_columns_bulan)["tagihan"].sum().reset_index()
    denda_per_bulan = fact_transaksi.groupby(group_by_columns_bulan)["denda"].sum().reset_index()
    pendapatan_per_bulan = fact_transaksi.groupby(group_by_columns_bulan)["jumlahbayar"].sum().reset_index()

    denda_per_goltarif = fact_transaksi.groupby(group_by_columns_goltarif)["denda"].sum().reset_index()
    pendapatan_per_goltarif = fact_transaksi.groupby(group_by_columns_goltarif)["jumlahbayar"].sum().reset_index()
    
    col1, col2, col3 = st.columns([1, 8, 1])
    
    # Warna untuk visualisasi
    warna_blues = px.colors.sequential.Blues_r  
    warna_pendapatan_global = warna_blues[0]  
    warna_tagihan_global = warna_blues[2]  
    warna_denda_global = warna_blues[-4]  
    warna_teal = px.colors.sequential.Teal 

    # Mapping warna untuk tiap wilayah
    warna_wilayah = {
        "PS": {"tagihan": warna_blues[1], "denda": warna_teal[4]},
        "SN": {"tagihan": warna_blues[2], "denda": warna_teal[3]},
        "UT": {"tagihan": warna_blues[3], "denda": warna_teal[2]}
    }

    # Buat layout dengan 3 kolom
    col1, col2, col3 = st.columns([4, 4, 7])

    with col3:
        fig = go.Figure()

        if pilihan_wilayah == "Semua Wilayah":
            # Hitung total tagihan + denda per tahun
            total_per_tahun = tagihan_per_tahun["tagihan"] + denda_per_tahun["denda"]

            # Hitung persentase
            tagihan_per_tahun["persen"] = tagihan_per_tahun["tagihan"] / total_per_tahun * 100
            denda_per_tahun["persen"] = denda_per_tahun["denda"] / total_per_tahun * 100

            # Tagihan
            fig.add_trace(go.Bar(
                x=tagihan_per_tahun["year"], 
                y=tagihan_per_tahun["persen"], 
                name="Tagihan",
                marker_color=warna_tagihan_global,
                text=tagihan_per_tahun["persen"].apply(lambda x: f"{x:.1f}%"),
                textposition="inside",
                hovertemplate="Tagihan: Rp %{customdata:,.0f} <br>Persentase: %{y:.1f}%",
                customdata=tagihan_per_tahun["tagihan"]
            ))

            # Denda → pakai posisi teks di luar
            fig.add_trace(go.Bar(
                x=denda_per_tahun["year"], 
                y=denda_per_tahun["persen"], 
                name="Denda",
                marker_color=warna_denda_global,
                text=denda_per_tahun["persen"].apply(lambda x: f"{x:.1f}%"),
                textposition="outside",  
                hovertemplate="Denda: Rp %{customdata:,.0f} <br>Persentase: %{y:.1f}%",
                customdata=denda_per_tahun["denda"]
            ))

            fig.update_layout(barmode="stack")

        else:
            tahun_list = sorted(tagihan_per_tahun["year"].unique())
            wilayah_list = tagihan_per_tahun["wilayah"].unique()

            for wilayah in wilayah_list:
                for idx, tahun in enumerate(tahun_list):
                    t_row = tagihan_per_tahun[(tagihan_per_tahun["year"] == tahun) & (tagihan_per_tahun["wilayah"] == wilayah)]
                    d_row = denda_per_tahun[(denda_per_tahun["year"] == tahun) & (denda_per_tahun["wilayah"] == wilayah)]

                    if not t_row.empty and not d_row.empty:
                        tagihan_val = t_row["tagihan"].values[0]
                        denda_val = d_row["denda"].values[0]

                        total_nasional = (
                            tagihan_per_tahun[tagihan_per_tahun["year"] == tahun]["tagihan"].sum()
                            + denda_per_tahun[denda_per_tahun["year"] == tahun]["denda"].sum()
                        )

                        tagihan_persen = tagihan_val / total_nasional * 100
                        denda_persen = denda_val / total_nasional * 100

                        show_legend = (idx == 0)  

                        fig.add_trace(go.Bar(
                            x=[tahun],
                            y=[tagihan_persen],
                            name=f"Tagihan - {wilayah}",
                            marker_color=warna_wilayah[wilayah]["tagihan"],
                            offsetgroup=wilayah,
                            legendgroup=wilayah,
                            showlegend=show_legend,
                            text=[f"{tagihan_persen:.1f}%"],
                            textposition="inside",
                            customdata=[tagihan_val],
                            hovertemplate=f"{wilayah} - Tagihan: Rp %{{customdata:,.0f}} <br>Persentase: %{{y:.1f}}%"
                        ))

                        fig.add_trace(go.Bar(
                            x=[tahun],
                            y=[denda_persen],
                            name=f"Denda - {wilayah}",
                            marker_color=warna_wilayah[wilayah]["denda"],
                            offsetgroup=wilayah,
                            base=[tagihan_persen],
                            legendgroup=wilayah,
                            showlegend=show_legend,
                            text=[f"{denda_persen:.1f}%"],
                            textposition="outside",
                            customdata=[denda_val],
                            hovertemplate=f"{wilayah} - Denda: Rp %{{customdata:,.0f}} <br>Persentase: %{{y:.1f}}%"
                        ))

        fig.update_layout(
            title="Tagihan dan Denda per Tahun (Persentase)",
            xaxis_title="Tahun",
            yaxis_title="Persentase (%)",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            yaxis=dict(range=[0, 100]),
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)


    with col2:
        # Grafik Pendapatan per Tahun
        if pilihan_wilayah == "Semua Wilayah":
            fig = px.bar(
                pendapatan_per_tahun, 
                x="year", 
                y="jumlahbayar", 
                text_auto=".2s",
                height = 380,
                color_discrete_sequence=[warna_pendapatan_global]
            )
        else:
            fig = px.bar(
                pendapatan_per_tahun, 
                x="year", 
                y="jumlahbayar", 
                color="wilayah",
                text_auto=".2s",
                height = 380,
                color_discrete_sequence=[warna_pendapatan_global, warna_tagihan_global, warna_denda_global]
            )

        # Konfigurasi layout grafik
        fig.update_layout(
            title="Pendapatan per Tahun",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Pendapatan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )

        # Tampilkan grafik
        st.plotly_chart(fig, use_container_width=True)

    with col1:
        # Menampilkan Total Pendapatan
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("<h5 style='font-size:14px;font-weight:bold;'>Total Pendapatan dari Tagihan</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='font-size:22px; font-weight:bold;'>Rp {total_tagihan:,.0f}</p>", unsafe_allow_html=True)

        st.markdown("<h5 style='font-size:14px;font-weight:bold;'>Total Pendapatan dari Denda</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='font-size:22px; font-weight:bold;'>Rp {total_denda:,.0f}</p>", unsafe_allow_html=True)

        st.markdown("<h5 style='font-size:14px;font-weight:bold;'>Total Pendapatan Keseluruhan</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='font-size:22px; font-weight:bold;'>Rp {total_pendapatan:,.0f}</p>", unsafe_allow_html=True)

    # Peta nama bulan
    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
        7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
    }

    # Ganti angka bulan dengan nama bulan
    tagihan_per_bulan["month"] = tagihan_per_bulan["month"].map(month_map)
    denda_per_bulan["month"] = denda_per_bulan["month"].map(month_map)
    pendapatan_per_bulan["month"] = pendapatan_per_bulan["month"].map(month_map)

    # Warna visualisasi
    warna_blues = px.colors.sequential.Blues_r
    warna_tagihan = warna_blues[1]  
    warna_denda = warna_blues[3]  

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()

        # Ambil daftar tahun & wilayah unik
        years = sorted(tagihan_per_bulan["year"].unique())

        if pilihan_wilayah == "Semua Wilayah":
            wilayahs = ["Semua Wilayah"]
            color_palette = px.colors.qualitative.Set1  # Warna berdasarkan tahun
        else:
            wilayahs = sorted(tagihan_per_bulan["wilayah"].unique())
            color_palette = px.colors.qualitative.Dark24  # Warna berdasarkan wilayah

        # Tetapkan warna
        entity_colors = {entity: color_palette[i % len(color_palette)] for i, entity in enumerate(years if pilihan_wilayah == "Semua Wilayah" else wilayahs)}

        for year in years:
            for wilayah in wilayahs:
                if pilihan_wilayah == "Semua Wilayah":
                    df_filtered_tagihan = tagihan_per_bulan[tagihan_per_bulan["year"] == year]
                    line_color = entity_colors[year]  # Warna berdasarkan tahun
                else:
                    df_filtered_tagihan = tagihan_per_bulan[
                        (tagihan_per_bulan["year"] == year) & (tagihan_per_bulan["wilayah"] == wilayah)
                    ]
                    line_color = entity_colors[wilayah]  # Warna berdasarkan wilayah

                # Menghitung persentase perubahan dengan batasan agar tidak naik terlalu ekstrem
                df_filtered_tagihan["prev_tagihan"] = df_filtered_tagihan["tagihan"].shift(1)
                df_filtered_tagihan["change_percentage"] = (
                    (df_filtered_tagihan["tagihan"] - df_filtered_tagihan["prev_tagihan"]) / df_filtered_tagihan["prev_tagihan"]
                ) * 100

                # Pastikan nilai tidak berlebihan (>100%) atau negatif yang tidak masuk akal
                df_filtered_tagihan["change_percentage"] = df_filtered_tagihan["change_percentage"].clip(-100, 100)
                df_filtered_tagihan["change_percentage"] = df_filtered_tagihan["change_percentage"].fillna(0)

                # Ambil label perubahan untuk hover
                hover_text = [
                    f"Change: {change_percentage:.2f}%" 
                    for change_percentage in df_filtered_tagihan["change_percentage"]
                ]

                # Tambahkan label tahun hanya di titik terakhir
                text_labels = [str(year) if k == len(df_filtered_tagihan) - 1 else "" for k in range(len(df_filtered_tagihan))]

                # Garis Tagihan
                fig.add_trace(go.Scatter(
                    x=df_filtered_tagihan["month"], 
                    y=df_filtered_tagihan["tagihan"], 
                    mode="lines+markers+text",
                    name=f"{wilayah} ({year})",
                    legendgroup=str(year),  # Grup legend berdasarkan tahun
                    text=text_labels, 
                    textposition="top right",
                    line=dict(color=line_color, width=2),
                    hoverinfo="x+y+text+name",  
                    hovertext=hover_text,  
                    visible=True if year == years[-1] else "legendonly"
                ))

        # Layout Grafik
        fig.update_layout(
            title="Tagihan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Jumlah",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(categoryorder="array", categoryarray=list(month_map.values())),
            hovermode="closest",
            showlegend=True,
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Tambahkan kategori untuk Tagihan dan Denda
        denda_per_goltarif['kategori'] = 'Denda'
        pendapatan_per_goltarif['kategori'] = 'Tagihan'

        # Gabungkan data dengan penamaan yang sesuai
        if "wilayah" in denda_per_goltarif.columns and "wilayah" in pendapatan_per_goltarif.columns:
            combined_data = pd.concat([
                denda_per_goltarif[['kodegoltarif', 'denda', 'kategori', 'wilayah']],
                pendapatan_per_goltarif[['kodegoltarif', 'jumlahbayar', 'kategori', 'wilayah']].rename(columns={'jumlahbayar': 'denda'})
            ])
        else:
            combined_data = pd.concat([
                denda_per_goltarif[['kodegoltarif', 'denda', 'kategori']],
                pendapatan_per_goltarif[['kodegoltarif', 'jumlahbayar', 'kategori']].rename(columns={'jumlahbayar': 'denda'})
            ])

        # Warna biru visualisasi
        warna_tagihan = px.colors.sequential.Blues_r[2]  
        warna_denda = px.colors.sequential.Blues_r[5]    

        # Jika memilih "Semua Wilayah"
        if "wilayah" in combined_data.columns:
            fig = px.bar(
                combined_data,
                y="kodegoltarif",
                x="denda",
                color="kategori",
                facet_col="wilayah",
                text_auto=True,
                orientation="h",
                height = 380,
                color_discrete_map={
                    "Tagihan": warna_tagihan,
                    "Denda": warna_denda
                }
            )
        else: # Jika tidak memilih "Semua Wilayah"
            fig = px.bar(
                combined_data,
                y="kodegoltarif",
                x="denda",
                color="kategori",
                text_auto=True,
                orientation="h",
                height = 400,
                color_discrete_map={
                    "Tagihan": warna_tagihan,
                    "Denda": warna_denda
                }
            )

        fig.update_traces(textposition="outside")
        fig.update_layout(
            title="Tagihan dan Denda per Golongan Tarif",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="",  
            yaxis_title="Golongan Tarif",
            font=dict(color="#003366"),  
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(showgrid=False),
            yaxis=dict(categoryorder="total ascending"),
            barmode='group'
        )

        # Hapus semua label X bawaan dari subplot (facet_col)
        fig.for_each_xaxis(lambda xaxis: xaxis.update(title=""))

        # Ambil warna default dari sumbu Y
        warna_sumbu_y = fig.layout.yaxis.color if fig.layout.yaxis.color else "gray"

        # Tambahkan label X di tengah dengan warna abu-abu seperti Y-axis
        fig.add_annotation(
            text="Jumlah Tagihan dan Denda",
            x=0.5,  
            y=-0.2,  
            xref="paper", 
            yref="paper",
            showarrow=False,
            font=dict(size=14, color=warna_sumbu_y)  
        )

        st.plotly_chart(fig, use_container_width=True)
   
def show_dashboard_pengaduan():
    dim_pelanggan, _, _, dim_waktu, dim_jenispengaduan, fact_pengaduan, _, _, _ = load_data_cached()

    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pengaduan Pelanggan</h2>", unsafe_allow_html=True)

    # Cek apakah data tersedia
    if fact_pengaduan.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return

    # Merge data
    fact_pengaduan = fact_pengaduan.merge(dim_waktu, on="id_waktu", how="left")
    fact_pengaduan = fact_pengaduan.merge(dim_jenispengaduan, on="id_jenispengaduan", how="left")
    fact_pengaduan = fact_pengaduan.merge(dim_pelanggan, left_on="idpelanggan", right_on="kodepelanggan", how="left")
    fact_pengaduan["year"] = fact_pengaduan["year"].astype(str)  

    # Filter Rentang Tahun
    tahun_options = sorted(fact_pengaduan["year"].unique()) 
    tahun_min, tahun_max = st.sidebar.select_slider(
        "**Pilih Rentang Tahun**", 
        options=tahun_options,
        value=(tahun_options[0], tahun_options[-1])
    )

    # Filter berdasarkan tahun
    fact_pengaduan_filtered = fact_pengaduan[
        (fact_pengaduan["year"] >= tahun_min) & (fact_pengaduan["year"] <= tahun_max)
    ]

    # Filter Wilayah
    pilihan_wilayah = st.sidebar.radio("**Pilih Wilayah**", ["Semua Wilayah", "Pilih Wilayah"])

    if pilihan_wilayah == "Pilih Wilayah":
        wilayah_terpilih = []

        # Checkbox wilayah dalam 3 kolom
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.checkbox("Pusat", value=True):
                wilayah_terpilih.append("PS")
        with col2:
            if st.checkbox("Selatan", value=True):
                wilayah_terpilih.append("SN")
        with col1:
            if st.checkbox("Utara", value=True):
                wilayah_terpilih.append("UT")

        # Filter berdasarkan wilayah yang dipilih
        if wilayah_terpilih:
            fact_pengaduan_filtered = fact_pengaduan_filtered[fact_pengaduan_filtered["wilayah"].isin(wilayah_terpilih)]
   
    # Cek jika tidak ada wilayah yang dipilih (kecuali pilihan "Semua Wilayah" dianggap valid)
    if pilihan_wilayah == "Pilih Wilayah" and not wilayah_terpilih:
        st.warning("Silakan pilih **wilayah** terlebih dahulu di sidebar.")
        return

    # Hitung total pengaduan setelah filter
    total_pengaduan = len(fact_pengaduan_filtered)

    # Baris 1: Total Pengaduan (Card) di satu kolom
    col1, col2 = st.columns([1, 2])  

    with col1:
        # Card Total Pengaduan Keseluruhan 
        st.markdown( " ")
        st.markdown(
            "<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pengaduan Keseluruhan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pengaduan:,}</p>", 
            unsafe_allow_html=True
        )

    # Warna visualisasi
    warna_blue_r = px.colors.sequential.Blues_r[1::2]  

    with col2:
        # Jika Semua Wilayah 
        if pilihan_wilayah == "Semua Wilayah":
            pengaduan_tahunan = fact_pengaduan_filtered.groupby("year").size().reset_index(name="total_pengaduan")
            pengaduan_tahunan["year"] = pengaduan_tahunan["year"].astype(str)  
            fig = px.bar(
                pengaduan_tahunan,
                x="year",
                y="total_pengaduan",
                text="total_pengaduan",
                color_discrete_sequence=warna_blue_r,
                height=300
            )
        # Jika Pilih Wilayah 
        else:
            pengaduan_tahunan = fact_pengaduan_filtered.groupby(["year", "wilayah"]).size().reset_index(name="total_pengaduan")
            pengaduan_tahunan["year"] = pengaduan_tahunan["year"].astype(str)  # Ubah ke string
            fig = px.bar(
                pengaduan_tahunan,
                x="year",
                y="total_pengaduan",
                color="wilayah", 
                text="total_pengaduan",
                barmode="group",  
                color_discrete_sequence=warna_blue_r,
                 height=300
            )

        # Layout Grafik
        fig.update_layout(
            title="Total Pengaduan per Tahun",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Jumlah Pengaduan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)
 
    with col1:
        # Jika Semua Wilayah 
        if pilihan_wilayah == "Semua Wilayah":
            pengaduan_jenis = fact_pengaduan_filtered.groupby("jenis_pengaduan").size().reset_index(name="total_pengaduan")
            fig = px.bar(
                pengaduan_jenis,
                x="jenis_pengaduan",
                y="total_pengaduan",
                text_auto=True,
                color_discrete_sequence=warna_blue_r,
                height=550 
            )

        # Jika Pilih Wilayah 
        else:
            pengaduan_jenis = fact_pengaduan_filtered.groupby(["wilayah", "jenis_pengaduan"]).size().reset_index(name="total_pengaduan")
            fig = px.bar(
                pengaduan_jenis,
                x="jenis_pengaduan",
                y="total_pengaduan",
                color="wilayah",  
                text_auto=True,
                barmode="group",  
                color_discrete_sequence=warna_blue_r,
                height=550   
            )

        # Layout Grafik
        fig.update_layout(
            title="Total Pengaduan per Jenis",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Jenis Pengaduan",
            yaxis_title="Jumlah Pengaduan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20)
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)

    # Mapping angka bulan ke nama bulan
    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
        7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
    }

    with col2:
        fig = go.Figure()
        color_palette = [c for c in px.colors.qualitative.Dark24 if c.lower() != "#e31a1c"]

        # Fungsi untuk menghitung perubahan persentase dengan batasan
        def adjust_percentage_change(series):
            series = series.pct_change().fillna(0) * 100  
            series = series.replace([float("inf"), float("-inf")], 0)  
            series = series.clip(-100, 100)  
            return series

        # Jika memilih "Semua Wilayah", lakukan groupby tanpa wilayah
        if pilihan_wilayah == "Semua Wilayah":
            df_filtered = fact_pengaduan_filtered.groupby(["year", "month"]).size().reset_index(name="total_pengaduan")
        else:
            df_filtered = fact_pengaduan_filtered.groupby(["year", "month", "wilayah"]).size().reset_index(name="total_pengaduan")

        # Membuat daftar tahun yang ada dalam data
        tahun_terpilih = sorted(df_filtered["year"].unique())
        tahun_terakhir = tahun_terpilih[-1]

        # Pastikan setiap kombinasi tahun dan bulan ada
        all_months = pd.DataFrame([(y, m) for y in tahun_terpilih for m in range(1, 13)], columns=["year", "month"])

        if pilihan_wilayah == "Semua Wilayah":
            df_filtered = all_months.merge(df_filtered, on=["year", "month"], how="left").fillna(0)
        else:
            df_filtered = all_months.merge(df_filtered, on=["year", "month"], how="left").fillna(0)

        # Ubah angka bulan menjadi nama bulan
        df_filtered["month_num"] = df_filtered["month"]
        df_filtered["month"] = df_filtered["month_num"].map(month_map)

        # Jika memilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            for i, year in enumerate(tahun_terpilih):
                df_year = df_filtered[df_filtered["year"] == year].copy()
                
                # Menghitung perubahan dalam persentase menggunakan fungsi
                df_year["change"] = adjust_percentage_change(df_year["total_pengaduan"])

                text_labels = [f"{year}" if k == len(df_year) - 1 else "" for k in range(len(df_year))]

                fig.add_trace(go.Scatter(
                    x=df_year["month"], 
                    y=df_year["total_pengaduan"], 
                    mode="lines+markers+text",  
                    name=f"{year} - Total",  
                    legendgroup=str(year),  
                    line=dict(color=color_palette[i % len(color_palette)]),
                    hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                    customdata=df_year["change"],
                    visible=True if year == tahun_terakhir else "legendonly",
                    text=text_labels,
                    textposition="top right"
                ))

        # Jika memilih wilayah tertentu
        else:
            wilayah_list = df_filtered["wilayah"].unique()
            
            for i, year in enumerate(tahun_terpilih):
                df_year = df_filtered[df_filtered["year"] == year]

                for j, wilayah in enumerate(wilayah_list):
                    df_wilayah = df_year[df_year["wilayah"] == wilayah].copy()

                    if df_wilayah.empty:
                        continue  

                    # Menghitung perubahan dalam persentase menggunakan fungsi
                    df_wilayah["change"] = adjust_percentage_change(df_wilayah["total_pengaduan"])
                    
                    text_labels = [f"{year}" if k == len(df_wilayah) - 1 else "" for k in range(len(df_wilayah))]

                    fig.add_trace(go.Scatter(
                        x=df_wilayah["month"], 
                        y=df_wilayah["total_pengaduan"], 
                        mode="lines+markers+text",  
                        name=f"{year} - {wilayah}",  
                        legendgroup=str(year),  
                        line=dict(color=color_palette[j % len(color_palette)]),
                        hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                        customdata=df_wilayah["change"],
                        visible=True if year == tahun_terakhir else "legendonly",
                        text=text_labels,
                        textposition="top right"
                    ))

        # Layout Grafik
        fig.update_layout(
            title="Pengaduan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Jumlah Pengaduan",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                categoryorder="array",
                categoryarray=[month_map[i] for i in range(1, 13)],
                tickmode="array",
                tickvals=[month_map[i] for i in range(1, 13)],
                ticktext=[month_map[i] for i in range(1, 13)]
            ),
            legend_title="Tahun - Wilayah",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            height=350
        )

        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_pemutusan():
    dim_pelanggan, fact_transaksi, dim_goltarif, dim_waktu, _, _, dim_realisasi, fact_pemutusan,_ = load_data_cached()

    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pemutusan Sambungan</h2>", unsafe_allow_html=True)
        
    if fact_pemutusan.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return

    fact_pemutusan = fact_pemutusan.merge(dim_waktu, on="id_waktu", how="left")
    fact_pemutusan = fact_pemutusan.merge(dim_realisasi, on="id_realisasi", how="left")
    fact_pemutusan = fact_pemutusan.merge(dim_pelanggan, on="kodepelanggan", how="left")

    fact_pemutusan["year"] = fact_pemutusan["year"].astype(str)
    fact_pemutusan["month"] = pd.to_numeric(fact_pemutusan["month"])  

    # Filter berdasarkan tahun
    tahun_options = sorted(fact_pemutusan["year"].unique())
    tahun_min, tahun_max = st.sidebar.select_slider(
        "**Pilih Rentang Tahun**", 
        options=tahun_options,
        value=(tahun_options[0], tahun_options[-1])
    )

    fact_pemutusan_filtered = fact_pemutusan[
        (fact_pemutusan["year"] >= tahun_min) & (fact_pemutusan["year"] <= tahun_max)
    ]

    # Filter berdasarkan wilayah
    pilihan_wilayah = st.sidebar.radio("**Pilih Wilayah**", ["Semua Wilayah", "Pilih Wilayah"])

    wilayah_terpilih = None  
    
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
            fact_pemutusan_filtered = fact_pemutusan_filtered[fact_pemutusan_filtered["wilayah"].isin(wilayah_terpilih)]

    # Cek jika tidak ada wilayah yang dipilih (kecuali pilihan "Semua Wilayah" dianggap valid)
    if pilihan_wilayah == "Pilih Wilayah" and not wilayah_terpilih:
        st.warning("Silakan pilih **wilayah** terlebih dahulu di sidebar.")
        return

    total_pemutusan = fact_pemutusan_filtered[["kodepelanggan", "id_waktu"]].shape[0]

    # Pemutusan per Bulan
    if pilihan_wilayah == "Semua Wilayah":
        pemutusan_bulanan = (
            fact_pemutusan_filtered
            .groupby(["year", "month"])["kodepelanggan"]
            .nunique()
            .reset_index()
        )
    else:
        pemutusan_bulanan = (
            fact_pemutusan_filtered
            .groupby(["year", "month", "wilayah"])["kodepelanggan"]
            .nunique()
            .reset_index()
        )

    # Pemutusan berdasarkan Golongan Tarif
    pemutusan_goltarif = (
        fact_pemutusan_filtered
        .merge(fact_transaksi, on="kodepelanggan")
        .merge(dim_goltarif, on="kodegoltarif")
    )

    if pilihan_wilayah == "Semua Wilayah":
        pemutusan_goltarif = (
            pemutusan_goltarif
            .groupby("kodegoltarif")["kodepelanggan"]
            .nunique()
            .reset_index()
        )
    else:
        pemutusan_goltarif = (
            pemutusan_goltarif
            .groupby(["kodegoltarif", "wilayah"])["kodepelanggan"]
            .nunique()
            .reset_index()
        )

    # Layout utama
    col1, col2 = st.columns([3, 7])  

    with col1:
        # Card Total Pemutusan Sambungan
        st.markdown("")
        st.markdown("")
        st.markdown(
            "<h5 style='text-align: center; font-size:16px;font-weight:bold;'>Total Pemutusan Sambungan</h5>", 
            unsafe_allow_html=True
        )
        st.markdown(
            f"<p style='text-align: center; font-size:26px; font-weight:bold;'>{total_pemutusan:,}</p>", 
            unsafe_allow_html=True
        )
    
    warna_blue_r = px.colors.sequential.Blues_r[1::2] 

    with col1:
        # Jika "Semua Wilayah" 
        if pilihan_wilayah == "Semua Wilayah":
            pemutusan_tahunan = fact_pemutusan_filtered.groupby("year").size().reset_index(name="total_pemutusan")
            pemutusan_tahunan["year"] = pemutusan_tahunan["year"].astype(str)  # Ubah ke string

            fig = px.bar(
                pemutusan_tahunan,
                x="year",
                y="total_pemutusan",
                text="total_pemutusan",
                color_discrete_sequence=warna_blue_r,
                height=600
            )

        # Jika "Pilih Wilayah"
        else:
            pemutusan_tahunan = fact_pemutusan_filtered.groupby(["year", "wilayah"]).size().reset_index(name="total_pemutusan")
            pemutusan_tahunan["year"] = pemutusan_tahunan["year"].astype(str)  # Ubah ke string

            fig = px.bar(
                pemutusan_tahunan,
                x="year",
                y="total_pemutusan",
                color="wilayah", 
                text="total_pemutusan",
                barmode="group",  
                color_discrete_sequence=warna_blue_r,
                height = 600
            )

        # Layout Grafik
        fig.update_layout(
            title="Total Pemutusan per Tahun",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Jumlah Pemutusan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = go.Figure()

        # Pilihan palet warna yang kontras 
        color_palette = [c for c in px.colors.qualitative.Dark24 if c.lower() != "#e31a1c"]  

        # Urutkan tahun yang dipilih
        tahun_terpilih = sorted(pemutusan_bulanan["year"].unique())  
        tahun_terakhir = tahun_terpilih[-1]  

        # Mapping angka bulan ke nama bulan
        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun", 
            7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
        }

        # Urutkan data berdasarkan bulan numerik
        pemutusan_bulanan["month_numeric"] = pemutusan_bulanan["month"]
        pemutusan_bulanan["month_name"] = pemutusan_bulanan["month_numeric"].map(month_map)  

        # Pastikan setiap kombinasi tahun dan bulan ada, untuk menghindari data kosong
        all_months = pd.DataFrame([(y, m) for y in tahun_terpilih for m in range(1, 13)], columns=["year", "month_numeric"])
        pemutusan_bulanan = all_months.merge(pemutusan_bulanan, on=["year", "month_numeric"], how="left").fillna(0)

        # Fungsi untuk membatasi lonjakan persentase
        def adjust_percentage_change(series):
            series = series.pct_change().fillna(0) * 100
            series = series.replace([float("inf"), float("-inf")], 0)
            series = series.clip(-100, 100)  # Batasi agar tidak terlalu ekstrem
            return series

        # Jika memilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            for i, year in enumerate(tahun_terpilih):
                df_year = pemutusan_bulanan[pemutusan_bulanan["year"] == year].groupby("month_numeric")["kodepelanggan"].sum().reset_index()

                # Mengurutkan bulan numerik
                df_year = df_year.sort_values("month_numeric")

                # Menghitung perubahan dalam persentase dengan batas maksimum
                df_year["change"] = adjust_percentage_change(df_year["kodepelanggan"])

                # Menambahkan label tahun hanya di titik terakhir
                text_labels = [f"{year}" if k == len(df_year) - 1 else "" for k in range(len(df_year))]

                fig.add_trace(go.Scatter(
                    x=df_year["month_numeric"].apply(lambda x: month_map.get(x, x)),  
                    y=df_year["kodepelanggan"], 
                    mode="lines+markers+text",  
                    name=str(year),  
                    legendgroup=str(year),  
                    line=dict(
                        color=color_palette[i % len(color_palette)],  
                        width=2  
                    ),  
                    hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                    customdata=df_year["change"],
                    visible=True if year == tahun_terakhir else "legendonly",
                    text=text_labels,  
                    textposition="top right"  
                ))

        # Jika memilih wilayah tertentu
        else:
            wilayah_list = pemutusan_bulanan["wilayah"].unique()  

            for i, year in enumerate(tahun_terpilih):
                df_year = pemutusan_bulanan[pemutusan_bulanan["year"] == year]

                for j, wilayah in enumerate(wilayah_list):
                    df_wilayah = df_year[df_year["wilayah"] == wilayah]

                    if df_wilayah.empty:
                        continue  

                    # Mengurutkan bulan numerik
                    df_wilayah = df_wilayah.sort_values("month_numeric")

                    # Menghitung perubahan dalam persentase dengan batas maksimum
                    df_wilayah["change"] = adjust_percentage_change(df_wilayah["kodepelanggan"])

                    # Menambahkan label tahun hanya di titik terakhir
                    text_labels = [f"{year}" if k == len(df_wilayah) - 1 else "" for k in range(len(df_wilayah))]

                    fig.add_trace(go.Scatter(
                        x=df_wilayah["month_numeric"].apply(lambda x: month_map.get(x, x)),  
                        y=df_wilayah["kodepelanggan"], 
                        mode="lines+markers+text",  
                        name=f"{year} - {wilayah}",  
                        legendgroup=str(year),  
                        line=dict(
                            color=color_palette[j % len(color_palette)],  
                            width=2  
                        ),
                        hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                        customdata=df_wilayah["change"],
                        visible=True if year == tahun_terakhir else "legendonly",
                        text=text_labels,  
                        textposition="top right"  
                    ))

        # Layout Grafik
        fig.update_layout(
            title="Pemutusan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Jumlah Pemutusan",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                tickmode="array",
                tickvals=list(month_map.values()),  
                ticktext=list(month_map.values())  
            ),
            legend_title="Tahun - Wilayah",
            font=dict(color="#003366"),
            height=350
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Tambahkan kategori untuk Pemutusan
        pemutusan_goltarif["kategori"] = "Pemutusan"

        # Jika memilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            combined_data = (
                pemutusan_goltarif
                .groupby("kodegoltarif")["kodepelanggan"]
                .sum()
                .reset_index()
            )
            combined_data["kategori"] = "Pemutusan"

        else:
            if "wilayah" in pemutusan_goltarif.columns:
                combined_data = pemutusan_goltarif
            else:
                combined_data = pemutusan_goltarif.drop(columns=["wilayah"], errors="ignore")

        warna_pemutusan = px.colors.sequential.Blues_r[2]  

        # Jika memilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            fig = px.bar(
                combined_data,
                y="kodegoltarif",
                x="kodepelanggan",
                color="kategori",
                text_auto=True,
                orientation="h",
                color_discrete_map={"Pemutusan": warna_pemutusan}
            )
        else:
            fig = px.bar(
                combined_data,
                y="kodegoltarif",
                x="kodepelanggan",
                color="kategori",
                facet_col="wilayah",
                text_auto=True,
                orientation="h",
                color_discrete_map={"Pemutusan": warna_pemutusan}
            )

        fig.update_traces(textposition="outside")
        fig.update_layout(
            title="Pemutusan per Golongan Tarif",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Jumlah Pemutusan",
            yaxis_title="Golongan Tarif",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(showgrid=False),
            yaxis=dict(categoryorder="total ascending"),
            barmode="group",
            height = 350,
            showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_sbbaru():
    _, _, _, dim_waktu, _, _, dim_realisasi, _, fact_sbbaru = load_data_cached()

    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Dashboard Pendaftaran Sambungan Baru</h2>", unsafe_allow_html=True)

    # Cek apakah data fact_sbbaru kosong
    if fact_sbbaru.empty:
        st.warning("Belum ada data yang ingin ditampilkan.")
        return

    # Merge data
    fact_sbbaru = fact_sbbaru.merge(dim_waktu, on="id_waktu", how="left")
    fact_sbbaru = fact_sbbaru.merge(dim_realisasi, on="id_realisasi", how="left")
    
    fact_sbbaru["year"] = fact_sbbaru["year"].astype(str)
    fact_sbbaru["month"] = pd.to_numeric(fact_sbbaru["month"])

    # **Pilih Rentang Tahun**
    tahun_options = sorted(fact_sbbaru["year"].unique())
    tahun_min, tahun_max = st.sidebar.select_slider(
        "**Pilih Rentang Tahun**", 
        options=tahun_options,
        value=(tahun_options[0], tahun_options[-1])
    )

    # **Filter Wilayah (dengan 3 kolom checkbox)**
    pilihan_wilayah = st.sidebar.radio("**Pilih Wilayah**", ["Semua Wilayah", "Pilih Wilayah"])

    wilayah_terpilih = fact_sbbaru["wilayah"].dropna().unique().tolist()  

    if pilihan_wilayah == "Pilih Wilayah":
        wilayah_terpilih = []

        # Ambil daftar wilayah unik dari fact_sbbaru
        wilayah_options = sorted(fact_sbbaru["wilayah"].dropna().unique())

        # Bagi pilihan wilayah menjadi 3 kolom
        col1, col2, col3 = st.sidebar.columns(3)
        for i, wilayah in enumerate(wilayah_options):
            if i % 3 == 0:
                with col1:
                    if st.checkbox(wilayah, value=True):
                        wilayah_terpilih.append(wilayah)
            elif i % 3 == 1:
                with col2:
                    if st.checkbox(wilayah, value=True):
                        wilayah_terpilih.append(wilayah)
            else:
                with col3:
                    if st.checkbox(wilayah, value=True):
                        wilayah_terpilih.append(wilayah)

    # Cek jika tidak ada wilayah yang dipilih (kecuali pilihan "Semua Wilayah" dianggap valid)
    if pilihan_wilayah == "Pilih Wilayah" and not wilayah_terpilih:
        st.warning("Silakan pilih **wilayah** terlebih dahulu di sidebar.")
        return
    
    # Filter Pelanggan Aktif (Y)
    include_y_pelanggan = st.sidebar.radio(
        "**Tampilkan Pelanggan yang Sudah Aktif (Y)?**",
        ["Ya", "Tidak"],
        index=0  
    )

    # Terapkan Filter Data
    fact_sbbaru_filtered = fact_sbbaru[
        (fact_sbbaru["year"] >= tahun_min) & (fact_sbbaru["year"] <= tahun_max)
    ]

    if pilihan_wilayah == "Pilih Wilayah" and wilayah_terpilih:
        fact_sbbaru_filtered = fact_sbbaru_filtered[fact_sbbaru_filtered["wilayah"].isin(wilayah_terpilih)]

    if include_y_pelanggan == "Tidak":
        fact_sbbaru_filtered = fact_sbbaru_filtered[fact_sbbaru_filtered["jenis_realisasi"] != "Y"]

    # Cek apakah data kosong setelah filter
    if fact_sbbaru_filtered.empty:
        st.warning("Tidak ada data yang sesuai dengan filter yang dipilih.")
        return

    # Data Agregasi
    total_pendaftaran = fact_sbbaru_filtered.shape[0]
    total_biaya = fact_sbbaru_filtered['jumlah'].sum()
    pendaftaran_bulanan = fact_sbbaru_filtered.groupby(["year", "month", "wilayah"]).agg({"kodecpelanggan": "nunique"}).reset_index()
    biaya_bulanan = fact_sbbaru_filtered.groupby(["year", "month", "wilayah"]).agg({"jumlah": "sum"}).reset_index()
    pendaftaran_realisasi = fact_sbbaru_filtered.groupby(["jenis_realisasi", "wilayah", "year"]).size().reset_index(name="total_pendaftaran")

    warna_blue_r = px.colors.sequential.Blues_r[1::2]

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<h5 style='text-align: center; font-size:14px;font-weight:bold;'>Total Pendaftaran Sambungan Baru</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; font-size:22px; font-weight:bold;'>{total_pendaftaran:,}</p>", unsafe_allow_html=True)

    with col2:
        st.markdown("<h5 style='text-align: center; font-size:14px;font-weight:bold;'>Total Biaya Sambungan Baru</h5>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; font-size:22px; font-weight:bold;'>{total_biaya:,}</p>", unsafe_allow_html=True)

    col1, col2 = st.columns([4, 7])
    
    with col1:
        # Jika "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            df_biaya = biaya_bulanan.groupby("year")["jumlah"].sum().reset_index()
            df_biaya["year"] = df_biaya["year"].astype(str)  # Ubah ke string

            fig = px.bar(
                df_biaya,
                x="year",
                y="jumlah",
                text="jumlah",
                color_discrete_sequence=warna_blue_r,
                height=350
            )

        # Jika memilih wilayah tertentu
        else:
            df_biaya = biaya_bulanan.groupby(["year", "wilayah"])["jumlah"].sum().reset_index()
            df_biaya["year"] = df_biaya["year"].astype(str)  # Ubah ke string

            fig = px.bar(
                df_biaya,
                x="year",
                y="jumlah",
                color="wilayah",
                text="jumlah",
                barmode="group",
                color_discrete_sequence=warna_blue_r,
                height=350
            )
        fig.update_traces(
            texttemplate="%{text:,}",  
            textposition="outside"  
        )

        # Layout Grafik
        fig.update_layout(
            title="Total Biaya Pemasangan per Tahun",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun",
            yaxis_title="Total Biaya Pemasangan",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Tambahkan kategori berdasarkan jenis realisasi
        pendaftaran_realisasi['kategori'] = pendaftaran_realisasi['jenis_realisasi']

        # Cek dan ubah nama kolom jika perlu
        if "year" not in pendaftaran_realisasi.columns:
            pendaftaran_realisasi.rename(columns={"tahun": "year"}, inplace=True)

        # Jika pilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            df_pendaftaran = pendaftaran_realisasi.groupby(["year", "kategori"]).sum().reset_index()
        # Jika pilih "Pilih Wilayah"
        elif "wilayah" in pendaftaran_realisasi.columns:
            df_pendaftaran = pendaftaran_realisasi.groupby(["year", "kategori", "wilayah"]).sum().reset_index()
        else:
            df_pendaftaran = pendaftaran_realisasi.groupby(["year", "kategori"]).sum().reset_index()

        warna_blue_r2 = px.colors.sequential.Blues_r

        # Visualisasi jika semua wilayah
        if pilihan_wilayah == "Semua Wilayah":
            fig = px.bar(
                df_pendaftaran,
                x="year",  
                y="total_pendaftaran",  
                color="kategori",
                text_auto=True,
                height=350,
                color_discrete_sequence=warna_blue_r2
            )
        # Visualisasi jika pilih wilayah tertentu
        else:
            fig = px.bar(
                df_pendaftaran,
                x="year",  
                y="total_pendaftaran",  
                color="kategori",
                facet_col="wilayah",  
                text_auto=True,
                height=350,
                color_discrete_sequence=warna_blue_r2
            )

        # Konfigurasi tampilan grafik
        fig.update_traces(textposition="outside")
        fig.update_layout(
            title="Pendaftaran per Tahun",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Tahun", 
            yaxis_title="Total Pendaftaran",
            font=dict(color="#003366"),
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(showgrid=False, type="category"), 
            yaxis=dict(categoryorder="total ascending"),
            barmode="group"
        )

        # Tampilkan grafik
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2) 

    with col1:
        fig = go.Figure()

        # Pilihan palet warna yang kontras
        color_palette = [c for c in px.colors.qualitative.Dark24 if c.lower() != "#e31a1c"]

        # Urutkan tahun yang dipilih
        tahun_terpilih = sorted(pendaftaran_bulanan["year"].unique())
        tahun_terakhir = tahun_terpilih[-1]  # Tahun terakhir sebagai yang utama

        # Mapping angka bulan ke nama bulan
        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
            7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
        }
        pendaftaran_bulanan["month"] = pendaftaran_bulanan["month"].map(month_map)

        # Pastikan semua bulan tersedia dalam urutan yang benar
        all_months = list(month_map.values())

        # Fungsi untuk membatasi lonjakan perubahan persentase
        def adjust_percentage_change(series):
            series = series.pct_change().fillna(0) * 100  # Hitung perubahan persentase
            series = series.replace([float("inf"), float("-inf")], 0)  # Hilangkan inf
            series = series.clip(-100, 100)  # Batasi perubahan agar tidak ekstrem
            return series

        # Jika memilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            for i, year in enumerate(tahun_terpilih):
                df_year = pendaftaran_bulanan[pendaftaran_bulanan["year"] == year].groupby("month")["kodecpelanggan"].sum()
                df_year = df_year.reindex(all_months, fill_value=0).reset_index()

                # Menghitung perubahan dalam persentase dengan batas maksimum
                df_year["change"] = adjust_percentage_change(df_year["kodecpelanggan"])

                # Menentukan label hanya untuk titik terakhir
                text_labels = [""] * (len(df_year) - 1) + [str(year)]  

                fig.add_trace(go.Scatter(
                    x=df_year["month"],
                    y=df_year["kodecpelanggan"],
                    mode="lines+markers+text",
                    name=str(year),
                    legendgroup=str(year),
                    line=dict(color=color_palette[i % len(color_palette)], width=2),
                    hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                    customdata=df_year["change"],
                    text=text_labels,  
                    textposition="top right",  
                    visible=True if year == tahun_terakhir else "legendonly"
                ))

        # Jika memilih wilayah tertentu
        else:
            for i, year in enumerate(tahun_terpilih):
                df_year = pendaftaran_bulanan[(pendaftaran_bulanan["year"] == year) & (pendaftaran_bulanan["wilayah"].isin(wilayah_terpilih))]

                for j, wilayah in enumerate(wilayah_terpilih):
                    df_wilayah = df_year[df_year["wilayah"] == wilayah].groupby("month")["kodecpelanggan"].sum()
                    df_wilayah = df_wilayah.reindex(all_months, fill_value=0).reset_index()

                    # Menghitung perubahan dalam persentase dengan batas maksimum
                    df_wilayah["change"] = adjust_percentage_change(df_wilayah["kodecpelanggan"])

                    # Menentukan label hanya untuk titik terakhir
                    text_labels = [""] * (len(df_wilayah) - 1) + [str(year)]  

                    fig.add_trace(go.Scatter(
                        x=df_wilayah["month"],
                        y=df_wilayah["kodecpelanggan"],
                        mode="lines+markers+text",
                        name=f"{year} - {wilayah}",
                        legendgroup=str(year),
                        line=dict(color=color_palette[j % len(color_palette)], width=2),
                        hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                        customdata=df_wilayah["change"],
                        text=text_labels,  
                        textposition="top right",  
                        visible=True if year == tahun_terakhir else "legendonly"
                    ))

        # Layout Grafik
        fig.update_layout(
            title="Pendaftaran per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Jumlah Pendaftaran",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(tickmode="array", tickvals=all_months, ticktext=all_months),
            legend_title="Tahun - Wilayah",
            font=dict(color="#003366"),
            height=350
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = go.Figure()

        # Pilihan palet warna yang kontras
        color_palette = [c for c in px.colors.qualitative.Dark24 if c.lower() != "#e31a1c"]

        # Urutkan tahun yang dipilih
        tahun_terpilih = sorted(biaya_bulanan["year"].unique())
        tahun_terakhir = tahun_terpilih[-1]  # Tahun terbaru sebagai yang utama

        # Mapping angka bulan ke nama bulan
        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mei", 6: "Jun",
            7: "Jul", 8: "Agu", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
        }
        biaya_bulanan["month"] = biaya_bulanan["month"].map(month_map)

        # Pastikan semua bulan tersedia dalam urutan yang benar
        all_months = list(month_map.values())

        # Fungsi untuk membatasi lonjakan perubahan persentase
        def adjust_percentage_change(series):
            series = series.pct_change().fillna(0) * 100  # Hitung perubahan persentase
            series = series.replace([float("inf"), float("-inf")], 0)  # Hilangkan inf
            series = series.clip(-100, 100)  # Batasi perubahan agar tidak ekstrem
            return series

        # Jika memilih "Semua Wilayah"
        if pilihan_wilayah == "Semua Wilayah":
            for i, year in enumerate(tahun_terpilih):
                df_year = biaya_bulanan[biaya_bulanan["year"] == year].groupby("month")["jumlah"].sum()
                df_year = df_year.reindex(all_months, fill_value=0).reset_index()

                # Menghitung perubahan dalam persentase dengan batas maksimum
                df_year["change"] = adjust_percentage_change(df_year["jumlah"])

                # Menentukan label hanya untuk titik terakhir
                text_labels = [""] * (len(df_year) - 1) + [str(year)]

                fig.add_trace(go.Scatter(
                    x=df_year["month"],
                    y=df_year["jumlah"],
                    mode="lines+markers+text",
                    name=str(year),
                    legendgroup=str(year),
                    line=dict(color=color_palette[i % len(color_palette)], width=2),
                    hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                    customdata=df_year["change"],
                    text=text_labels,
                    textposition="top right",
                    visible=True if year == tahun_terakhir else "legendonly"
                ))

        # Jika memilih wilayah tertentu
        else:
            for i, year in enumerate(tahun_terpilih):
                df_year = biaya_bulanan[(biaya_bulanan["year"] == year) & (biaya_bulanan["wilayah"].isin(wilayah_terpilih))]

                for j, wilayah in enumerate(wilayah_terpilih):
                    df_wilayah = df_year[df_year["wilayah"] == wilayah].groupby("month")["jumlah"].sum()
                    df_wilayah = df_wilayah.reindex(all_months, fill_value=0).reset_index()

                    # Menghitung perubahan dalam persentase dengan batas maksimum
                    df_wilayah["change"] = adjust_percentage_change(df_wilayah["jumlah"])

                    # Menentukan label hanya untuk titik terakhir
                    text_labels = [""] * (len(df_wilayah) - 1) + [str(year)]

                    fig.add_trace(go.Scatter(
                        x=df_wilayah["month"],
                        y=df_wilayah["jumlah"],
                        mode="lines+markers+text",
                        name=f"{year} - {wilayah}",
                        legendgroup=str(year),
                        line=dict(color=color_palette[j % len(color_palette)], width=2),
                        hovertemplate="%{x}, %{y:,.0f}<br>Perubahan: %{customdata:.2f}%",
                        customdata=df_wilayah["change"],
                        text=text_labels,
                        textposition="top right",
                        visible=True if year == tahun_terakhir else "legendonly"
                    ))

        # Layout Grafik
        fig.update_layout(
            title="Total Biaya Pemasangan per Bulan",
            xaxis_title="Bulan",
            yaxis_title="Total Biaya Pemasangan",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(tickmode="array", tickvals=all_months, ticktext=all_months),
            legend_title="Tahun - Wilayah",
            font=dict(color="#003366"),
            height=350
        )

        # Menampilkan Grafik
        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_forecasting():
    show_forecasting()

def show_tabel():
    tabel()

# Load dan resize gambar
logo_path = "dashboard/Logo Perumda.png"

# Membuat 3 kolom di sidebar
col1, col2, col3 = st.sidebar.columns([1, 6, 1])

# Menempatkan gambar di kolom kedua
if os.path.exists(logo_path):
    with col2:
        st.image(logo_path, width=160)
else:
    st.sidebar.error("Gambar tidak ditemukan! Periksa kembali path-nya.")

st.sidebar.markdown("---")
st.sidebar.title("Menu")

if "page" not in st.session_state:
    st.session_state["page"] = "Dashboard"
    st.session_state["dashboard_choice"] = "Dashboard Pelanggan"

col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button("Dashboard", key="dashboard_button"):
        st.session_state["page"] = "Dashboard"
        st.rerun()

with col2:
    if st.button("Proses Data", key="proses_data_button"):
        st.session_state["page"] = "Proses Data"
        st.rerun()

if st.session_state["page"] == "Dashboard":
    dashboard_options = ["Pelanggan", "Pemakaian Air", "Pendapatan", "Pengaduan", "Pemutusan", "Sambungan Baru", "Forecast", "Rincian Data"]
    
    st.session_state["dashboard_choice"] = st.sidebar.selectbox(
        "Pilih Dashboard", 
        dashboard_options, 
        index=0
    )

    if st.session_state["dashboard_choice"] == "Pelanggan":
        show_dashboard_pelanggan()
    elif st.session_state["dashboard_choice"] == "Pemakaian Air":
        show_dashboard_pemakaian_air()
    elif st.session_state["dashboard_choice"] == "Pendapatan":
        show_dashboard_pendapatan()
    elif st.session_state["dashboard_choice"] == "Pengaduan":
        show_dashboard_pengaduan()
    elif st.session_state["dashboard_choice"] == "Pemutusan":
        show_dashboard_pemutusan()
    elif st.session_state["dashboard_choice"] == "Sambungan Baru":
        show_dashboard_sbbaru()
    elif st.session_state["dashboard_choice"] == "Forecast":
        show_dashboard_forecasting()
    elif st.session_state["dashboard_choice"] == "Rincian Data":
        show_tabel()

elif st.session_state["page"] == "Proses Data":
    proses.show_proses_etl()