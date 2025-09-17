import streamlit as st
import pandas as pd
import io
from sqlalchemy import create_engine, text

# Setup koneksi
DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
engine = create_engine(DATABASE_URL)

# Ambil data dari query SQL
def load_data(query):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

# Fungsi agregasi per bulan
def get_monthly_summary(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    df["bulan"] = df[date_column].dt.month
    df["tahun"] = df[date_column].dt.year
    df["nama_bulan"] = df[date_column].dt.strftime("%B")
    return df

# Fungsi gabung agregasi
def merge_aggregations(aggregations: list, on_cols=["tahun", "nama_bulan"]):
    from functools import reduce
    return reduce(lambda left, right: pd.merge(left, right, on=on_cols, how="outer"), aggregations).fillna(0)

def get_dict_column(df, group_cols, target_col):
    return df.groupby(group_cols)[target_col].value_counts().unstack().fillna(0).astype(int).groupby(level=0).apply(lambda x: x.to_dict(orient="records")[0]).reset_index(name=target_col)

def dict_to_multiline(d):
    return "\n".join(f"{k}: {v}" for k, v in d.items())

@st.cache_data(show_spinner=False)
def create_excel(dataframe, sheet_name):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()

# Fungsi utama
def tabel():
    st.markdown("<h2 style='text-align: center; font-size:40px;font-weight:bold;'>Rincian Data</h2>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Pilih Tanggal Mulai")
    with col2:
        end_date = st.date_input("Pilih Tanggal Akhir")

    if start_date > end_date:
        st.warning("Tanggal mulai tidak boleh lebih besar dari tanggal akhir.")
        return

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    tab1, tab2, tab3, tab4 = st.tabs(["Transaksi", "Pengaduan", "Pemutusan", "Sambungan Baru"])

    # Tab Transaksi
    with tab1:
        df = load_data(f"""
            SELECT 
                ft.kodepelanggan, ft.jumlahbayar, ft.kodegoltarif, ft.denda, ft.tagihan, ft.pemakaian,
                dp.status, dp.wilayah, dw.date
            FROM fact_transaksi ft
            LEFT JOIN dim_pelanggan dp ON ft.kodepelanggan = dp.kodepelanggan
            LEFT JOIN dim_waktu dw ON ft.id_waktu = dw.id_waktu
            WHERE dw.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """)

        st.subheader("Data Transaksi")
        mode = st.radio("Filter Tampilan", ["Seluruh Data", "Per Bulan"], horizontal=True)

        if df.empty:
            st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
        else:
            if mode == "Seluruh Data":
                st.markdown(f"<p style='font-size:10pt;'>Jumlah data: {len(df)}</p>", unsafe_allow_html=True)

                # Tampilkan tabel dulu
                st.dataframe(df, use_container_width=True)

                # Spinner sambil generate Excel
                with st.spinner("Mempersiapkan file Excel..."):
                    excel_data = create_excel(df, "Seluruh Data")

                # Tombol download setelah excel siap
                st.download_button(
                    label="Unduh Excel",
                    data=excel_data,
                    file_name="data_transaksi_seluruh.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            else:
                df = get_monthly_summary(df, "date")

                ringkasan = df.groupby(["tahun", "nama_bulan"]).agg({
                    "jumlahbayar": "sum",
                    "denda": "sum",
                    "tagihan": "sum",
                    "pemakaian": "sum"
                }).reset_index()

                goltarif = df.groupby(["tahun", "nama_bulan", "kodegoltarif"])["jumlahbayar"].sum().unstack(fill_value=0)
                goltarif = goltarif.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                    lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                ).reset_index(name="jumlahbayar_per_goltarif")

                wilayah = df.groupby(["tahun", "nama_bulan", "wilayah"])["jumlahbayar"].sum().unstack(fill_value=0)
                wilayah = wilayah.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                    lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                ).reset_index(name="jumlahbayar_per_wilayah")

                goltarif_count = df.groupby(["tahun", "nama_bulan"])["kodegoltarif"].value_counts().unstack(fill_value=0)
                goltarif_count = goltarif_count.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                    lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                ).reset_index(name="kodegoltarif")

                status = df.groupby(["tahun", "nama_bulan"])["status"].value_counts().unstack(fill_value=0)
                status = status.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                    lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                ).reset_index(name="status")

                wilayah_count = df.groupby(["tahun", "nama_bulan"])["wilayah"].value_counts().unstack(fill_value=0)
                wilayah_count = wilayah_count.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                    lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                ).reset_index(name="wilayah")

                hasil = ringkasan.merge(goltarif_count, on=["tahun", "nama_bulan"]) \
                    .merge(status, on=["tahun", "nama_bulan"]) \
                    .merge(wilayah_count, on=["tahun", "nama_bulan"]) \
                    .merge(goltarif, on=["tahun", "nama_bulan"]) \
                    .merge(wilayah, on=["tahun", "nama_bulan"])

                hasil["kodegoltarif"] = hasil["kodegoltarif"].apply(dict_to_multiline)
                hasil["status"] = hasil["status"].apply(dict_to_multiline)
                hasil["wilayah"] = hasil["wilayah"].apply(dict_to_multiline)
                hasil["jumlahbayar_per_goltarif"] = hasil["jumlahbayar_per_goltarif"].apply(dict_to_multiline)
                hasil["jumlahbayar_per_wilayah"] = hasil["jumlahbayar_per_wilayah"].apply(dict_to_multiline)

                hasil = hasil.rename(columns={
                    "nama_bulan": "bulan",
                    "jumlahbayar": "jumlah bayar",
                    "pemakaian": "pemakaian air",
                    "kodegoltarif": "jumlah pelanggan per gol tarif",
                    "status": "status pelanggan",
                    "jumlahbayar_per_goltarif": "jumlah bayar per gol tarif",
                    "jumlahbayar_per_wilayah": "jumlah bayar per wilayah"
                })

                # Tampilkan tabel dulu
                st.dataframe(hasil)

                with st.spinner("Mempersiapkan file Excel..."):
                    excel_data = create_excel(hasil, "Per Bulan")

                st.download_button(
                    label="Unduh Excel",
                    data=excel_data,
                    file_name="data_transaksi_per_bulan.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    # Tab Pengaduan
    with tab2:
        # Load data
        df = load_data(f"""
            SELECT dp.kodepelanggan, dp.status, dp.wilayah, djp.jenis_pengaduan, dw.date
            FROM fact_pengaduan fp
            LEFT JOIN dim_pelanggan dp ON fp.idpelanggan = dp.kodepelanggan
            LEFT JOIN dim_waktu dw ON fp.id_waktu = dw.id_waktu
            LEFT JOIN dim_jenispengaduan djp ON fp.id_jenispengaduan = djp.id_jenispengaduan
            WHERE dw.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """)

        st.subheader("Data Pengaduan")
        mode = st.radio("Filter Tampilan", ["Seluruh Data", "Per Bulan"], horizontal=True, key="pengaduan")

        if df.empty:
            st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
        else:
            if mode == "Seluruh Data":
                st.markdown(f"<p style='font-size:10pt;'>Jumlah data: {len(df)}</p>", unsafe_allow_html=True)
                st.dataframe(df, use_container_width=True)

                # Tombol unduh Excel untuk seluruh data
                with st.spinner("Mempersiapkan file Excel..."):
                    excel_data = create_excel(df, "Seluruh Data")

                st.download_button(
                    label="Unduh Excel",
                    data=excel_data,
                    file_name="data_pengaduan_seluruh.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            else:
                df = get_monthly_summary(df, "date")

                if df.empty:
                    st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
                else:
                    # Aggregasi per jenis pengaduan
                    jenis = df.groupby(["tahun", "nama_bulan", "jenis_pengaduan"]).size().unstack(fill_value=0)
                    jenis = jenis.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="jumlah pengaduan per jenis")

                    # Aggregasi per status
                    status = df.groupby(["tahun", "nama_bulan", "status"]).size().unstack(fill_value=0)
                    status = status.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="status pelanggan")

                    # Aggregasi per wilayah
                    wilayah = df.groupby(["tahun", "nama_bulan", "wilayah"]).size().unstack(fill_value=0)
                    wilayah = wilayah.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="wilayah pelanggan")

                    # Gabungkan semua hasil
                    hasil = jenis.merge(status, on=["tahun", "nama_bulan"]) \
                                .merge(wilayah, on=["tahun", "nama_bulan"])

                    # Ganti nama kolom "nama_bulan" menjadi "bulan"
                    hasil = hasil.rename(columns={"nama_bulan": "bulan"})

                    # Ubah nilai dictionary jadi teks multiline
                    hasil["jumlah pengaduan per jenis"] = hasil["jumlah pengaduan per jenis"].apply(dict_to_multiline)
                    hasil["status pelanggan"] = hasil["status pelanggan"].apply(dict_to_multiline)
                    hasil["wilayah pelanggan"] = hasil["wilayah pelanggan"].apply(dict_to_multiline)

                    # Tampilkan hasil
                    st.dataframe(hasil)

                    # Tombol unduh Excel untuk data per bulan
                    with st.spinner("Mempersiapkan file Excel..."):
                        excel_data = create_excel(hasil, "Per Bulan")

                    st.download_button(
                        label="Unduh Excel",
                        data=excel_data,
                        file_name="data_pengaduan_per_bulan.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

    # Tab Pemutusan
    with tab3:
        # Load data
        df = load_data(f"""
            SELECT dp.kodepelanggan, dp.status, dp.wilayah, dr.jenis_realisasi, dw.date
            FROM fact_pemutusan fp
            LEFT JOIN dim_pelanggan dp ON fp.kodepelanggan = dp.kodepelanggan
            LEFT JOIN dim_waktu dw ON fp.id_waktu = dw.id_waktu
            LEFT JOIN dim_realisasi dr ON fp.id_realisasi = dr.id_realisasi
            WHERE dw.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """)

        st.subheader("Data Pemutusan Sambungan")
        mode = st.radio("Filter Tampilan", ["Seluruh Data", "Per Bulan"], horizontal=True, key="pemutusan")

        if df.empty:
            st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
        else:
            if mode == "Seluruh Data":
                st.markdown(f"<p style='font-size:10pt;'>Jumlah data: {len(df)}</p>", unsafe_allow_html=True)
                st.dataframe(df, use_container_width=True)

                # Tombol unduh Excel untuk seluruh data
                with st.spinner("Mempersiapkan file Excel..."):
                    excel_data = create_excel(df, "Seluruh Data")

                st.download_button(
                    label="Unduh Excel",
                    data=excel_data,
                    file_name="data_pemutusan_seluruh.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                df = get_monthly_summary(df, "date")

                if df.empty:
                    st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
                else:
                    # Aggregasi per jenis realisasi
                    realisasi = df.groupby(["tahun", "nama_bulan", "jenis_realisasi"]).size().unstack(fill_value=0)
                    realisasi = realisasi.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="jenis realisasi")

                    # Aggregasi per status pelanggan
                    status = df.groupby(["tahun", "nama_bulan", "status"]).size().unstack(fill_value=0)
                    status = status.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="status pelanggan")

                    # Aggregasi per wilayah pelanggan
                    wilayah = df.groupby(["tahun", "nama_bulan", "wilayah"]).size().unstack(fill_value=0)
                    wilayah = wilayah.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="wilayah pelanggan")

                    # Gabungkan hasil agregasi
                    hasil = realisasi.merge(status, on=["tahun", "nama_bulan"]) \
                                    .merge(wilayah, on=["tahun", "nama_bulan"])

                    # Ganti nama kolom "nama_bulan" menjadi "bulan"
                    hasil = hasil.rename(columns={"nama_bulan": "bulan"})

                    # Ubah isi dictionary jadi multiline string agar tampil rapi
                    hasil["jenis realisasi"] = hasil["jenis realisasi"].apply(dict_to_multiline)
                    hasil["status pelanggan"] = hasil["status pelanggan"].apply(dict_to_multiline)
                    hasil["wilayah pelanggan"] = hasil["wilayah pelanggan"].apply(dict_to_multiline)

                    # Tampilkan hasil
                    st.dataframe(hasil)

                    # Tombol unduh Excel untuk data per bulan
                    with st.spinner("Mempersiapkan file Excel..."):
                        excel_data = create_excel(hasil, "Per Bulan")

                    st.download_button(
                        label="Unduh Excel",
                        data=excel_data,
                        file_name="data_pemutusan_per_bulan.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

   # Tab Sambungan Baru
    with tab4:
        # Load data
        df = load_data(f"""
            SELECT fs.kodecpelanggan, fs.wilayah, dr.jenis_realisasi, fs.jumlah, dw.date
            FROM fact_sbbaru fs
            LEFT JOIN dim_realisasi dr ON fs.id_realisasi = dr.id_realisasi
            LEFT JOIN dim_waktu dw ON fs.id_waktu = dw.id_waktu
            WHERE dw.date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """)

        st.subheader("Data Sambungan Baru")
        mode = st.radio("Filter Tampilan", ["Seluruh Data", "Per Bulan"], horizontal=True, key="sbbaru")

        if df.empty:
            st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
        else:
            if mode == "Seluruh Data":
                st.markdown(f"<p style='font-size:10pt;'>Jumlah data: {len(df)}</p>", unsafe_allow_html=True)
                st.dataframe(df, use_container_width=True)

                # Tombol unduh Excel untuk seluruh data
                with st.spinner("Mempersiapkan file Excel..."):
                    excel_data = create_excel(df, "Seluruh Data")

                st.download_button(
                    label="Unduh Excel",
                    data=excel_data,
                    file_name="data_sbbaru_seluruh.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                df = get_monthly_summary(df, "date")

                if df.empty:
                    st.info("Data belum tersedia untuk rentang waktu yang dipilih.")
                else:
                    # Total jumlah sambungan baru per bulan
                    jumlah = df.groupby(["tahun", "nama_bulan"])["jumlah"].sum().reset_index(name="total biaya")

                    # Agregasi wilayah
                    wilayah = df.groupby(["tahun", "nama_bulan", "wilayah"]).size().unstack(fill_value=0)
                    wilayah = wilayah.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="wilayah")

                    # Agregasi jenis realisasi
                    realisasi = df.groupby(["tahun", "nama_bulan", "jenis_realisasi"]).size().unstack(fill_value=0)
                    realisasi = realisasi.reset_index().groupby(["tahun", "nama_bulan"]).apply(
                        lambda x: x.drop(columns=["tahun", "nama_bulan"]).sum().to_dict()
                    ).reset_index(name="jenis realisasi")

                    # Gabungkan semua
                    hasil = jumlah.merge(wilayah, on=["tahun", "nama_bulan"], how="left") \
                                .merge(realisasi, on=["tahun", "nama_bulan"], how="left")

                    # Ganti nama kolom "nama_bulan" menjadi "bulan"
                    hasil = hasil.rename(columns={"nama_bulan": "bulan"})

                    # Format dictionary menjadi multiline string
                    hasil["wilayah"] = hasil["wilayah"].apply(dict_to_multiline)
                    hasil["jenis realisasi"] = hasil["jenis realisasi"].apply(dict_to_multiline)

                    # Tampilkan hasil
                    st.dataframe(hasil)

                    # Tombol unduh Excel untuk data per bulan
                    with st.spinner("Mempersiapkan file Excel..."):
                        excel_data = create_excel(hasil, "Per Bulan")

                    st.download_button(
                        label="Unduh Excel",
                        data=excel_data,
                        file_name="data_sbbaru_per_bulan.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
