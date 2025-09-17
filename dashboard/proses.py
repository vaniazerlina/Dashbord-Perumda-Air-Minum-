import sys
import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import time
from datetime import datetime

# Path modul ETL
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ETL.extract import extract_data
from ETL.transform import main as transform_main
from ETL.load import main as load_main

# Koneksi ke database PostgreSQL
DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
engine = create_engine(DATABASE_URL)

import streamlit as st

def show_login():
    st.markdown("<h1 style='text-align: center;'>Login</h1>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 3, 1])  
    with col2:
        username = st.text_input("Username", max_chars=20)  
        password = st.text_input("Password", type="password", max_chars=20)  

        if st.button("Login"):
            try:
                with engine.connect() as conn:
                    query = text("""
                        SELECT * FROM admin
                        WHERE username = :username AND password = crypt(:password, password)
                    """)
                    result = conn.execute(query, {"username": username, "password": password}).fetchone()
            except Exception as e:
                st.error(f"❌ Gagal koneksi ke database: {e}")
                return
            
            if result:
                st.session_state["logged_in"] = True
                st.success("✅ Login berhasil!")
                st.rerun()
            else:
                st.error("❌ Username atau Password salah. Coba lagi.")

def get_etl_history(limit=20, offset=0):
    query = text("""
        SELECT id_riwayat, timestamp, start_date, end_date 
        FROM etl_history 
        ORDER BY timestamp DESC 
        LIMIT :limit OFFSET :offset
    """)
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params={"limit": limit, "offset": offset})
    except Exception:
        return pd.DataFrame(columns=["id_riwayat", "timestamp", "start_date", "end_date"])

def check_data_availability(start_date, end_date):
    dataframes = extract_data(start_date, end_date)
    
    tables_to_check = ["brek", "trx", "pemutusan", "pengaduan", "sbbaru"]
    
    for table in tables_to_check:
        if table in dataframes and not dataframes[table].empty:
            return True  
    
    return False 

def check_existing_etl(start_date, end_date):
    query = text("""
        SELECT 1 FROM etl_history
        WHERE 
            (start_date <= :start_date AND end_date >= :start_date)
            OR 
            (start_date <= :end_date AND end_date >= :end_date)
        LIMIT 1
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {
            "start_date": start_date,
            "end_date": end_date
        }).fetchone()
    
    return result is not None

def log_etl_history(start_date, end_date):
    query = text("""
        INSERT INTO etl_history (timestamp, start_date, end_date, status) 
        VALUES (NOW(), :start_date, :end_date, 'Selesai')
    """)
    try:
        with engine.connect() as conn:
            conn.execute(query, {"start_date": start_date, "end_date": end_date})
            conn.commit()
    except Exception as e:
        st.warning(f"⚠️ Gagal mencatat riwayat ETL: {e}")

def run_etl(start_date, end_date):
    if st.session_state.get("etl_running", False):
        st.warning("⚠️ Proses ETL sedang berjalan. Harap tunggu hingga selesai.")
        return

    st.session_state["etl_running"] = True
    try:
        with st.spinner("🔄 Extracting data..."):
            try:
                dataframes = extract_data(start_date, end_date)
                print(f"Extract selesai: { {k: len(v) for k, v in dataframes.items()} }")
            except Exception as e:
                print(f"ERROR di Extract: {e}")
                st.error(f"❌ Gagal dalam tahap Extract: {e}")
                st.session_state["etl_running"] = False
                return

        with st.spinner("🔄 Transforming data..."):
            try:
                transformed_data = transform_main(dataframes, engine)
                print(f"Transform selesai: { {k: len(v) for k, v in transformed_data.items()} }")
            except Exception as e:
                print(f"ERROR di Transformasi: {e}")
                st.error(f"❌ Gagal dalam tahap Transformasi: {e}")
                return

        if not transformed_data:
            st.error("❌ Transformasi selesai, tetapi tidak ada data yang bisa dimuat ke database.")
            return

        with st.spinner("🔄 Loading data to PostgreSQL..."):
            try:
                load_main(transformed_data)
                print("Load selesai!")
            except Exception as e:
                print(f"ERROR di Loading: {e}")
                st.error(f"❌ Gagal dalam tahap Loading: {e}")
                return

        log_etl_history(start_date, end_date)
        st.success("✅ Proses ETL selesai! Data telah berhasil dimuat ke database.")
        time.sleep(2)
        st.cache_data.clear()
    except Exception as e:
        st.error(f"❌ Terjadi kesalahan dalam proses ETL: {e}")
        print(f"ERROR: {e}")
    finally:
        st.session_state["etl_running"] = False  
        st.rerun()  

def delete_existing_data(start_date, end_date):
    fact_tables = ["fact_transaksi", "fact_sbbaru", "fact_pemutusan", "fact_pengaduan"]

    try:
        with engine.connect() as conn:
            for table in fact_tables:
                query = text(f"""
                    DELETE FROM {table} 
                    WHERE id_waktu IN (
                        SELECT id_waktu FROM dim_waktu 
                        WHERE date >= :start_date AND date <= :end_date
                    )
                """)
                conn.execute(query, {"start_date": start_date, "end_date": end_date})
            conn.commit()
    except Exception as e:
        st.error(f"❌ Gagal menghapus data lama dari database: {e}")

def show_proses_etl():
    # Pastikan pengguna sudah login
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        show_login()  # Tampilkan form login jika belum login
        return  # Hentikan eksekusi selanjutnya jika belum login
    st.title("Proses Data")

    tabs = st.tabs(["Proses ETL", "Riwayat ETL"])

    with tabs[0]:
        st.write("Pilih rentang waktu dan jalankan proses ETL.")

        col1, col2 = st.columns(2)
        with col1:
            selected_start = st.date_input("Tanggal Mulai")
        with col2:
            selected_end = st.date_input("Tanggal Akhir")
        
        # Konversi format date input menjadi string (YYYY-MM-DD)
        start_date_str = selected_start.strftime("%Y-%m-%d")
        end_date_str = selected_end.strftime("%Y-%m-%d")
        
        # Simpan state untuk menyembunyikan button setelah ETL selesai
        if "etl_completed" not in st.session_state:
            st.session_state["etl_completed"] = False
        if "delete_confirmation" not in st.session_state:
            st.session_state["delete_confirmation"] = False

        # Hanya tampilkan tombol ETL jika belum selesai
        if not st.session_state["etl_completed"]:
            if st.button("Mulai ETL"):
                # Periksa apakah ada data dalam rentang tanggal yang dipilih
                if not check_data_availability(start_date_str, end_date_str):
                    st.error(f"❌ Tidak ada data dari {start_date_str} hingga {end_date_str}. Silakan pilih tanggal lain.")
                    return  # Hentikan eksekusi jika tidak ada data
                
                if check_existing_etl(start_date_str, end_date_str):
                    st.warning(f"⚠️ Data untuk rentang {start_date_str} - {end_date_str} sudah pernah diproses.")
                    st.session_state["delete_confirmation"] = True  # Simpan state untuk menampilkan tombol konfirmasi
                    st.rerun()
                else:
                    run_etl(start_date_str, end_date_str)
                    st.session_state["etl_completed"] = True  # Set ETL selesai
                    st.rerun()  # Refresh untuk menyembunyikan button

        # Tampilkan tombol konfirmasi 
        if st.session_state["delete_confirmation"]:
            st.warning("Apakah Anda ingin menghapus data lama dan memproses ulang?")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Iya, proses ulang"):
                    # Reset dulu sebelum proses jalan dan sebelum rerun
                    st.session_state["etl_completed"] = False
                    st.session_state["delete_confirmation"] = False

                    with st.status("Menghapus data lama...", expanded=True) as status:
                        delete_existing_data(start_date_str, end_date_str)
                        status.update(label="Data lama terhapus. Memulai ulang ETL...", state="running")
                        run_etl(start_date_str, end_date_str)
                        status.update(label="Proses selesai!", state="complete")

                    st.session_state["etl_completed"] = True
                    st.rerun()

            with col2:
                if st.button("❌ Tidak, batal"):
                    st.warning("⚠️ Proses ETL dibatalkan.")
                    st.session_state["delete_confirmation"] = False  # Reset state
                    st.rerun()

    with tabs[1]:
        LIMIT = 20
        if "etl_page" not in st.session_state:
            st.session_state["etl_page"] = 0  # offset dalam satuan halaman

        offset = st.session_state["etl_page"] * LIMIT
        etl_history = get_etl_history(limit=LIMIT, offset=offset)

        if etl_history.empty and st.session_state["etl_page"] > 0:
            st.session_state["etl_page"] -= 1  # kembali ke halaman sebelumnya jika tidak ada data
            st.rerun()

        if etl_history.empty:
            st.write("❌ Belum ada riwayat ETL yang tersedia.")
        else:
            etl_history = etl_history.drop(columns=["id_riwayat"]) 
            etl_history = etl_history.rename(columns={
                "timestamp": "Waktu Proses",
                "start_date": "Tanggal Mulai",
                "end_date": "Tanggal Akhir"
            })
            etl_history = etl_history.reset_index(drop=True)
            etl_history.index = range(1, len(etl_history) + 1)
            st.dataframe(etl_history, use_container_width=True)

            col1, col2, col3 = st.columns([1, 1, 4])
            with col1:
                if st.button("Sebelumnya") and st.session_state["etl_page"] > 0:
                    st.session_state["etl_page"] -= 1
                    st.rerun()
            with col2:
                if st.button("Selanjutnya") and not etl_history.empty:
                    st.session_state["etl_page"] += 1
                    st.rerun()
            with col3:
                st.caption(f"Halaman: {st.session_state['etl_page'] + 1}")
