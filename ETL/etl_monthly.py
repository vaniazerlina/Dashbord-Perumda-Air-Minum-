from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
from extract import extract_data
from transform import main as transform_main
from load import main as load_main

# Koneksi ke database
DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
engine = create_engine(DATABASE_URL)

def get_all_etl_ranges():
    query = "SELECT start_date, end_date FROM etl_history"
    try:
        df = pd.read_sql(query, engine)
        # Konversi ke date
        df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
        df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
        return df
    except:
        return pd.DataFrame(columns=["start_date", "end_date"])

def delete_partial_etl(start_date, end_date):
    fact_tables = ["fact_transaksi", "fact_sbbaru", "fact_pemutusan", "fact_pengaduan"]
    with engine.begin() as conn:  
        for table in fact_tables:
            query = text(f"""
                DELETE FROM {table}
                WHERE id_waktu IN (
                    SELECT id_waktu FROM dim_waktu
                    WHERE date >= :start_date AND date <= :end_date
                )
            """)
            conn.execute(query, {"start_date": start_date, "end_date": end_date})

def get_unprocessed_months():
    etl_ranges = get_all_etl_ranges()
    today = datetime.today().date()  # Ubah ke date
    first_month = datetime(2021, 1, 1).date()
    months = []

    while first_month < today.replace(day=1):
        start_date = first_month

        # Jika bulan ini adalah bulan berjalan
        if first_month.year == today.year and first_month.month == today.month:
            end_date = today
        else:
            # Ambil akhir bulan
            next_month = (first_month.replace(day=28) + timedelta(days=4)).replace(day=1)
            end_date = (next_month - timedelta(days=1))

        # Cek apakah bulan ini sudah diproses penuh
        is_fully_processed = any(
            row["start_date"] <= start_date and row["end_date"] >= end_date
            for _, row in etl_ranges.iterrows()
        )

        if not is_fully_processed:
            months.append((start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))

        # Lanjut ke bulan berikutnya
        first_month = (first_month.replace(day=28) + timedelta(days=4)).replace(day=1)

    return months

def log_etl_history(start_date, end_date):
    query = text("""
        INSERT INTO etl_history (timestamp, start_date, end_date, status)
        VALUES (NOW(), :start_date, :end_date, 'Selesai')
    """)
    with engine.begin() as conn:
        conn.execute(query, {"start_date": start_date, "end_date": end_date})

def run_etl_for_range(start_date, end_date):
    print(f"\n[ETL] Mulai proses ETL untuk: {start_date} s.d. {end_date}")

    # Hapus data partial sebelum extract supaya data tidak duplikat
    delete_partial_etl(start_date, end_date)

    dataframes = extract_data(start_date, end_date)
    if not any(not df.empty for df in dataframes.values()):
        print(f"Tidak ada data untuk rentang {start_date} - {end_date}. Lewati.")
        return

    try:
        transformed_data = transform_main(dataframes, engine)
        if not transformed_data:
            print(f"Transformasi selesai, tapi tidak ada data untuk dimuat.")
            return

        load_main(transformed_data)
        log_etl_history(start_date, end_date)
        print(f"ETL selesai dan dicatat untuk: {start_date} - {end_date}")
    except Exception as e:
        print(f"Gagal ETL untuk {start_date} - {end_date}: {e}")

def run_monthly_etl():
    unprocessed_months = get_unprocessed_months()
    if not unprocessed_months:
        print("Semua bulan sudah diproses. Tidak ada ETL baru dijalankan.")
        return

    print(f"Menjalankan ETL untuk {len(unprocessed_months)} bulan yang belum diproses...\n")
    for start_date, end_date in unprocessed_months:
        run_etl_for_range(start_date, end_date)

if __name__ == "__main__":
    run_monthly_etl()
