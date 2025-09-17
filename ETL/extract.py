import pandas as pd
from sqlalchemy import create_engine

# String koneksi
DATABASE_URL = "postgresql://postgres:admin@localhost:5433/perumda"
engine = create_engine(DATABASE_URL)

# Daftar tabel
TABLES = ["pelanggan", "goltarif", "brek", "trx", "pemutusan", "pengaduan", "sbbaru"]

def extract_data(start_date, end_date):
    """Mengambil data dari database berdasarkan rentang tanggal yang dipilih."""
    dataframes = {}

    for table in TABLES:
        if table in ["brek", "trx"]:
            query = f"""
                SELECT * FROM {table}
                WHERE (tahun || '-' || LPAD(bulan::TEXT, 2, '0') || '-01')::date 
                BETWEEN '{start_date}' AND '{end_date}'
            """
        elif table == "pemutusan":
            query = f"""
                SELECT * FROM {table}
                WHERE tglstk::date BETWEEN '{start_date}' AND '{end_date}';
            """
        elif table == "pengaduan":
            query = f"""
                SELECT * FROM {table}
                WHERE tgl::timestamp::date BETWEEN '{start_date}' AND '{end_date}';
            """
        elif table == "sbbaru":
            query = f"""
                SELECT * FROM {table}
                WHERE tglreg::date BETWEEN '{start_date}' AND '{end_date}';
            """
        else:
            query = f"SELECT * FROM {table};"

        try:
            dataframes[table] = pd.read_sql_query(query, engine)
        except Exception as e:
            print(f"Gagal mengambil data dari {table}: {e}")

    return dataframes
