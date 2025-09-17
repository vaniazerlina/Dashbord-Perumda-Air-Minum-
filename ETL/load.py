import pandas as pd
from sqlalchemy import create_engine

def insert_data(df, table_name, conn):
    try:
        # Menyisipkan data ke dalam tabel PostgreSQL 
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data berhasil dimasukkan ke {table_name}.")
    except Exception as e:
        print(f"Terjadi kesalahan saat memasukkan data ke {table_name}: {e}")

def main(transformed_data):
    DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
    engine_test_perumda = create_engine(DATABASE_URL)
    conn = engine_test_perumda.connect()
    
    # Daftar tabel yang sudah di-insert saat transformasi
    exclude_tables = ["dim_pelanggan", "dim_goltarif", "dim_waktu"]

    table_mappings = {
        "dim_jenispengaduan": ("id_jenispengaduan", {"id_jenispengaduan": "id_jenispengaduan", "jenis_pengaduan": "jenis_pengaduan"}),
        "dim_realisasi": ("id_realisasi", {"id_realisasi": "id_realisasi", "jenis_realisasi": "jenis_realisasi"}),
        "fact_transaksi": ("id_transaksi", {"kodepelanggan": "kodepelanggan", "jumlahbayar": "jumlahbayar", "denda": "denda", "kodegoltarif": "kodegoltarif", "tagihan": "tagihan", "pemakaian": "pemakaian", "id_waktu": "id_waktu", "id_transaksi": "id_transaksi"}),
        "fact_pemutusan": ("id_pemutusan", {"kodepelanggan": "kodepelanggan", "id_realisasi": "id_realisasi", "id_waktu": "id_waktu", "id_pemutusan": "id_pemutusan"}),
        "fact_pengaduan": ("id_pengaduan", {"idpelanggan": "idpelanggan", "id_jenisPengaduan": "id_jenisPengaduan", "id_waktu": "id_waktu", "id_pengaduan": "id_pengaduan"}),
        "fact_sbbaru": ("kodepelanggan", {"kodepelanggan": "kodepelanggan", "id_realisasi": "id_realisasi", "wilayah": "wilayah", "id_waktu": "id_waktu", "jumlah":"jumlah"})
    }
    
    for table_name, (primary_key, column_mapping) in table_mappings.items():
        # Skip tabel yang sudah di-insert di fungsi transformasi
        if table_name in exclude_tables:
            print(f"Skipping insert untuk {table_name}, sudah dilakukan di transformasi.")
            continue

        if table_name in transformed_data:
            data = transformed_data[table_name]

            if isinstance(data, pd.DataFrame):
                df = data.rename(columns=column_mapping)
                insert_data(df, table_name, conn)
            elif isinstance(data, tuple):
                print(f"Data untuk {table_name} adalah tuple. Diperlukan DataFrame untuk pengolahan.")
            else:
                print(f"Data untuk {table_name} bukan DataFrame atau tuple, jenis data: {type(data)}")
        else:
            print(f"Data untuk {table_name} tidak ditemukan dalam hasil transformasi.")

    conn.close()
