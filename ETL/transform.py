import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

DATABASE_URL = "postgresql://postgres:admin@localhost:5433/test_perumda"
engine_test_perumda  = create_engine(DATABASE_URL)

def transform_pelanggan(dataframes, engine_test_perumda):
    print("Memulai proses transformasi pelanggan...")

    df_pelanggan = dataframes['pelanggan']
    print("Data pelanggan sebelum transformasi:")
    print(df_pelanggan.head())

    print("Tipe data df_pelanggan:")
    print(df_pelanggan.dtypes)

    # Menghapus duplikat
    df_pelanggan.drop_duplicates(inplace=True)
    print("Data pelanggan setelah menghapus duplikat:")
    print(df_pelanggan.head())

    # Mengecek data pelanggan yang sudah ada di database
    existing_ids = pd.read_sql("SELECT kodepelanggan, wilayah, status FROM dim_pelanggan", engine_test_perumda)
    print("ID pelanggan yang sudah ada di database:")
    print(existing_ids.head())

    print("Tipe data existing_ids:")
    print(existing_ids.dtypes)

    # Pastikan tipe data 
    df_pelanggan['kodepelanggan'] = df_pelanggan['kodepelanggan'].astype(str)
    existing_ids['kodepelanggan'] = existing_ids['kodepelanggan'].astype(str)

    # Menggabungkan data berdasarkan kodepelanggan dan wilayah
    merged_df = df_pelanggan.merge(existing_ids, on=['kodepelanggan', 'wilayah'], how='left', suffixes=('_new', '_existing'))

    # Debug
    print("Kolom dalam merged_df:", merged_df.columns)

    # Jika kolom tidak ada, munculkan error dengan informasi tambahan
    if 'status_new' not in merged_df.columns or 'status_existing' not in merged_df.columns:
        raise ValueError(f"Kolom status_new atau status_existing tidak ditemukan setelah merge. Kolom yang tersedia: {merged_df.columns}")

    # Menyaring data yang perlu diupdate atau yang baru
    df_pelanggan_to_update = merged_df[merged_df['status_new'] != merged_df['status_existing']]
    df_pelanggan_to_insert = merged_df[merged_df['status_existing'].isna()]

    # Mengambil hanya kolom yang dibutuhkan untuk update dan insert
    df_pelanggan_to_update = df_pelanggan_to_update[['kodepelanggan', 'status_new', 'wilayah']]
    df_pelanggan_to_update.rename(columns={'status_new': 'status'}, inplace=True)

    df_pelanggan_to_insert = df_pelanggan_to_insert[['kodepelanggan', 'status_new', 'wilayah']]
    df_pelanggan_to_insert.rename(columns={'status_new': 'status'}, inplace=True)

    print("Data pelanggan yang perlu diupdate:")
    print(df_pelanggan_to_update.head())
    print("Data pelanggan yang perlu diinsert:")
    print(df_pelanggan_to_insert.head())

    # Menyimpan data pelanggan baru atau update ke database jika ada
    if not df_pelanggan_to_insert.empty or not df_pelanggan_to_update.empty:
        print("Menyimpan data pelanggan ke database...")

        try:
            with engine_test_perumda.connect() as connection:  
                # Insert data pelanggan baru yang belum ada
                if not df_pelanggan_to_insert.empty:
                    df_pelanggan_to_insert.to_sql('dim_pelanggan', connection, if_exists='append', index=False)

                # Update status pelanggan yang berubah
                if not df_pelanggan_to_update.empty:
                    for _, row in df_pelanggan_to_update.iterrows():
                        query = """
                            UPDATE dim_pelanggan
                            SET status = :status
                            WHERE kodepelanggan = :kodepelanggan
                            AND wilayah = :wilayah
                        """
                        connection.execute(text(query), {
                            "status": row['status'],
                            "kodepelanggan": row['kodepelanggan'],
                            "wilayah": row['wilayah']
                        })

                connection.commit()  
            print("Data pelanggan berhasil disimpan/diupdate ke database.")
        except Exception as e:
            print(f"⚠️ Gagal menyimpan data pelanggan ke database: {str(e)}")

    return df_pelanggan_to_update, df_pelanggan_to_insert

def transform_goltarif(dataframes, engine_test_perumda):
    print("Memulai proses transformasi golongan tarif...")

    df_goltarif = dataframes['goltarif'].copy()
    print("Data golongan tarif sebelum transformasi:")
    print(df_goltarif.head())

    # Menghapus duplikat
    df_goltarif.drop_duplicates(inplace=True)
    print("Data golongan tarif setelah menghapus duplikat:")
    print(df_goltarif.head())

    # Mengecek data golongan tarif yang sudah ada di database
    existing_goltarif = pd.read_sql("SELECT kodegoltarif, namagoltarif FROM dim_goltarif", engine_test_perumda)
    print("Data golongan tarif yang sudah ada di database:")
    print(existing_goltarif.head())

    # Pastikan tipe data sama sebelum merge
    df_goltarif['kodegoltarif'] = df_goltarif['kodegoltarif'].astype(str)
    existing_goltarif['kodegoltarif'] = existing_goltarif['kodegoltarif'].astype(str)

    # Menggabungkan data baru dengan data yang sudah ada
    merged_df = df_goltarif.merge(existing_goltarif, on='kodegoltarif', how='left', suffixes=('_new', '_existing'))

    # Menyaring data untuk update dan insert
    df_goltarif_to_update = merged_df[merged_df['namagoltarif_new'] != merged_df['namagoltarif_existing']]
    df_goltarif_to_insert = merged_df[merged_df['namagoltarif_existing'].isna()]

    # Mengambil kolom yang dibutuhkan untuk update dan insert
    df_goltarif_to_update = df_goltarif_to_update[['kodegoltarif', 'namagoltarif_new']]
    df_goltarif_to_update.rename(columns={'namagoltarif_new': 'namagoltarif'}, inplace=True)

    df_goltarif_to_insert = df_goltarif_to_insert[['kodegoltarif', 'namagoltarif_new']]
    df_goltarif_to_insert.rename(columns={'namagoltarif_new': 'namagoltarif'}, inplace=True)

    print("Data golongan tarif yang perlu diupdate:")
    print(df_goltarif_to_update.head())
    print("Data golongan tarif yang perlu diinsert:")
    print(df_goltarif_to_insert.head())

    # Menyimpan data baru atau update ke database jika ada
    if not df_goltarif_to_insert.empty or not df_goltarif_to_update.empty:
        print("Menyimpan data golongan tarif ke database...")

        try:
            with engine_test_perumda.connect() as connection:
                # Insert data baru
                if not df_goltarif_to_insert.empty:
                    df_goltarif_to_insert.to_sql('dim_goltarif', connection, if_exists='append', index=False)

                # Update data yang berubah
                if not df_goltarif_to_update.empty:
                    for _, row in df_goltarif_to_update.iterrows():
                        query = """
                            UPDATE dim_goltarif
                            SET namagoltarif = :namagoltarif
                            WHERE kodegoltarif = :kodegoltarif
                        """
                        connection.execute(text(query), {
                            "namagoltarif": row['namagoltarif'],
                            "kodegoltarif": row['kodegoltarif']
                        })

                connection.commit()  
            print("Data golongan tarif berhasil disimpan/diupdate ke database.")
        except Exception as e:
            print(f"⚠️ Gagal menyimpan data golongan tarif ke database: {str(e)}")

    return df_goltarif_to_update, df_goltarif_to_insert

def transform_waktu(engine_test_perumda):
    print("Memulai proses transformasi data waktu...")

    start_date, end_date = "2020-01-01", pd.to_datetime('today').strftime('%Y-%m-%d')
    date_range = pd.date_range(start=start_date, end=end_date)
    
    df_waktu = pd.DataFrame({
        "id_waktu": range(1, len(date_range) + 1),
        "date": date_range,
        "day": date_range.day,
        "month": date_range.month,
        "year": date_range.year
    })
    
    df_waktu[['id_waktu']] = df_waktu[['id_waktu']].astype(str)
    df_waktu['date'] = pd.to_datetime(df_waktu['date']).dt.date
    df_waktu.drop_duplicates(inplace=True)

    print("Data waktu setelah dibuat:")
    print(df_waktu.head())

    # Membaca data dari dim_waktu, jika kosong maka buat dataframe kosong
    try:
        existing_waktu = pd.read_sql("SELECT id_waktu, date, day, month, year FROM dim_waktu", engine_test_perumda)
        print("Data waktu yang sudah ada di database:")
        print(existing_waktu.head())
    except Exception as e:
        existing_waktu = pd.DataFrame(columns=['id_waktu', 'date', 'day', 'month', 'year'])
        print("Tabel dim_waktu masih kosong:", str(e))

    # Jika existing_waktu kosong, langsung gunakan df_waktu baru
    if existing_waktu.empty:
        print("Menggunakan data baru karena tabel dim_waktu kosong.")
        try:
            df_waktu.to_sql('dim_waktu', engine_test_perumda, if_exists='append', index=False, method='multi')
            print("✅ Data waktu berhasil disimpan ke database.")
        except Exception as e:
            print(f"⚠️ Gagal menyimpan data waktu ke database: {e}")
        return df_waktu

    # Memastikan kolom day, month, year selalu ada
    if "day" not in existing_waktu.columns:
        existing_waktu["day"] = existing_waktu["date"].dt.day
    if "month" not in existing_waktu.columns:
        existing_waktu["month"] = existing_waktu["date"].dt.month
    if "year" not in existing_waktu.columns:
        existing_waktu["year"] = existing_waktu["date"].dt.year

    print("Data waktu setelah memastikan kolom day, month, year:")
    print(existing_waktu.head())

    # Filter tanggal yang belum ada
    df_waktu_to_process = df_waktu[~df_waktu['date'].isin(existing_waktu['date'])]

    if df_waktu_to_process.empty:
        print("Tidak ada data baru yang perlu diproses.")
        return existing_waktu  # Gunakan data yang sudah ada di database

    print("Data waktu yang akan diproses:")
    print(df_waktu_to_process.head())
    
    # Tambahkan penyimpanan ke database
    try:
        df_waktu_to_process.to_sql('dim_waktu', engine_test_perumda, if_exists='append', index=False, method='multi')
        print("Data waktu baru berhasil disimpan ke database.")
    except Exception as e:
        print("Gagal menyimpan data waktu ke database:", str(e))

    # Gabungkan data lama dengan data baru
    df_waktu = pd.concat([existing_waktu, df_waktu_to_process], ignore_index=True)

    # Pastikan semua kolom tetap ada
    df_waktu = df_waktu.fillna(0)
    df_waktu[['day', 'month', 'year']] = df_waktu[['day', 'month', 'year']].astype(int)

    print("Data waktu setelah digabungkan dengan data yang sudah ada:")
    print(df_waktu.head())

    return df_waktu

def transform_transaksi(dataframes, df_pelanggan, df_waktu, engine_test_perumda):
    print("Memulai proses transformasi data transaksi...")

    df_brek, df_trx = dataframes['brek'], dataframes['trx']
    
    # Menghapus duplikat sebelum merge
    print("Menghapus duplikat di df_brek dan df_trx...")
    df_brek.drop_duplicates(subset=['kodepelanggan', 'tahun', 'bulan'], inplace=True)
    df_trx.drop_duplicates(subset=['kodepelanggan', 'tahun', 'bulan'], inplace=True)

    print("Data df_brek setelah menghapus duplikat:")
    print(df_brek.head())

    print("Data df_trx setelah menghapus duplikat:")
    print(df_trx.head())

    # Merging df_brek dan df_trx
    print("Merging df_brek dan df_trx...")
    df_transaksi = df_brek.merge(df_trx, on=['kodepelanggan', 'tahun', 'bulan'], how='outer')

    print("Data df_transaksi setelah merge:")
    print(df_transaksi.head())

    # Menambahkan kolom periode
    print("Menambahkan kolom periode...")
    df_transaksi['periode'] = pd.to_datetime(df_transaksi['tahun'].astype(str) + '-' + df_transaksi['bulan'].astype(str) + '-01').dt.date

    print("Data df_transaksi setelah menambahkan kolom periode:")
    print(df_transaksi.head())

    # Pastikan df_waktu['date'] dalam format datetime.date
    print("Mengubah kolom 'date' di df_waktu menjadi datetime.date...")
    df_waktu['date'] = pd.to_datetime(df_waktu['date']).dt.date  

    print("Data df_waktu setelah format perubahan:")
    print(df_waktu.head())

    # Merge dengan dim_waktu
    print("Merging df_transaksi dengan df_waktu...")
    df_transaksi = df_transaksi.merge(df_waktu, left_on='periode', right_on='date', how='left')

    print("Data df_transaksi setelah merge dengan df_waktu:")
    print(df_transaksi.head())

    # Daftar kolom yang ingin disimpan
    columns_to_keep = ['kodepelanggan', 'jumlahbayar', 'denda', 'kodegoltarif', 'tagihan', 'pemakaian', 'id_waktu']
    df_transaksi = df_transaksi[columns_to_keep]

    print("Data df_transaksi setelah memilih kolom yang akan disimpan:")
    print(df_transaksi.head())

    # Menangani nilai null pada kolom tagihan
    print("Menangani nilai null di kolom tagihan...")
    df_transaksi['tagihan'] = df_transaksi.apply(lambda row: row['jumlahbayar'] if pd.isnull(row['tagihan']) and row['denda'] == 0 else (row['jumlahbayar'] - row['denda']) if pd.isnull(row['tagihan']) else row['tagihan'], axis=1)
    df_transaksi.dropna(subset=['tagihan'], inplace=True)
    df_transaksi.fillna({'jumlahbayar': 0, 'denda': 0}, inplace=True)

    print("Data df_transaksi setelah menangani nilai null di kolom tagihan:")
    print(df_transaksi.head())

    # Mengecek data yang sudah ada di database
    print("Mengambil data transaksi yang sudah ada di database...")
    existing_transaksi = pd.read_sql("SELECT kodepelanggan, id_waktu FROM fact_transaksi", engine_test_perumda)

    print("Data existing_transaksi dari database:")
    print(existing_transaksi.head())

    # Menggunakan set_index untuk memeriksa transaksi yang sudah ada
    df_transaksi_to_process = df_transaksi[~df_transaksi.set_index(['kodepelanggan', 'id_waktu']).index.isin(existing_transaksi.set_index(['kodepelanggan', 'id_waktu']).index)]

    print("Data yang belum ada di database (df_transaksi_to_process):")
    print(df_transaksi_to_process.head())

    # Kembalikan transaksi yang belum ada di database
    return df_transaksi_to_process

def transform_pengaduan(dataframes, df_waktu, engine_test_perumda):
    print("Memulai proses transformasi data pengaduan...")

    df_pengaduan = dataframes['pengaduan'].copy()  
    
    # Mengganti nilai None atau string kosong pada kolom jnspengaduan dengan 'lainnya'
    print("Mengganti nilai None atau string kosong pada kolom jnspengaduan dengan 'lainnya'...")
    df_pengaduan['jnspengaduan'] = df_pengaduan['jnspengaduan'].replace({None: 'lainnya', '': 'lainnya'}).str.lower()

    print("Data df_pengaduan setelah mengganti nilai kosong:")
    print(df_pengaduan.head())

    # Menghapus baris dengan nilai NaN di kolom 'idpelanggan' dan 'tgl'
    print("Menghapus baris dengan NaN di kolom 'idpelanggan' dan 'tgl'...")
    df_pengaduan.dropna(subset=['idpelanggan', 'tgl'], inplace=True)

    print("Data df_pengaduan setelah menghapus NaN:")
    print(df_pengaduan.head())

    # Menghapus duplikat sebelum merge
    print("Menghapus duplikat pada df_pengaduan...")
    df_pengaduan.drop_duplicates(subset=['idpelanggan', 'jnspengaduan', 'tgl'], inplace=True)

    print("Data df_pengaduan setelah menghapus duplikat:")
    print(df_pengaduan.head())

    # Mengambil daftar jenis pengaduan yang sudah ada di database
    print("Mengambil daftar jenis pengaduan yang sudah ada di database...")
    existing_jenis = pd.read_sql("SELECT id_jenispengaduan, jenis_pengaduan FROM dim_jenispengaduan", engine_test_perumda)

    print("Data existing_jenis dari database:")
    print(existing_jenis.head())

    # Membentuk DataFrame jenis pengaduan unik dari data baru
    print("Membentuk DataFrame jenis pengaduan unik...")
    df_jenisPengaduan = pd.DataFrame({'jenis_pengaduan': df_pengaduan['jnspengaduan'].unique()}).drop_duplicates()

    print("Data df_jenisPengaduan unik:")
    print(df_jenisPengaduan.head())

    # Menggabungkan dengan yang sudah ada di database untuk mendapatkan ID jika ada
    print("Menggabungkan df_jenisPengaduan dengan existing_jenis untuk mendapatkan ID...")
    df_jenisPengaduan = df_jenisPengaduan.merge(existing_jenis, on="jenis_pengaduan", how="left")

    print("Data df_jenisPengaduan setelah merge dengan existing_jenis:")
    print(df_jenisPengaduan.head())

    # Menentukan ID terakhir dari database
    print("Menentukan ID terakhir dari database...")
    last_id_result = pd.read_sql("SELECT MAX(id_jenispengaduan) AS last_id FROM dim_jenispengaduan", engine_test_perumda)
    last_id = int(last_id_result['last_id'].fillna(0).iloc[0])  # Tangani NULL agar tidak menyebabkan error

    print(f"ID terakhir di database: {last_id}")

    # Pastikan id_jenispengaduan di df_jenisPengaduan berupa integer
    df_jenisPengaduan['id_jenispengaduan'] = pd.to_numeric(
        df_jenisPengaduan['id_jenispengaduan'], errors='coerce'
    ).fillna(0).astype(int)

    # Identifikasi entri baru yang belum memiliki ID
    new_entries = df_jenisPengaduan['id_jenispengaduan'] == 0

    # Menambahkan ID baru dengan range integer
    df_jenisPengaduan.loc[new_entries, 'id_jenispengaduan'] = range(last_id + 1, last_id + 1 + new_entries.sum())

    print("Data jenis pengaduan yang akan diproses:")
    print(df_jenisPengaduan[new_entries].head())

    # **Simpan jenis pengaduan baru ke database**
    if new_entries.any():
        print("Menyimpan data jenis pengaduan baru ke database...")
        try:
            with engine_test_perumda.begin() as connection:
                df_jenisPengaduan[new_entries].to_sql('dim_jenispengaduan', connection, if_exists='append', index=False)
            print("Data jenis pengaduan baru berhasil disimpan ke database.")
        except Exception as e:
            print(f"Gagal menyimpan data jenis pengaduan ke database: {str(e)}")

    print("Data df_jenisPengaduan setelah menentukan ID baru:")
    print(df_jenisPengaduan.head())

    # Merge dengan pengaduan agar id_jenispengaduan masuk ke df_pengaduan
    print("Merging df_pengaduan dengan df_jenisPengaduan untuk mendapatkan id_jenispengaduan...")
    df_pengaduan = df_pengaduan.merge(df_jenisPengaduan, left_on='jnspengaduan', right_on='jenis_pengaduan', how='left')

    print("Data df_pengaduan setelah merge dengan df_jenisPengaduan:")
    print(df_pengaduan.head())

    # Menghapus kolom yang tidak diperlukan
    df_pengaduan.drop(columns=['jenis_pengaduan', 'jnspengaduan'], inplace=True)

    # Mengubah kolom 'tgl' menjadi format date
    print("Mengubah kolom 'tgl' menjadi format date...")
    df_pengaduan['tgl'] = pd.to_datetime(df_pengaduan['tgl']).dt.date

    print("Data df_pengaduan setelah format perubahan kolom 'tgl':")
    print(df_pengaduan.head())

    # Mengubah kolom 'date' di df_waktu menjadi tipe date jika belum
    print("Mengubah kolom 'date' di df_waktu menjadi datetime.date...")
    df_waktu['date'] = pd.to_datetime(df_waktu['date']).dt.date

    print("Data df_waktu setelah format perubahan:")
    print(df_waktu.head())

    # Merge df_pengaduan dengan df_waktu berdasarkan 'tgl' dan 'date'
    print("Merging df_pengaduan dengan df_waktu berdasarkan 'tgl' dan 'date'...")
    df_pengaduan = df_pengaduan.merge(df_waktu, left_on='tgl', right_on='date', how='left')

    print("Data df_pengaduan setelah merge dengan df_waktu:")
    print(df_pengaduan.head())

    # Menghapus kolom yang tidak diperlukan setelah merge
    df_pengaduan.drop(columns=['tgl', 'date', 'day', 'month', 'year'], inplace=True)

    # Mengecek data yang sudah ada di database
    print("Mengambil data pengaduan yang sudah ada di database...")
    existing_pengaduan = pd.read_sql("SELECT idpelanggan, id_waktu FROM fact_pengaduan", engine_test_perumda)

    print("Data existing_pengaduan dari database:")
    print(existing_pengaduan.head())

    # Menentukan data yang belum ada di database
    df_pengaduan_to_process = df_pengaduan[~df_pengaduan.set_index(['idpelanggan', 'id_waktu']).index.isin(existing_pengaduan.set_index(['idpelanggan', 'id_waktu']).index)]

    print("Data yang belum ada di database (df_pengaduan_to_process):")
    print(df_pengaduan_to_process.head())

    print("Data jenis pengaduan baru yang perlu dimasukkan ke database:")
    print(df_jenisPengaduan[new_entries].head())

    return df_pengaduan_to_process, df_jenisPengaduan[new_entries]

def transform_pemutusan(dataframes, df_waktu, engine_test_perumda):  
    print("Memulai proses transformasi data pemutusan...")

    df_pemutusan = dataframes['pemutusan']

    # Menghapus duplikat berdasarkan kodepelanggan dan tglstk
    print("Menghapus duplikat berdasarkan 'kodepelanggan' dan 'tglstk'...")
    df_pemutusan.drop_duplicates(subset=['kodepelanggan', 'tglstk'], inplace=True)
    
    print("Data df_pemutusan setelah menghapus duplikat:")
    print(df_pemutusan.head())

    # Mengambil nilai unik realisasi dari pemutusan (menghapus nilai kosong)
    print("Mengambil nilai unik dari kolom 'realisasistk' setelah menghapus nilai kosong...")
    unique_realisasi = df_pemutusan['realisasistk'].dropna().astype(str).str.strip()
    unique_realisasi = unique_realisasi[unique_realisasi != ""].unique()
    
    print("Nilai unik realisasi yang ditemukan:")
    print(unique_realisasi)

    # Pilih kolom yang diperlukan
    print("Memilih kolom yang diperlukan: 'kodepelanggan', 'tglstk', 'realisasistk'...")
    df_pemutusan = df_pemutusan[['kodepelanggan', 'tglstk', 'realisasistk']]
    
    print("Data df_pemutusan setelah memilih kolom yang diperlukan:")
    print(df_pemutusan.head())

    # Mengubah kolom 'tglstk' menjadi format date
    print("Mengubah kolom 'tglstk' menjadi format date...")
    df_pemutusan['tglstk'] = pd.to_datetime(df_pemutusan['tglstk']).dt.date
    
    print("Data df_pemutusan setelah konversi 'tglstk':")
    print(df_pemutusan.head())

    # Merge dengan df_waktu berdasarkan tanggal
    print("Merging df_pemutusan dengan df_waktu berdasarkan 'tglstk' dan 'date'...")
    df_pemutusan = df_pemutusan.merge(df_waktu, left_on='tglstk', right_on='date', how='left').drop(columns=['tglstk', 'date', 'day', 'month', 'year'])
    
    print("Data df_pemutusan setelah merge dengan df_waktu:")
    print(df_pemutusan.head())

    # Mengecek data yang sudah ada di database
    print("Mengambil data pemutusan yang sudah ada di database...")
    existing_pemutusan = pd.read_sql("SELECT kodepelanggan, id_waktu FROM fact_pemutusan", engine_test_perumda)
    
    print("Data existing_pemutusan dari database:")
    print(existing_pemutusan.head())

    # Menentukan data yang belum ada di database
    df_pemutusan_to_process = df_pemutusan[~df_pemutusan.set_index(['kodepelanggan', 'id_waktu']).index.isin(existing_pemutusan.set_index(['kodepelanggan', 'id_waktu']).index)]
    
    print("Data df_pemutusan_to_process yang belum ada di database:")
    print(df_pemutusan_to_process.head())

    print("Proses transformasi data pemutusan selesai.")

    return df_pemutusan_to_process, unique_realisasi

def transform_sbbaru(dataframes, df_waktu, engine_test_perumda):
    print("Memulai proses transformasi data sbbaru...")

    df_sbbaru = dataframes['sbbaru']

    # Menghapus duplikat berdasarkan kodecpelanggan dan tglreg
    print("Menghapus duplikat berdasarkan 'kodecpelanggan' dan 'tglreg'...")
    df_sbbaru.drop_duplicates(subset=['kodecpelanggan', 'tglreg'], inplace=True)

    print("Data df_sbbaru setelah menghapus duplikat:")
    print(df_sbbaru.head())

    # Mengisi nilai NaN pada kolom jumlah dengan 0 dan mengubahnya menjadi integer
    print("Mengisi nilai NaN pada kolom 'jumlah' dengan 0 dan mengubahnya menjadi integer...")
    df_sbbaru['jumlah'] = df_sbbaru['jumlah'].fillna(0).astype(int)

    print("Data df_sbbaru setelah mengisi nilai NaN pada 'jumlah':")
    print(df_sbbaru.head())

    # Membuat kolom wilayah berdasarkan 2 karakter pertama kodecpelanggan
    print("Membuat kolom 'wilayah' berdasarkan 2 karakter pertama dari 'kodecpelanggan'...")
    df_sbbaru['wilayah'] = df_sbbaru['kodecpelanggan'].str[:2]

    print("Data df_sbbaru setelah menambahkan kolom 'wilayah':")
    print(df_sbbaru.head())

    # Menandai realisasi sebagai 'Y' jika jumlah sudah dibayar (tidak nol)
    print("Menandai 'realisasi' sebagai 'Y' jika 'jumlah' > 0...")
    df_sbbaru.loc[df_sbbaru['jumlah'] > 0, 'realisasi'] = 'Y'

    print("Data df_sbbaru setelah update kolom 'realisasi':")
    print(df_sbbaru.head())

    # Filter untuk menghapus baris dengan kondisi tertentu
    print("Melakukan filtering data untuk menghapus nilai yang tidak valid...")
    df_sbbaru = df_sbbaru[~(
        df_sbbaru['jumlah'].isna() |  
        (df_sbbaru['realisasi'].astype(str).str.strip() == '') |  
        (df_sbbaru['realisasi'] == ' ') |  
        (~df_sbbaru['kodecpelanggan'].astype(str).str.contains('REG', na=False))  
    )]

    print("Data df_sbbaru setelah filtering:")
    print(df_sbbaru.head())

    # Mengambil nilai unik realisasi setelah filtering
    print("Mengambil nilai unik realisasi dari sbbaru...")
    unique_realisasi = df_sbbaru['realisasi'].dropna().astype(str).str.strip()
    unique_realisasi = unique_realisasi[unique_realisasi != ""].unique()

    print("Nilai unik realisasi yang ditemukan:")
    print(unique_realisasi)

    # Pilih kolom yang diperlukan
    print("Memilih kolom yang diperlukan: 'kodecpelanggan', 'tglreg', 'realisasi', 'wilayah', 'jumlah'...")
    df_sbbaru = df_sbbaru[['kodecpelanggan', 'tglreg', 'realisasi', 'wilayah', 'jumlah']]

    print("Data df_sbbaru setelah memilih kolom:")
    print(df_sbbaru.head())

    # Mengubah kolom 'tglreg' menjadi format date
    print("Mengubah kolom 'tglreg' menjadi format date...")
    df_sbbaru['tglreg'] = pd.to_datetime(df_sbbaru['tglreg']).dt.date

    print("Data df_sbbaru setelah konversi 'tglreg':")
    print(df_sbbaru.head())

    # Merge dengan df_waktu
    print("Merging df_sbbaru dengan df_waktu berdasarkan 'tglreg' dan 'date'...")
    df_sbbaru = df_sbbaru.merge(df_waktu, left_on='tglreg', right_on='date', how='left').drop(columns=['tglreg', 'date', 'day', 'month', 'year'])

    print("Data df_sbbaru setelah merge dengan df_waktu:")
    print(df_sbbaru.head())

    # Mengecek data yang sudah ada di database
    print("Mengambil data sbbaru yang sudah ada di database...")
    existing_sbbaru = pd.read_sql("SELECT kodecpelanggan, id_waktu FROM fact_sbbaru", engine_test_perumda)

    print("Data existing_sbbaru dari database:")
    print(existing_sbbaru.head())

    # Menentukan data yang belum ada di database
    df_sbbaru_to_process = df_sbbaru[~df_sbbaru.set_index(['kodecpelanggan', 'id_waktu']).index.isin(existing_sbbaru.set_index(['kodecpelanggan', 'id_waktu']).index)]

    print("Data df_sbbaru_to_process yang belum ada di database:")
    print(df_sbbaru_to_process.head())

    print("Proses transformasi data sbbaru selesai.")

    return df_sbbaru_to_process, unique_realisasi

def transform_realisasi(unique_realisasi_pemutusan, unique_realisasi_sbbaru, engine_test_perumda):
    print("Memulai proses transformasi data realisasi...")

    # Menggabungkan nilai unik dari pemutusan dan sbbaru
    print("Menggabungkan nilai unik dari pemutusan dan sbbaru...")
    unique_realisasi = list(set(unique_realisasi_pemutusan) | set(unique_realisasi_sbbaru))

    print("Nilai unik realisasi setelah digabungkan:")
    print(unique_realisasi)

    # Membuat DataFrame untuk realisasi baru
    df_realisasi = pd.DataFrame({'jenis_realisasi': unique_realisasi}).drop_duplicates()

    print("Data df_realisasi setelah dibuat:")
    print(df_realisasi.head())

    # Mengambil data realisasi yang sudah ada di database
    print("Mengambil data realisasi yang sudah ada di database...")
    existing_realisasi = pd.read_sql("SELECT id_realisasi, jenis_realisasi FROM dim_realisasi", engine_test_perumda)

    print("Data existing_realisasi dari database:")
    print(existing_realisasi.head())

    # Menyaring data yang belum ada di database
    df_realisasi_to_insert = df_realisasi[~df_realisasi['jenis_realisasi'].isin(existing_realisasi['jenis_realisasi'])]

    print("Data realisasi yang perlu diinsert:")
    print(df_realisasi_to_insert.head())

    # Insert data baru jika ada
    if not df_realisasi_to_insert.empty:
        print("Menyimpan data realisasi baru ke database...")
        try:
            with engine_test_perumda.begin() as connection:
                df_realisasi_to_insert.to_sql('dim_realisasi', connection, if_exists='append', index=False)
            print("Data realisasi baru berhasil disimpan ke database.")
        except Exception as e:
            print(f"⚠️ Gagal menyimpan data realisasi ke database: {str(e)}")
    else:
        print("Tidak ada data realisasi baru yang perlu disimpan.")

    # Mengambil kembali data realisasi yang sudah lengkap dari database
    print("Mengambil kembali data realisasi terbaru dari database...")
    df_realisasi = pd.read_sql("SELECT id_realisasi, jenis_realisasi FROM dim_realisasi", engine_test_perumda)

    print("Data df_realisasi setelah update:")
    print(df_realisasi.head())

    print("Transformasi data realisasi selesai.")

    return df_realisasi

def main(dataframes, engine_test_perumda):
    df_pelanggan = transform_pelanggan(dataframes, engine_test_perumda)
    df_goltarif = transform_goltarif(dataframes, engine_test_perumda)
    df_waktu = transform_waktu(engine_test_perumda)
    df_transaksi = transform_transaksi(dataframes, df_pelanggan, df_waktu, engine_test_perumda)
    df_pengaduan, df_jenisPengaduan = transform_pengaduan(dataframes, df_waktu, engine_test_perumda)
    df_pemutusan, unique_realisasi_pemutusan = transform_pemutusan(dataframes, df_waktu, engine_test_perumda)
    df_sbbaru, unique_realisasi_sbbaru = transform_sbbaru(dataframes, df_waktu, engine_test_perumda)
    df_realisasi = transform_realisasi(unique_realisasi_pemutusan, unique_realisasi_sbbaru, engine_test_perumda)

    # Merge df_realisasi ke df_pemutusan dan df_sbbaru
    df_pemutusan = df_pemutusan.merge(df_realisasi, left_on='realisasistk', right_on='jenis_realisasi', how='left').drop(columns=['realisasistk', 'jenis_realisasi'])
    df_sbbaru = df_sbbaru.merge(df_realisasi, left_on='realisasi', right_on='jenis_realisasi', how='left').drop(columns=['realisasi', 'jenis_realisasi'])

    return {
        "dim_pelanggan": df_pelanggan,
        "dim_goltarif": df_goltarif,
        "dim_waktu": df_waktu,
        "dim_realisasi": df_realisasi,
        "fact_transaksi": df_transaksi,
        "dim_jenispengaduan": df_jenisPengaduan,
        "fact_pengaduan": df_pengaduan,
        "fact_pemutusan": df_pemutusan,
        "fact_sbbaru": df_sbbaru,
    }
