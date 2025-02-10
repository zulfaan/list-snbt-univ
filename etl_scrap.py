from bs4 import BeautifulSoup
import pandas as pd
import requests
import luigi


# Task untuk mengekstrak data PTN dari SIDATA
class ExtractPTN(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan
    
    def output(self):
        return luigi.LocalTarget('raw-data/raw_list_univ.csv') # Tempat penyimpanan data yang diekstrak

    def run(self):
        # URL untuk mengakses data PTN di SIDATA
        base_url = "https://sidatagrun-public-1076756628210.asia-southeast2.run.app/ptn_sb.php?ptn={}"
        # URL utama dengan parameter berbeda (ptn=-1, ptn=-2, ptn=-3)
        pages = [-1, -2, -3]
        # List untuk menyimpan data universitas
        list_universitas = []

        for page in pages:
            url = base_url.format(page) # Membuat URL dengan parameter page
            response = requests.get(url) # Mengirim permintaan GET ke URL
            soup = BeautifulSoup(response.text, 'html.parser') # Parsing HTML dengan BeautifulSoup
            table = soup.find('table', {'class': 'table table-striped'}) # Mencari tabel dengan class 'table table-striped'
            rows = table.find_all('tr')[1:] # Mengambil semua baris dalam tabel, kecuali header

            for row in rows: # Iterasi setiap baris dalam tabel
                cols = row.find_all('td')

                if len(cols) >= 6: # Pastikan jumlah kolom cukup
                    kode = cols[1].find('a').text.strip() # Mengambil kode universitas
                    nama = cols[2].find('a').text.strip() # Mengambil nama universitas
                    kab_kota = cols[3].text.strip() # Mengambil kab/kota universitas
                    provinsi_1 = cols[4].text.strip() # Mengambil provinsi-1 universitas
                    provinsi_2 = cols[5].text.strip() if cols[5].text.strip() else None # Mengambil provinsi-2 universitas (bisa kosong)
                    link_profil = f'https://sidatagrun-public-1076756628210.asia-southeast2.run.app/ptn_sb.php?ptn={kode[1:]}' # Link profil universitas

                    # Menambahkan data universitas ke dalam list
                    list_universitas.append({
                        'kode': kode,
                        'nama': nama,
                        'kab_kota': kab_kota,
                        'provinsi_1': provinsi_1,
                        'provinsi_2': provinsi_2,
                        'link_profil': link_profil
                    })
        raw_univ_df = pd.DataFrame(list_universitas) # Membuat DataFrame dari list universitas
        raw_univ_df.to_csv(self.output().path, index=False) # Menyimpan DataFrame ke dalam file CSV

class ExtractProdi(luigi.Task):
    def requires(self):
        return ExtractPTN() # Task ini membutuhkan output dari task ExtractPTN
    
    def output(self):
        return luigi.LocalTarget('raw-data/raw_list_prodi.csv') # Tempat penyimpanan data yang diekstrak
    
    def run(self):
        universitas_data = pd.read_csv(self.input().path) # Membaca data universitas dari file CSV
        list_prodi = [] # List untuk menyimpan data program studi

        for index, row in universitas_data.iterrows(): # Iterasi setiap baris dalam data universitas
            kode_univ = row['kode'] # Mengambil kode universitas
            nama_univ = row['nama'] # Mengambil nama universitas
            url = row['link_profil'] # Mengambil link profil universitas

            response = requests.get(url) # Mengirim permintaan GET ke URL
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find('table', {'class': 'table-striped'}).find_all('tr')[1:]  # Ambil semua baris, kecuali header

            for table in tables: # Iterasi setiap tabel dalam halaman profil universitas
                cols = table.find_all('td')

                if len(cols) < 7:
                    continue # Skip jika jumlah kolom kurang dari 7
                link = cols[2].find('a')
                if not link:
                    continue # Skip jika elemen <a> tidak ditemukan
                kode_prodi = cols[1].text.strip() # Mengambil kode program studi
                program_studi = link.text.strip() # Mengambil nama program studi
                jenjang = cols[3].text.strip() # Mengambil jenjang program studi
                daya_tampung = cols[4].text.strip() # Mengambil daya tampung program studi
                peminat = cols[5].text.strip() # Mengambil peminat program studi
                portofolio = cols[6].text.strip() # Mengambil nilai terendah program studi

                # Menambahkan data program studi ke dalam list
                list_prodi.append({
                    'kode_univ': kode_univ,
                    'nama_univ': nama_univ,
                    'kode_prodi': kode_prodi,
                    'program_studi': program_studi,
                    'jenjang': jenjang,
                    'daya_tampung_2025': daya_tampung,
                    'peminat_2024': peminat,
                    'jenis_portofolio': portofolio
                })

        raw_prodi_df = pd.DataFrame(list_prodi) # Membuat DataFrame dari list program studi
        raw_prodi_df.to_csv(self.output().path, index=False) # Menyimpan DataFrame ke dalam file CSV

# Memulai eksekusi program
if __name__ == '__main__':
    # Membangun pipeline Luigi dengan urutan tugas yang telah ditentukan
    luigi.build([ExtractPTN(),
                ExtractProdi()],
                local_scheduler=True) # Menjalankan scheduler lokal untuk mengatur eskusi tugas