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



# Memulai eksekusi program
if __name__ == '__main__':
    # Membangun pipeline Luigi dengan urutan tugas yang telah ditentukan
    luigi.build([ExtractPTN()],
                local_scheduler=True) # Menjalankan scheduler lokal untuk mengatur eskusi tugas