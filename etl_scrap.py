# import requests
# from bs4 import BeautifulSoup
# import pandas as pd

# # URL target untuk web scraping
# url = "https://sidatagrun-public-1076756628210.asia-southeast2.run.app/ptn_sb.php?ptn=-1"

# # Mengirim permintaan GET untuk mendapatkan HTML
# response = requests.get(url)

# # Parsing HTML dengan BeautifulSoup
# soup = BeautifulSoup(response.text, 'html.parser')

# # Mencari tabel yang diinginkan
# table = soup.find('table', {'class': 'table table-striped'})

# # Menyiapkan list untuk menampung data
# data = []

# # Mengambil setiap baris dalam tabel
# rows = table.find_all('tr')[1:]  # Mengabaikan header

# for row in rows:
#     cols = row.find_all('td')
    
#     if len(cols) >= 6:  # Pastikan ada cukup kolom
#         kode = cols[1].find('a').text.strip()  # Kode di kolom kedua
#         nama = cols[2].find('a').text.strip()  # Nama di kolom ketiga
#         kab_kota = cols[3].text.strip()        # Kab/Kota di kolom keempat
#         provinsi_1 = cols[4].text.strip()      # Provinsi-1 di kolom kelima
#         provinsi_2 = cols[5].text.strip() if cols[5].text.strip() else None  # Provinsi-2 di kolom keenam (bisa kosong)
#         link_profil = f"https://sidatagrun-public-1076756628210.asia-southeast2.run.app/ptn_sb.php?ptn={kode[1:]}" # Link profil
        
#         # Menambahkan data ke list
#         data.append({
#             "kode": kode,
#             "nama": nama,
#             "kab_kota": kab_kota,
#             "provinsi_1": provinsi_1,
#             "provinsi_2": provinsi_2,
#             "link_profil": link_profil
#         })

# # Membuat dataframe dari data yang telah dikumpulkan
# df = pd.DataFrame(data)

# # Menyimpan dataframe ke CSV
# csv_file = "coba.csv"
# df.to_csv(csv_file, index=False)

import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL utama dengan parameter berbeda (ptn=-1, ptn=-2, ptn=-3)
url_template = "https://sidatagrun-public-1076756628210.asia-southeast2.run.app/ptn_sb.php?ptn={}"

# Daftar ptn yang akan di-loop
ptns = [-1, -2, -3]

# Fungsi untuk melakukan web scraping dan menyimpan hasil ke CSV
def scrape_and_save(ptn_value):
    # Menghasilkan URL dengan ptn yang berbeda
    url = url_template.format(ptn_value)

    # Mengirim permintaan GET untuk mendapatkan HTML
    response = requests.get(url)

    # Parsing HTML dengan BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Mencari tabel yang diinginkan
    table = soup.find('table', {'class': 'table table-striped'})

    # Menyiapkan list untuk menampung data
    data = []

    # Mengambil setiap baris dalam tabel
    rows = table.find_all('tr')[1:]  # Mengabaikan header

    for row in rows:
        cols = row.find_all('td')
        
        if len(cols) >= 6:  # Pastikan ada cukup kolom
            kode = cols[1].find('a').text.strip()  # Kode di kolom kedua
            nama = cols[2].find('a').text.strip()  # Nama di kolom ketiga
            kab_kota = cols[3].text.strip()        # Kab/Kota di kolom keempat
            provinsi_1 = cols[4].text.strip()      # Provinsi-1 di kolom kelima
            provinsi_2 = cols[5].text.strip() if cols[5].text.strip() else None  # Provinsi-2 di kolom keenam (bisa kosong)
            link_profil = f"https://sidatagrun-public-1076756628210.asia-southeast2.run.app/ptn_sb.php?ptn={kode[1:]}"  # Link profil

            # Menambahkan data ke list
            data.append({
                "kode": kode,
                "nama": nama,
                "kab_kota": kab_kota,
                "provinsi_1": provinsi_1,
                "provinsi_2": provinsi_2,
                "link_profil": link_profil
            })

    # Membuat dataframe dari data yang telah dikumpulkan
    df = pd.DataFrame(data)

    # Menyimpan dataframe ke CSV
    csv_file = f"data_{ptn_value}.csv"
    df.to_csv(csv_file, index=False)

    # Mengembalikan path file CSV
    return csv_file

# Iterasi untuk setiap ptn dan simpan hasilnya
csv_files = [scrape_and_save(ptn) for ptn in ptns]

# Menampilkan file CSV yang telah disimpan
csv_files

