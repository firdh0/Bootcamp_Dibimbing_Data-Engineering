# Analisis Data Loyalitas Pelanggan Maskapai

## Ringkasan Proyek

Proyek ini bertujuan untuk menganalisis data aktivitas dan demografi pelanggan dari sebuah program loyalitas maskapai penerbangan. Dengan menggunakan Apache Spark, kami melakukan serangkaian proses mulai dari pembersihan data, transformasi, analisis, hingga visualisasi untuk mendapatkan *insight* bisnis yang berharga.

Tujuan utamanya adalah для memahami perilaku pelanggan, mengidentifikasi segmen-segmen penting, dan menemukan pola yang dapat digunakan untuk meningkatkan strategi bisnis dan pemasaran.

---

## Alur Proses Analisis (Pipeline)

Pipeline data ini dirancang secara modular dan dijalankan secara berurutan oleh `main.py`. Berikut adalah tahapan utamanya:

1.  **Inisialisasi (`SparkManager`)**: Membuat dan mengonfigurasi sesi Spark yang menjadi fondasi seluruh proses.
2.  **Pemuatan Data (`DataLoader`)**: Memuat dataset mentah (`.csv`) dari direktori `data/raw/`.
3.  **Pembersihan Data (`DataCleaner`)**: Menjalankan serangkaian proses pembersihan pada setiap dataset, termasuk:
    * Standardisasi nama kolom.
    * Penanganan nilai duplikat.
    * Pemeriksaan dan imputasi nilai *null* (kosong) menggunakan metode statistik (mean, median, atau modus).
4.  **Penyimpanan Data Bersih (`DataSaver`)**: Menyimpan hasil dari tahap pembersihan ke dalam format Parquet di direktori `data/cleaned/` sebagai *checkpoint*.
5.  **Transformasi Data (`DataTransformer`)**: Menggabungkan (`join`) data aktivitas penerbangan dengan data demografi pelanggan berdasarkan `Loyalty_Number` untuk menciptakan satu dataset yang komprehensif.
6.  **Analisis Data (`CustomerDataAnalyzer`)**: Menjalankan serangkaian query SQL pada data yang telah digabungkan untuk menjawab pertanyaan-pertanyaan bisnis kunci. Hasil dari setiap analisis ini kemudian disimpan dalam format `.csv` di direktori `data/analysis_results_csv/`.
7.  **Visualisasi Data (`DataVisualizer`)**: Mengonversi hasil analisis menjadi visualisasi grafis (bar chart, line chart, scatter plot, dll.) dan menyimpannya sebagai file gambar (`.png`) di direktori `final_visualizations/`.

---

## Detail Analisis, Query, dan Interpretasi

Berikut adalah rincian dari **semua 14 analisis** yang dilakukan, beserta query SQL yang digunakan, hasil visualisasi, dan interpretasi dari potensi *insight* yang ditemukan.

### 1. Rata-rata Penerbangan per Pelanggan per Tahun
* **Pertanyaan:** Berapa rata-rata jumlah penerbangan untuk setiap pelanggan per tahunnya?
* **Query SQL:**
    ```sql
    SELECT
        Loyalty_Number,
        Year,
        AVG(Total_Flights) as Avg_Flights_Per_Year
    FROM flight_activity_view
    GROUP BY Loyalty_Number, Year
    ORDER BY Loyalty_Number, Year
    ```
* **Interpretasi Insight:** 
    1. Kontributor Terbesar: Pelanggan dengan tingkat pendidikan Sarjana (Bachelor) secara kolektif menjadi penyumbang terbesar untuk total jarak penerbangan.
    2. Penyebab Utama: Angka yang tinggi ini kemungkinan besar bukan karena setiap individu di segmen ini terbang lebih jauh, melainkan karena jumlah pelanggan dalam segmen "Bachelor" adalah yang paling banyak di dalam dataset. Grafik ini menunjukkan nilai total (akumulatif), bukan rata-rata per pelanggan.
    3. Segmen Bernilai Tinggi (Potensial): Total jarak yang rendah dari segmen "Master" dan "Doktor" kemungkinan disebabkan oleh jumlah populasi mereka yang lebih kecil. Ada kemungkinan jika dihitung rata-ratanya, justru segmen inilah yang memiliki nilai perjalanan per individu paling tinggi.
* **Kesimpulan:** 
Secara volume, segmen pelanggan "Bachelor" adalah pasar massa yang paling penting karena mereka menghasilkan total jarak tempuh terbesar. Namun, untuk strategi yang lebih spesifik seperti penawaran premium atau program loyalitas berjenjang, mengandalkan data total ini bisa menyesatkan.

### 2. Distribusi Poin Berdasarkan Kartu Loyalitas
* **Pertanyaan:** Bagaimana distribusi total poin yang diakumulasikan pelanggan berdasarkan jenis kartu loyalitas mereka?
* **Query SQL:**
    ```sql
    SELECT
        Loyalty_Card,
        SUM(Points_Accumulated) as Total_Points_Accumulated,
        AVG(Points_Accumulated) as Avg_Points_Accumulated_Per_Record
    FROM merged_view
    WHERE Loyalty_Card IS NOT NULL
    GROUP BY Loyalty_Card
    ORDER BY Total_Points_Accumulated DESC
    ```
* **Grafik:**
    ![Grafik Distribusi Poin per Kartu](final_visualizations/points_by_card.png)
* **Interpretasi Insight:** 
    1. Aktivitas Sangat Rendah untuk Mayoritas: Grafik ini menunjukkan bahwa di semua tiga jenis kartu (Aurora, Nova, Star), mayoritas besar anggota (lebih dari 75%) memiliki akumulasi poin yang sangat sedikit atau mendekati nol. Ini terlihat dari "kotak" (box) pada plot yang sangat pendek dan berada di bagian bawah.
    2. Didominasi oleh 'Super User': Seluruh aktivitas perolehan poin secara efektif didorong oleh segelintir anggota yang sangat aktif. Anggota-anggota ini direpresentasikan oleh titik-titik data di luar kotak (outliers) yang nilainya jauh melampaui anggota biasa.
    3. Tier Kartu Menunjukkan Nilai Tertinggi: Para 'super user' dengan akumulasi poin tertinggi sebagian besar terkonsentrasi pada pemegang kartu Star dan Nova. Ini menandakan bahwa pelanggan Anda yang paling bernilai (dalam hal perolehan poin) berada di dua tingkatan kartu tersebut.
* **Kesimpulan:** 
Program loyalitas Anda saat ini memiliki tingkat keterlibatan (engagement) yang rendah di kalangan mayoritas anggota. Program ini lebih berfungsi untuk memberi penghargaan kepada sekelompok kecil pelanggan elit yang sudah sangat aktif, daripada mendorong aktivitas bagi seluruh basis anggota.


### 3. Hubungan Pendidikan dan Jumlah Penerbangan
* **Pertanyaan:** Apakah ada hubungan antara tingkat pendidikan pelanggan dengan jumlah rata-rata penerbangan yang mereka lakukan?
* **Query SQL:**
    ```sql
    SELECT
        Education,
        AVG(Total_Flights) as Avg_Total_Flights_Per_Record,
        SUM(Total_Flights) as Sum_Total_Flights
    FROM merged_view
    WHERE Education IS NOT NULL
    GROUP BY Education
    ORDER BY Avg_Total_Flights_Per_Record DESC
    ```
* **Grafik:**
    ![Grafik Penerbangan per Pendidikan](final_visualizations/flights_by_education.png)
* **Interpretasi Insight:** 
    1. Tidak Ada Perbedaan yang Berarti: Grafik ini dengan jelas menunjukkan bahwa rata-rata jumlah penerbangan hampir sama di semua tingkat pendidikan. Semua segmen, mulai dari "High School or Below" hingga "Doctor", rata-rata melakukan penerbangan sekitar 12 kali.
    2. Frekuensi vs. Jarak: Temuan ini sangat menarik jika dibandingkan dengan analisis sebelumnya. Meskipun pelanggan "Doktor" dan "Master" terbang lebih jauh dalam setiap perjalanannya, mereka tidak terbang lebih sering daripada pelanggan dengan tingkat pendidikan lainnya.
    3. Perilaku yang Seragam: Dalam hal frekuensi atau jumlah penerbangan, semua segmen pelanggan Anda berperilaku sangat seragam.
* **Kesimpulan:** 
Tingkat pendidikan bukanlah faktor yang bisa digunakan untuk membedakan seberapa sering seorang pelanggan terbang.
Implikasinya adalah, jika ingin membuat program untuk meningkatkan frekuensi terbang (misalnya, promo "terbang X kali dapat bonus"), menargetkan pelanggan berdasarkan tingkat pendidikan mereka tidak akan menjadi strategi yang efektif. Semua segmen memiliki potensi yang sama untuk merespons promo semacam ini.
Ini memperkuat gambaran bahwa pelanggan pascasarjana adalah spesialis penerbangan jarak jauh (long-haul), sementara pelanggan lainnya terbang sama seringnya dengan kombinasi jarak pendek dan jauh.


### 4. Tren Penerbangan dari Waktu ke Waktu
* **Pertanyaan:** Bagaimana tren total jumlah penerbangan secara bulanan dari waktu ke waktu?
* **Query SQL:**
    ```sql
    SELECT
        Year,
        Month,
        SUM(Total_Flights) as Total_Flights_Per_Month
    FROM flight_activity_view
    GROUP BY Year, Month
    ORDER BY Year, Month
    ```
* **Grafik:**
    ![Grafik Tren Penerbangan Bulanan](final_visualizations/flight_trends.png)
* **Interpretasi Insight:** 
    1. Pola Musiman (Seasonal) yang Kuat: Grafik ini dengan sangat jelas menunjukkan bahwa tren jumlah penerbangan tidak naik atau turun secara linear, melainkan mengikuti pola musiman yang konsisten dan berulang setiap tahunnya.
    2. Puncak Musim Ramai (High Season): Ada dua periode puncak utama dalam setahun:
        - Musim Liburan Tengah Tahun (Juni - Agustus): Ini adalah periode tersibuk bagi maskapai, dengan lonjakan tertinggi terjadi pada bulan Agustus.
        - Musim Liburan Akhir Tahun (Desember): Terjadi lonjakan tajam kedua pada bulan Desember, yang jelas berkaitan dengan libur Natal dan Tahun Baru.
    3. Periode Sepi (Low Season): Jumlah penerbangan mencapai titik terendahnya pada periode setelah liburan tengah tahun, yaitu sekitar bulan September dan Oktober. Bulan Februari juga menunjukkan penurunan yang cukup signifikan setiap tahunnya.
* **Kesimpulan:** 
Bisnis penerbangan ini sangat bergantung pada musim (highly seasonal). Permintaan pelanggan sangat bisa diprediksi, di mana puncaknya terjadi pada pertengahan dan akhir tahun, sementara periode paling sepi adalah di awal musim gugur (September-Oktober).


### 5. Hubungan Gaji dan Jarak Penerbangan
* **Pertanyaan:** Apakah ada hubungan antara tingkat gaji (`Salary`) seorang pelanggan dengan jarak (`Distance`) penerbangan yang biasa mereka tempuh?
* **Query SQL:**
    ```sql
    SELECT
        Salary,
        Distance
    FROM merged_view
    WHERE Salary IS NOT NULL AND Distance > 0
    LIMIT 5000 
    ```
* **Grafik:**
    ![Grafik Hubungan Gaji dan Jarak Penerbangan](final_visualizations/salary_vs_distance.png)
* **Interpretasi Insight:** 
    1. Sebaran Data Acak: Titik-titik data pada grafik tersebar secara acak dan tidak membentuk pola yang jelas (seperti garis lurus yang menanjak atau menurun).
    2. Tidak Ada Tren: Ini menunjukkan bahwa kenaikan gaji seorang pelanggan tidak secara otomatis berarti mereka akan melakukan perjalanan yang lebih jauh. Pelanggan dengan gaji rendah bisa saja melakukan penerbangan jarak jauh, dan sebaliknya, pelanggan dengan gaji sangat tinggi bisa saja hanya melakukan penerbangan jarak pendek.
    3. Gaji Bukan Prediktor Jarak: Tingkat gaji seorang pelanggan terbukti bukanlah sebuah prediktor yang baik untuk memperkirakan seberapa jauh mereka akan terbang dalam suatu perjalanan.
* **Kesimpulan:** 
Keputusan seorang pelanggan untuk melakukan penerbangan jarak jauh atau pendek tidak ditentukan oleh tingkat pendapatan mereka.
Ini menyiratkan bahwa faktor lain—yang tidak terlihat di grafik ini—jauh lebih berpengaruh. Faktor-faktor tersebut kemungkinan besar adalah:
    - Tujuan Perjalanan: Apakah untuk bisnis, liburan, mengunjungi keluarga, atau lainnya.
    - Gaya Hidup (Lifestyle): Preferensi personal pelanggan terhadap jenis liburan.
    - Kebutuhan Pekerjaan: Jenis pekerjaan tertentu mungkin menuntut perjalanan jauh terlepas dari besaran gajinya.

Secara strategis, menggunakan data gaji untuk melakukan segmentasi pelanggan pada penawaran promo penerbangan jarak jauh tidak akan efektif. Segmentasi yang lebih baik seharusnya didasarkan pada histori perjalanan atau perilaku pelanggan itu sendiri.

### 6. Nilai Tukar Poin
* **Pertanyaan:** Bagaimana hubungan antara poin yang ditukarkan (`Points_Redeemed`) dengan nilai moneternya dalam dolar (`Dollar_Cost_Points_Redeemed`)?
* **Query SQL:**
    ```sql
    SELECT
        Points_Redeemed,
        Dollar_Cost_Points_Redeemed
    FROM flight_activity_view
    WHERE Points_Redeemed > 0
    ```
* **Grafik:**
    ![Grafik Nilai Tukar Poin](final_visualizations/points_exchange.png)
* **Interpretasi Insight:** 
    1. Hubungan Linier Positif yang Kuat: Titik-titik data pada grafik secara jelas membentuk pola garis lurus yang menanjak dari kiri bawah ke kanan atas. Ini menunjukkan adanya hubungan linier positif yang sangat kuat antara Points_Redeemed dan Dollar_Cost_Points_Redeemed. Artinya, seiring dengan peningkatan jumlah poin yang ditukarkan, nilai moneternya dalam dolar juga meningkat secara proporsional.
    2. Nilai Tukar Konstan: Garis lurus tersebut menunjukkan bahwa nilai dolar yang diperoleh per poin yang ditukarkan adalah konstan di seluruh rentang data yang diamati. Tidak ada indikasi adanya perubahan nilai tukar (misalnya, menjadi lebih tinggi atau lebih rendah) untuk penukaran poin dalam jumlah tertentu.
    3. Dapat Diprediksi: Karena hubungannya linier dan konsisten, nilai dolar yang akan didapatkan untuk sejumlah poin tertentu dapat dengan mudah diprediksi. Demikian pula, jumlah poin yang dibutuhkan untuk mencapai nilai dolar tertentu juga dapat dihitung.
* **Kesimpulan:** 
Nilai tukar antara poin yang ditukarkan dan nilai moneternya dalam dolar adalah tetap dan seragam. Keputusan untuk menukarkan sejumlah poin tertentu akan menghasilkan nilai dolar yang secara langsung proporsional dengan jumlah poin tersebut. Hal ini menyiratkan bahwa sistem penukaran poin ini beroperasi dengan model nilai tukar yang sangat transparan dan mudah dipahami, di mana setiap poin memiliki "bobot" dolar yang sama, terlepas dari total jumlah poin yang ditukarkan.

### 7. Distribusi Gaji Berdasarkan Pendidikan
* **Pertanyaan:** Bagaimana perbandingan distribusi gaji (`Salary`) di antara pelanggan dengan tingkat pendidikan (`Education`) yang berbeda-beda?
* **Query SQL:**
    ```sql
    SELECT
        Education,
        Salary
    FROM merged_view
    WHERE Education IS NOT NULL AND Salary IS NOT NULL
    ```
* **Grafik:**
    ![Grafik Distribusi Gaji Berdasarkan Pendidikan](final_visualizations/salary_dist_by_education.png)
* **Interpretasi Insight:** 
    1. Gaji Rata-rata Meningkat Seiring Tingkat Pendidikan: Secara umum, ada tren yang jelas bahwa median gaji (garis tengah dalam kotak boxplot) cenderung meningkat seiring dengan peningkatan tingkat pendidikan.
        - Doctor: Memiliki median gaji tertinggi dan rentang gaji yang paling luas (dari sekitar $50.000 hingga lebih dari $380.000, dengan outlier mencapai $400.000).
        - Master: Menunjukkan median gaji yang lebih tinggi dari Bachelor, College, dan High School, dengan rentang yang lebih terkonsentrasi di sekitar median.
        - Bachelor: Memiliki median gaji yang signifikan lebih tinggi dari College dan High School.
        - College: Memiliki median gaji yang relatif rendah, namun lebih tinggi dari High School or Below.
        - High School or Below: Menunjukkan median gaji terendah dan rentang interkuartil (IQR) yang paling sempit, mengindikasikan distribusi gaji yang lebih terkonsentrasi pada nilai yang lebih rendah.
    2. Variabilitas Gaji Berbeda Antar Tingkat Pendidikan:
        - Doctor dan Bachelor menunjukkan variabilitas gaji yang paling tinggi, ditunjukkan oleh ukuran kotak (IQR) dan rentang whisker yang lebih panjang, serta adanya outlier yang signifikan. Ini berarti ada perbedaan gaji yang lebih besar di antara individu dalam kelompok pendidikan ini.
        - Master memiliki rentang gaji yang cukup terkonsentrasi di sekitar median, menunjukkan variabilitas yang lebih rendah dibandingkan Doctor dan Bachelor.
        - College dan High School or Below menunjukkan variabilitas gaji yang paling rendah, dengan kotak dan whisker yang lebih pendek, mengindikasikan bahwa gaji cenderung lebih terkonsentrasi di sekitar median untuk kelompok-kelompok ini.

    3. Adanya Outlier di Beberapa Tingkat Pendidikan:
        - Bachelor dan Doctor memiliki sejumlah outlier di bagian bawah dan atas, menunjukkan bahwa ada individu dengan gaji yang jauh di bawah atau di atas sebagian besar rekan mereka dalam tingkat pendidikan yang sama. Outlier di kelompok Doctor juga mencakup gaji yang sangat tinggi, mendekati $400.000.
        - High School or Below juga memiliki beberapa outlier di bagian bawah, menunjukkan ada individu dengan gaji yang sangat rendah.
* **Kesimpulan:** 
Berdasarkan analisis distribusi gaji, dapat disimpulkan bahwa tingkat pendidikan merupakan faktor penting yang memengaruhi besaran gaji. Semakin tinggi tingkat pendidikan yang dicapai (terutama hingga gelar Doctor), semakin tinggi pula potensi median gaji yang dapat diperoleh. Selain itu, tingkat pendidikan yang lebih tinggi (seperti Bachelor dan Doctor) juga cenderung memiliki variabilitas gaji yang lebih besar, menunjukkan adanya rentang peluang finansial yang lebih luas di antara individu-individu dengan kualifikasi tersebut. Sebaliknya, tingkat pendidikan yang lebih rendah (seperti High School or Below) cenderung menghasilkan gaji yang lebih rendah dan distribusi gaji yang lebih homogen. Ini menggarisbawahi pentingnya pendidikan dalam potensi penghasilan seseorang.

### 8. Distribusi Gaji Keseluruhan
* **Pertanyaan:** Bagaimana distribusi pendapatan (`Salary`) keseluruhan pelanggan dalam program loyalitas ini?
* **Query SQL:**
    ```sql
    SELECT Salary 
    FROM merged_view TABLESAMPLE (10 PERCENT)
    WHERE Salary IS NOT NULL
    ```
* **Hasil:** Sampel 10% dari data gaji pelanggan, digunakan untuk membuat histogram.
* **Grafik:**
    ![Grafik Distribusi Gaji Keseluruhan](final_visualizations/salary_distribution.png)
* **Interpretasi Insight:** Histogram ini memberikan gambaran umum tentang profil ekonomi basis pelanggan. Jika mayoritas pelanggan berada di rentang gaji menengah, maka strategi pemasaran massal lebih efektif. Jika distribusinya condong ke kanan (gaji tinggi), maka fokus pada layanan premium lebih menjanjikan.

### 9. Komposisi Demografis
* **Pertanyaan:** Bagaimana komposisi status pernikahan (`Marital_Status`) dalam setiap kategori pendidikan (`Education`)?
* **Query SQL:**
    ```sql
    SELECT
        Education,
        Marital_Status,
        COUNT(*) as Customer_Count_Per_Segment
    FROM merged_view
    WHERE Education IS NOT NULL AND Marital_Status IS NOT NULL
    GROUP BY Education, Marital_Status
    ORDER BY Education, Marital_Status
    ```
* **Hasil:** Tabel agregat yang menghitung jumlah pelanggan untuk setiap kombinasi pendidikan dan status pernikahan.
* **Grafik:**
    ![Grafik Komposisi Demografis](final_visualizations/demographic_composition.png)
* **Interpretasi Insight:** *Grouped bar chart* dari data ini dapat mengungkap segmen demografis yang dominan. Misalnya, jika ditemukan bahwa segmen "Sarjana & Menikah" sangat besar, maka penawaran liburan keluarga bisa menjadi strategi yang sangat efektif.

### 10. Aktivitas Regional
* **Pertanyaan:** Provinsi manakah yang mencatatkan total jumlah penerbangan paling banyak?
* **Query SQL:**
    ```sql
    SELECT
        Province,
        SUM(Total_Flights) as Total_Flights_Per_Province
    FROM merged_view
    WHERE Province IS NOT NULL
    GROUP BY Province
    ORDER BY Total_Flights_Per_Province DESC
    ```
* **Hasil:** Tabel peringkat provinsi berdasarkan total penerbangan.
* **Grafik:**
    ![Grafik Aktivitas Regional](final_visualizations/regional_activity.png)
* **Interpretasi Insight:** Ini secara langsung menunjukkan pasar geografis terpenting. Provinsi dengan total penerbangan tertinggi adalah area kunci di mana investasi pemasaran, peningkatan layanan bandara, dan frekuensi penerbangan harus diprioritaskan.

### 11. Nilai Penukaran Poin per Wilayah
* **Pertanyaan:** Berapa total nilai dolar dari poin yang ditukarkan untuk setiap provinsi?
* **Query SQL:**
    ```sql
    SELECT
        Province,
        SUM(Dollar_Cost_Points_Redeemed) as Total_Redemption_Value
    FROM merged_view
    WHERE Province IS NOT NULL AND Dollar_Cost_Points_Redeemed > 0
    GROUP BY Province
    ORDER BY Total_Redemption_Value DESC
    ```
* **Hasil:** Peringkat provinsi berdasarkan total nilai penukaran poin.
* **Grafik:**
    ![Grafik Nilai Penukaran Poin per Wilayah](final_visualizations/regional_redemption.png)
* **Interpretasi Insight:** Analisis ini menunjukkan di mana pelanggan paling aktif menggunakan poin mereka. Jika suatu wilayah memiliki aktivitas penerbangan tinggi tetapi nilai penukaran rendah, ini bisa menjadi peluang untuk meluncurkan kampanye promosi penukaran poin di wilayah tersebut.

### 12. Demografi Finansial
* **Pertanyaan:** Berapakah rata-rata gaji pelanggan berdasarkan status pernikahan?
* **Query SQL:**
    ```sql
    SELECT
        Marital_Status,
        AVG(Salary) as Average_Salary
    FROM merged_view
    WHERE Marital_Status IS NOT NULL AND Salary IS NOT NULL
    GROUP BY Marital_Status
    ORDER BY Average_Salary DESC
    ```
* **Hasil:** Tabel yang menunjukkan rata-rata gaji untuk setiap status pernikahan.
* **Grafik:**
    ![Grafik Demografi Finansial](final_visualizations/avg_salary_by_marital_status.png)
* **Interpretasi Insight:** Membantu memahami daya beli dari segmen demografis yang berbeda. Jika pelanggan yang 'Menikah' memiliki rata-rata pendapatan tertinggi, ini memperkuat ide untuk menargetkan mereka dengan produk-produk bernilai tambah tinggi.

### 13. Komposisi Pelanggan Berdasarkan Jenis Kelamin
* **Pertanyaan:** Bagaimana proporsi pelanggan berdasarkan jenis kelamin (`Gender`)?
* **Query SQL:**
    ```sql
    SELECT
        Gender,
        COUNT(DISTINCT Loyalty_Number) as Number_of_Customers
    FROM merged_view
    WHERE Gender IS NOT NULL
    GROUP BY Gender
    ```
* **Hasil:** Jumlah pelanggan untuk setiap jenis kelamin.
* **Grafik:**
    ![Grafik Komposisi Pelanggan Berdasarkan Jenis Kelamin](final_visualizations/gender_composition.png)
* **Interpretasi Insight:** *Pie chart* dari data ini memberikan gambaran cepat tentang komposisi gender pelanggan. Informasi ini dapat digunakan untuk menyesuaikan gaya komunikasi dan visual dalam materi pemasaran agar lebih relevan bagi audiens mayoritas.

### 14. Keterlibatan Tier Loyalitas
* **Pertanyaan:** Manakah jenis kartu loyalitas (`Loyalty_Card`) yang anggotanya secara kolektif menempuh total jarak (`Distance`) penerbangan paling jauh?
* **Query SQL:**
    ```sql
    SELECT
        Loyalty_Card,
        SUM(Distance) as Total_Distance_Flown
    FROM merged_view
    WHERE Loyalty_Card IS NOT NULL
    GROUP BY Loyalty_Card
    ORDER BY Total_Distance_Flown DESC
    ```
* **Hasil:** Peringkat jenis kartu loyalitas berdasarkan total jarak terbang.
* **Grafik:**
    ![Grafik Keterlibatan Tier Loyalitas](final_visualizations/tier_engagement_by_distance.png)
* **Interpretasi Insight:** Analisis ini memvalidasi efektivitas program tingkatan (tier) loyalitas. Jika anggota dengan status kartu lebih tinggi (misal: 'Aurora') memang terbang lebih jauh secara signifikan, ini menunjukkan bahwa program insentif untuk mencapai tier yang lebih tinggi berhasil mendorong perilaku pelanggan yang diinginkan.
