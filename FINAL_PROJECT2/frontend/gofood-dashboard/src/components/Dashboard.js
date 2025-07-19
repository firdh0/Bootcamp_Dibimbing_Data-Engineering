// frontend/src/components/Dashboard.js
import React, { useState, useEffect } from 'react';
import './Dashboard.css'; 

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line, Scatter } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

const formatRupiah = (amount) => {
  if (amount === null || amount === undefined) {
    return 'N/A';
  }
  const numericAmount = typeof amount === 'string' ? parseFloat(amount) : amount;
  if (isNaN(numericAmount)) {
    return 'N/A';
  }
  const scaledAmount = numericAmount * 1000; 
  return new Intl.NumberFormat('id-ID', {
    style: 'currency',
    currency: 'IDR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(scaledAmount);
};

function Dashboard() {
  const [locationData, setLocationData] = useState(null);
  const [reviewsByDate, setReviewsByDate] = useState([]);
  const [greatestSavingsPromos, setGreatestSavingsPromos] = useState([]);
  const [cheapestRestaurants, setCheapestRestaurants] = useState([]);
  const [ratingCorrelationData, setRatingCorrelationData] = useState([]);
  
  const [userBudget, setUserBudget] = useState('');
  const [bestValueRecommendations, setBestValueRecommendations] = useState([]);
  const [recoLoading, setRecoLoading] = useState(false);
  const [recoError, setRecoError] = useState(null);

  const [loading, setLoading] = useState(true); 
  const [error, setError] = useState(null);     

  useEffect(() => {
    if ("geolocation" in navigator) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          setLocationData({
            latitude: position.coords.latitude,
            longitude: position.coords.longitude,
          });
          console.log("Lokasi Pengguna:", position.coords);
        },
        (err) => {
          console.error("Error mengambil lokasi:", err);
          setError("Gagal mengambil lokasi. Pastikan izin lokasi diaktifkan.");
        }
      );
    } else {
      setError("Geolocation tidak didukung di browser ini.");
    }
  }, []);

  const fetchBestValueRecommendations = async () => {
    setRecoLoading(true);
    setRecoError(null);
    try {
      const response = await fetch(`/api/best-value-recommendations?budget=${userBudget}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setBestValueRecommendations(data);
    } catch (error) {
      console.error("Gagal mengambil rekomendasi best value:", error);
      setRecoError("Gagal memuat rekomendasi. Coba lagi.");
    } finally {
      setRecoLoading(false);
    }
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true); 

        const reviewsRes = await fetch('/api/reviews-by-date'); 
        const reviewsData = await reviewsRes.json();
        setReviewsByDate(reviewsData);

        const savingsRes = await fetch('/api/greatest-savings-promos');
        const savingsData = await savingsRes.json();
        setGreatestSavingsPromos(savingsData);

        const cheapestRes = await fetch('/api/cheapest-restaurants?categories=ayam%20geprek,kopi%20susu,nasi%20padang');
        const cheapestData = await cheapestRes.json();
        setCheapestRestaurants(cheapestData);

        const correlationRes = await fetch('/api/rating-correlation-data');
        const correlationData = await correlationRes.json();
        setRatingCorrelationData(correlationData);

      } catch (error) {
        console.error("Gagal mengambil data dari BigQuery API:", error);
        setError("Gagal memuat data dari server.");
      } finally {
        setLoading(false); 
      }
    };

    fetchData();
  }, []); 

  // --- Data dan Opsi untuk Chart.js ---
  // MODIFIKASI: Tambahkan pemeriksaan reviewsByDate.length > 0 sebelum memetakan data
  const reviewsChartData = {
    labels: reviewsByDate.length > 0 ? reviewsByDate.map(d => d.date) : [],
    datasets: [
      {
        label: 'Jumlah Ulasan',
        data: reviewsByDate.length > 0 ? reviewsByDate.map(d => d.review_count) : [],
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 1,
      },
    ],
  };
  const reviewsChartOptions = {
    responsive: true,
    plugins: {
      title: {
        display: true,
        text: 'Jumlah Ulasan per Tanggal',
      },
      legend: {
        position: 'top',
      },
      tooltip: {
        mode: 'index',
        intersect: false,
      },
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Tanggal',
        },
      },
      y: {
        title: {
          display: true,
          text: 'Jumlah Ulasan',
        },
        beginAtZero: true,
      },
    },
  };

  // MODIFIKASI: Tambahkan pemeriksaan cheapestRestaurants.length > 0
  const cheapestRestaurantsChartData = {
    labels: cheapestRestaurants.length > 0 ? cheapestRestaurants.map(r => `${r.Nama_Menu} (${r.Nama_Restoran})`) : [],
    datasets: [
      {
        label: 'Harga Rata-rata (Rp)',
        data: cheapestRestaurants.length > 0 ? cheapestRestaurants.map(r => r.Avg_Price_Per_Menu_Resto) : [],
        backgroundColor: 'rgba(153, 102, 255, 0.6)',
        borderColor: 'rgba(153, 102, 255, 1)',
        borderWidth: 1,
      },
    ],
  };
  const cheapestRestaurantsChartOptions = {
    responsive: true,
    plugins: {
      title: {
        display: true,
        text: 'Harga Rata-rata Menu Termurah per Restoran',
      },
      legend: {
        position: 'top',
      },
      tooltip: {
        mode: 'index',
        intersect: false,
      },
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Menu (Restoran)',
        },
      },
      y: {
        title: {
          display: true,
          text: 'Harga Rata-rata (Rp)',
        },
        beginAtZero: true,
      },
    },
  };

  // MODIFIKASI: Tambahkan pemeriksaan ratingCorrelationData.length > 0
  const ratingCorrelationChartData = {
    datasets: [
      {
        label: 'Rating vs Harga Menu Rata-rata',
        data: ratingCorrelationData.length > 0 ? ratingCorrelationData.map(d => ({
          x: d.avg_rating,
          y: d.Avg_Menu_Price,
          name: d.Nama_Restoran,
          rating_tier: d.Rating_Tier
        })) : [],
        backgroundColor: 'rgba(255, 99, 132, 0.6)',
        borderColor: 'rgba(255, 99, 132, 1)',
        pointRadius: 5,
        pointHoverRadius: 7,
      },
    ],
  };
  const ratingCorrelationChartOptions = {
    responsive: true,
    plugins: {
      title: {
        display: true,
        text: 'Korelasi Rating Restoran dengan Harga Menu Rata-rata',
      },
      legend: {
        position: 'top',
      },
      tooltip: {
        callbacks: {
          label: function(context) {
            const label = context.dataset.label || '';
            if (context.raw) {
              return `${context.raw.name}: Rating ${context.raw.x}, Harga ${formatRupiah(context.raw.y)}`;
            }
            return label;
          }
        }
      }
    },
    scales: {
      x: {
        type: 'linear',
        position: 'bottom',
        title: {
          display: true,
          text: 'Rating Rata-rata Restoran',
        },
        min: 0,
        max: 5,
      },
      y: {
        type: 'linear',
        position: 'left',
        title: {
          display: true,
          text: 'Harga Menu Rata-rata (Rp)',
        },
        beginAtZero: true,
      },
    },
  };


  if (loading) return <p>Memuat data visualisasi...</p>;
  if (error) return <p style={{ color: 'red' }}>Error: {error}</p>;

  return (
    <div className="dashboard-container">
      <h2>Ringkasan Data GoFood</h2>

      {/* Bagian Data Lokasi Pengguna */}
      <div className="card">
        <h3>Data Lokasi Pengguna:</h3>
        {locationData ? (
          <p>
            Latitude: {locationData.latitude}, Longitude: {locationData.longitude}
          </p>
        ) : (
          <p>Lokasi tidak tersedia atau izin diblokir.</p>
        )}
      </div>

      {/* Bagian Baru: Rekomendasi "Best Value" berdasarkan Budget */}
      <div className="card">
        <h3>Rekomendasi "Best Value" (Sesuai Budget Anda):</h3>
        <div className="form-group">
          <label htmlFor="budget-input">Masukkan Budget Makanan (Rp):</label>
          <input
            type="number"
            id="budget-input"
            value={userBudget}
            onChange={(e) => setUserBudget(e.target.value)}
            placeholder="Contoh: 50000"
            min="0"
          />
          <button onClick={fetchBestValueRecommendations} disabled={recoLoading || !userBudget}>
            {recoLoading ? 'Memuat...' : 'Cari Rekomendasi'}
          </button>
        </div>
        {recoError && <p style={{ color: 'red' }}>Error: {recoError}</p>}
        {bestValueRecommendations.length > 0 ? (
          <div className="table-responsive">
            <table>
              <thead>
                <tr>
                  <th>Nama Menu</th>
                  <th>Restoran</th>
                  <th>Kategori</th>
                  <th>Harga Dasar (Rp)</th>
                  <th>Nama Promo</th>
                  <th>Harga Setelah Promo (Rp)</th>
                  <th>Total Penghematan (Rp)</th>
                  <th>Rating Restoran</th>
                </tr>
              </thead>
              <tbody>
                {bestValueRecommendations.map((item, index) => (
                  <tr key={index}>
                    <td>{item.Nama_Menu}</td>
                    <td>{item.Nama_Restoran}</td>
                    <td>{item.Kategori_Menu}</td>
                    <td>{formatRupiah(item.Base_Price_Menu_Resto)}</td> 
                    <td>{item.Nama_Promo || 'Tidak Ada Promo'}</td>
                    <td>{formatRupiah(item.Price_After_Food_Promo)}</td> 
                    <td>{formatRupiah(item.Total_Savings)}</td> 
                    <td>{item.Rating_Restoran || 'N/A'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          !recoLoading && userBudget && <p>Tidak ada rekomendasi yang sesuai dengan budget Anda.</p>
        )}
      </div>

      {/* Bagian Visualisasi: Ulasan per Tanggal */}
      <div className="card">
        <h3>Ulasan per Tanggal:</h3>
        {reviewsByDate.length > 0 ? (
          <div className="chart-container">
            <Bar data={reviewsChartData} options={reviewsChartOptions} />
          </div>
        ) : (
          <p>Tidak ada data ulasan per tanggal.</p>
        )}
      </div>

      {/* Bagian Visualisasi: Promosi dengan Penghematan Terbesar */}
      <div className="card">
        <h3>Promosi dengan Penghematan Terbesar:</h3>
        {greatestSavingsPromos.length > 0 ? (
          <div className="table-responsive">
            <table>
              <thead>
                <tr>
                  <th>Nama Promo</th>
                  <th>Nama Restoran</th>
                  <th>Penghematan Nominal (Rp)</th>
                  <th>Persentase Diskon</th>
                </tr>
              </thead>
              <tbody>
                {greatestSavingsPromos.map((promo, index) => (
                  <tr key={index}>
                    <td>{promo.Nama_Promo}</td>
                    <td>{promo.Nama_Restoran}</td>
                    <td>{formatRupiah(promo.Potensi_Penghematan_Nominal)}</td> 
                    <td>{promo.Persentase_Diskon_Promo ? (promo.Persentase_Diskon_Promo * 100).toFixed(0) + '%' : 'N/A'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p>Tidak ada data promosi penghematan terbesar.</p>
        )}
      </div>

      {/* Bagian Visualisasi: Restoran Termurah per Kategori */}
      <div className="card">
        <h3>Restoran Termurah per Kategori Makanan Populer:</h3>
        {cheapestRestaurants.length > 0 ? (
          <div className="chart-container">
            <Bar data={cheapestRestaurantsChartData} options={cheapestRestaurantsChartOptions} />
            <div className="table-responsive">
              <table>
                <thead>
                  <tr>
                    <th>Nama Menu</th>
                    <th>Kategori</th>
                    <th>Restoran Termurah</th>
                    <th>Harga Rata-rata (Rp)</th>
                    <th>Kota</th>
                  </tr>
                </thead>
                <tbody>
                  {cheapestRestaurants.map((resto, index) => (
                    <tr key={index}>
                      <td>{resto.Nama_Menu}</td>
                      <td>{resto.Kategori_Menu}</td>
                      <td>{resto.Nama_Restoran}</td>
                      <td>{formatRupiah(resto.Avg_Price_Per_Menu_Resto)}</td> 
                      <td>{resto.Kota_Restoran}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <p>Tidak ada data restoran termurah untuk kategori yang dipilih.</p>
        )}
      </div>

      {/* Bagian Visualisasi: Data Korelasi Rating, Harga, Promosi */}
      <div className="card">
        <h3>Korelasi Rating Restoran dengan Harga dan Promosi:</h3>
        {ratingCorrelationData.length > 0 ? (
          <div className="chart-container">
            <Scatter data={ratingCorrelationChartData} options={ratingCorrelationChartOptions} />
            <div className="table-responsive">
              <table>
                <thead>
                  <tr>
                    <th>Nama Restoran</th>
                    <th>Rating</th>
                    <th>Avg Harga Menu (Rp)</th>
                    <th>Max Diskon %</th>
                    <th>Jumlah Promo Aktif</th>
                    <th>Rating Tier</th>
                  </tr>
                </thead>
                <tbody>
                  {ratingCorrelationData.map((data, index) => (
                    <tr key={index}>
                      <td>{data.Nama_Restoran}</td>
                      <td>{data.avg_rating}</td>
                      <td>{formatRupiah(data.Avg_Menu_Price)}</td> 
                      <td>{data.Max_Active_Discount_Percentage ? (data.Max_Active_Discount_Percentage * 100).toFixed(0) + '%' : 'N/A'}</td>
                      <td>{data.Active_Promo_Count}</td>
                      <td>{data.Rating_Tier}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p>
              * Analisis korelasi lebih mendalam (koefisien korelasi) dapat dilakukan di backend atau tools BI.
            </p>
          </div>
        ) : (
          <p>Tidak ada data korelasi rating.</p>
        )}
      </div>
    </div>
  );
}

export default Dashboard;