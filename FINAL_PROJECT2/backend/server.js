// backend/server.js

// Muat variabel lingkungan dari file .env
require('dotenv').config(); 

const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors'); 

const app = express();
const port = process.env.PORT || 3001; 

app.use(cors({ origin: 'http://localhost:3000' })); 
app.use(express.json()); 

const bigquery = new BigQuery({
  projectId: process.env.BIGQUERY_PROJECT_ID, 
});

console.log(`Backend API akan terhubung ke BigQuery Project: ${process.env.BIGQUERY_PROJECT_ID}`);
console.log(`BigQuery Dataset: ${process.env.BIGQUERY_DATASET_ID}`);

// === Endpoint API untuk Mengambil Data Ulasan per Tanggal ===
app.get('/api/reviews-by-date', async (req, res) => {
  const query = `
    SELECT
      CAST(date AS STRING) AS date, 
      COUNT(id_time) AS review_count
    FROM
      \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_date\` 
    GROUP BY
      date
    ORDER BY
      date
    LIMIT 100;
  `;

  try {
    const [rows] = await bigquery.query({
      query: query,
      location: 'asia-southeast2', 
    });
    console.log(`Berhasil mengambil ${rows.length} baris dari dim_date.`);
    res.json(rows);
  } catch (error) {
    console.error("ERROR mengambil data dari /api/reviews-by-date:", error);
    res.status(500).json({ 
      error: 'Gagal mengambil data dari BigQuery untuk reviews-by-date.',
      details: error.message 
    });
  }
});

// === Endpoint API untuk 1. Promosi dengan Penghematan Terbesar ===
app.get('/api/greatest-savings-promos', async (req, res) => {
    const query = `
        SELECT
            dp.name AS Nama_Promo,
            dp.description AS Detail_Promo,
            dr.name AS Nama_Restoran,
            dr.avg_rating AS Rating_Restoran,
            dp.discount_percentage AS Persentase_Diskon_Promo,
            dp.max_discount AS Maksimal_Diskon_Nominal,
            dp.min_purchase AS Minimum_Pembelian,
            dp.additional_discount AS Diskon_Tambahan_Makanan,
            dp.delivery_discount AS Diskon_Ongkir,
            
            (SELECT AVG(ft_inner.original_price) 
             FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.fact_transaction\` ft_inner 
             WHERE ft_inner.id_restaurant = dr.id_restaurant
            ) AS Avg_Menu_Price_Resto,

            LEAST(
                COALESCE( (SELECT AVG(ft_inner.original_price) FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.fact_transaction\` ft_inner WHERE ft_inner.id_restaurant = dr.id_restaurant) * dp.discount_percentage, 0),
                COALESCE(dp.max_discount, 9999999.0)
            ) + COALESCE(dp.additional_discount, 0.0) + COALESCE(dp.delivery_discount, 0.0)
            AS Potensi_Penghematan_Nominal
        FROM
            \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_promotion\` dp
        JOIN
            \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_restaurant\` dr ON dp.name LIKE CONCAT('%', dr.name, '%')
        WHERE
            dp.status = 'aktif'
        ORDER BY
            Potensi_Penghematan_Nominal DESC
        LIMIT 20;
    `;

    try {
        const [rows] = await bigquery.query({
            query: query,
            location: 'asia-southeast2',
        });
        console.log(`Berhasil mengambil ${rows.length} promosi dengan penghematan terbesar.`);
        res.json(rows);
    } catch (error) {
        console.error("ERROR mengambil data dari /api/greatest-savings-promos:", error);
        res.status(500).json({ 
            error: 'Gagal mengambil data promosi dengan penghematan terbesar.',
            details: error.message 
        });
    }
});

// === Endpoint API untuk 2. Pola Distribusi Harga & Restoran Termurah ===
app.get('/api/cheapest-restaurants', async (req, res) => {
    const categories = req.query.categories ? req.query.categories.split(',') : ['ayam geprek', 'kopi susu', 'nasi padang']; 
    const query = `
        WITH MenuPriceStats AS (
            SELECT
                dm.name AS Nama_Menu,
                dm.category AS Kategori_Menu,
                dr.name AS Nama_Restoran,
                dr.city AS Kota_Restoran,
                ft.original_price
            FROM
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.fact_transaction\` ft
            JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_menu\` dm ON ft.id_menu = dm.id_menu
            JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_restaurant\` dr ON ft.id_restaurant = dr.id_restaurant
            WHERE
                dm.name IN UNNEST(${JSON.stringify(categories)})
                AND ft.original_price IS NOT NULL AND ft.original_price > 0
        ),
        RestaurantAvgPrices AS (
            SELECT
                Nama_Restoran,
                Kota_Restoran,
                Nama_Menu,
                Kategori_Menu,
                AVG(original_price) AS Avg_Price_Per_Menu_Resto
            FROM
                MenuPriceStats
            GROUP BY
                Nama_Restoran, Kota_Restoran, Nama_Menu, Kategori_Menu
        ),
        RankedRestaurantPrices AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY Nama_Menu, Kota_Restoran ORDER BY Avg_Price_Per_Menu_Resto ASC) AS Rank_Harga_Murah
            FROM
                RestaurantAvgPrices
        )
        SELECT
            r.Nama_Restoran,
            r.Kota_Restoran,
            r.Nama_Menu,
            r.Kategori_Menu,
            r.Avg_Price_Per_Menu_Resto
        FROM
            RankedRestaurantPrices r
        WHERE
            r.Rank_Harga_Murah = 1 
        ORDER BY
            r.Kategori_Menu, r.Kota_Restoran, r.Avg_Price_Per_Menu_Resto ASC
        LIMIT 50;
    `;

    try {
        const [rows] = await bigquery.query({
            query: query,
            location: 'asia-southeast2',
        });
        console.log(`Berhasil mengambil ${rows.length} restoran termurah.`);
        res.json(rows);
    } catch (error) {
        console.error("ERROR mengambil data dari /api/cheapest-restaurants:", error);
        res.status(500).json({ 
            error: 'Gagal mengambil data restoran termurah.',
            details: error.message 
        });
    }
});

// === Endpoint API untuk 3. Korelasi Rating dengan Harga dan Promosi ===
app.get('/api/rating-correlation-data', async (req, res) => {
    const query = `
        WITH RestaurantAggregates AS (
            SELECT
                dr.id_restaurant,
                dr.name AS Nama_Restoran,
                dr.avg_rating,
                AVG(ft.original_price) AS Avg_Menu_Price,
                MAX(CASE WHEN dp.status = 'aktif' THEN dp.discount_percentage ELSE 0 END) AS Max_Active_Discount_Percentage,
                MAX(CASE WHEN dp.status = 'aktif' THEN dp.max_discount ELSE 0 END) AS Max_Active_Discount_Nominal,
                COUNT(DISTINCT CASE WHEN dp.status = 'aktif' THEN dp.id_promo ELSE NULL END) AS Active_Promo_Count
            FROM
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_restaurant\` dr
            LEFT JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.fact_transaction\` ft ON dr.id_restaurant = ft.id_restaurant
            LEFT JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_promotion\` dp ON dr.name = dp.name 
            GROUP BY
                dr.id_restaurant, dr.name, dr.avg_rating
            HAVING
                dr.avg_rating IS NOT NULL
                AND AVG(ft.original_price) IS NOT NULL
        )
        SELECT
            Nama_Restoran,
            avg_rating,
            Avg_Menu_Price,
            Max_Active_Discount_Percentage,
            Max_Active_Discount_Nominal,
            Active_Promo_Count,
            CASE
                WHEN avg_rating >= 4.5 THEN 'Rating_Tinggi'
                WHEN avg_rating >= 4.0 THEN 'Rating_Menengah'
                ELSE 'Rating_Rendah'
            END AS Rating_Tier
        FROM
            RestaurantAggregates
        ORDER BY
            avg_rating DESC
        LIMIT 100;
    `;

    try {
        const [rows] = await bigquery.query({
            query: query,
            location: 'asia-southeast2',
        });
        console.log(`Berhasil mengambil ${rows.length} data korelasi rating.`);
        res.json(rows);
    } catch (error) {
        console.error("ERROR mengambil data dari /api/rating-correlation-data:", error);
        res.status(500).json({ 
            error: 'Gagal mengambil data korelasi rating.',
            details: error.message 
        });
    }
});

// === Endpoint API Baru: Rekomendasi "Best Value" berdasarkan Budget ===
app.get('/api/best-value-recommendations', async (req, res) => {
    const userBudget = parseFloat(req.query.budget); 
    const foodCategory = req.query.category || null; 

    if (isNaN(userBudget) || userBudget <= 0) {
        return res.status(400).json({ error: 'Budget yang valid diperlukan.' });
    }

    const query = `
        WITH MenuBasePrices AS (
            SELECT
                dm.id_menu,
                dm.name AS Nama_Menu,
                dm.category AS Kategori_Menu,
                dr.id_restaurant,
                dr.name AS Nama_Restoran,
                dr.city AS Kota_Restoran,
                dr.avg_rating AS Rating_Restoran,
                AVG(ft.original_price) AS Base_Price_Menu_Resto -- Harga dasar menu di restoran ini (nilai penuh)
            FROM
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.fact_transaction\` ft
            JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_menu\` dm ON ft.id_menu = dm.id_menu
            JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_restaurant\` dr ON ft.id_restaurant = dr.id_restaurant
            WHERE
                ft.original_price IS NOT NULL AND ft.original_price > 0
            GROUP BY
                dm.id_menu, dm.name, dm.category, dr.id_restaurant, dr.name, dr.city, dr.avg_rating
        ),
        ActivePromosForRestaurants AS (
            SELECT
                dp.id_promo,
                dp.name AS Nama_Promo, 
                dp.description AS Detail_Promo,
                dp.discount_percentage,
                dp.max_discount,
                dp.min_purchase,
                dp.additional_discount,
                dp.delivery_discount,
                dr.id_restaurant,
                dr.name AS Nama_Restoran 
            FROM
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_promotion\` dp
            JOIN
                \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.dim_restaurant\` dr ON dp.name LIKE CONCAT('%', dr.name, '%') 
            WHERE
                dp.status = 'aktif'
        ),
        MenuWithEffectivePrice AS (
            SELECT
                mbp.id_menu,
                mbp.Nama_Menu,
                mbp.Kategori_Menu,
                mbp.id_restaurant,
                mbp.Nama_Restoran,
                mbp.Kota_Restoran,
                mbp.Rating_Restoran,
                mbp.Base_Price_Menu_Resto, -- Harga dasar penuh
                apr.id_promo,
                apr.Nama_Promo,
                apr.discount_percentage,
                apr.max_discount,
                apr.min_purchase,
                apr.additional_discount,
                apr.delivery_discount,
                
                -- Hitung harga efektif setelah diskon makanan (dalam nilai penuh)
                (mbp.Base_Price_Menu_Resto - 
                 LEAST(mbp.Base_Price_Menu_Resto * COALESCE(apr.discount_percentage, 0), COALESCE(apr.max_discount, mbp.Base_Price_Menu_Resto)) - 
                 COALESCE(apr.additional_discount, 0)
                ) AS Price_After_Food_Promo, -- Ini yang akan difilter dan dikirim ke frontend

                -- Hitung total penghematan (dalam nilai penuh)
                (LEAST(mbp.Base_Price_Menu_Resto * COALESCE(apr.discount_percentage, 0), COALESCE(apr.max_discount, mbp.Base_Price_Menu_Resto)) + 
                 COALESCE(apr.additional_discount, 0) + 
                 COALESCE(apr.delivery_discount, 0)
                ) AS Total_Savings
                
            FROM
                MenuBasePrices mbp
            LEFT JOIN 
                ActivePromosForRestaurants apr ON mbp.id_restaurant = apr.id_restaurant AND mbp.Base_Price_Menu_Resto >= COALESCE(apr.min_purchase, 0)
        ),
        RankedEffectivePrices AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY id_menu, id_restaurant ORDER BY Total_Savings DESC, Price_After_Food_Promo ASC) AS rn 
            FROM
                MenuWithEffectivePrice
        )
        SELECT
            id_menu,
            Nama_Menu,
            Kategori_Menu,
            id_restaurant,
            Nama_Restoran,
            Kota_Restoran,
            Rating_Restoran,
            Base_Price_Menu_Resto, -- Tetap dalam nilai penuh
            id_promo,
            Nama_Promo,
            Price_After_Food_Promo, 
            Total_Savings
        FROM
            RankedEffectivePrices
        WHERE
            rn = 1 
            AND Price_After_Food_Promo <= ${userBudget} -- Filter menggunakan harga penuh (sesuai input user)
            ${foodCategory ? `AND Kategori_Menu = '${foodCategory}'` : ''} 
        ORDER BY
            Price_After_Food_Promo ASC, 
            Rating_Restoran DESC,       
            Total_Savings DESC          
        LIMIT 50; 
    `;

    try {
        const [rows] = await bigquery.query({
            query: query,
            location: 'asia-southeast2',
        });
        console.log(`Berhasil mengambil ${rows.length} rekomendasi best value untuk budget Rp${userBudget}.`);
        res.json(rows);
    } catch (error) {
        console.error("ERROR mengambil data dari /api/best-value-recommendations:", error);
        res.status(500).json({ 
            error: 'Gagal mengambil rekomendasi best value.',
            details: error.message 
        });
    }
});


// Server mulai mendengarkan request
app.listen(port, () => {
  console.log(`Backend API berjalan di http://localhost:${port}`);
});