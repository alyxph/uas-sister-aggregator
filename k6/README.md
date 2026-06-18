# K6 Load Testing untuk Pub-Sub Log Aggregator

Skrip ini mensimulasikan beberapa pengirim (*Virtual Users / VUs*) yang mengirimkan logs/events ke `POST /publish` secara bersamaan. Pengujian ini memastikan sistem mampu menangani beban tinggi dan fitur *deduplication* bekerja secara efektif di bawah situasi konkurensi.

## Instalasi

K6 harus diinstal pada mesin lokal Anda. Lihat [panduan instalasi K6 resmi](https://k6.io/docs/get-started/installation/).

## Menjalankan Pengujian

1. Pastikan seluruh sistem aggregator berjalan:
   ```bash
   docker compose up -d
   ```

2. Jalankan skrip K6 dari direktori utama project (bukan dari dalam k6/):
   ```bash
   k6 run k6/load_test.js
   ```

## Metrik Kustom

Skrip K6 mencatat beberapa metrik spesifik project ini:
- `events_published`: Total events yang berhasil dikirim ke API.
- `duplicates_sent`: Sebagian dari events tersebut (~35%) sengaja dikirim sebagai duplikat untuk menguji *idempotency*.
- `publish_latency`: Waktu yang dibutuhkan dari `POST /publish` hingga menerima respon HTTP 202.

Pada akhir tes, K6 akan mengambil statistik terbaru dari `/stats` dan menampilkannya, memastikan `unique_processed` dan `duplicate_dropped` ditangani dengan benar.
