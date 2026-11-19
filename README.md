# Pipeline Monitoring dan Alerting dengan Apache Airflow

Praktikum 9 mata kuliah Praktikum Data Processing Workflow (D4 Sains Data Terapan, PENS):
memantau pipeline ETL transaksi penjualan dengan Airflow UI, metrik execution duration, latency,
dan failure rate, serta mengirim alert lewat callback dan webhook Discord. Mengikuti modul
[PANDUAN_PRAKTIKUM_PIPELINE_MONITORING_ALERTING.md](https://github.com/isbatuzzin/Pemrosesan-data/tree/main/Monitoring)
beserta starter kit `pipeline_monitoring_practicum.zip`.

```text
transactions.csv → extract_data → transform_data → load_data → evaluate_metrics → output + metrik
                      (event task) → callback Airflow → notification_utils → Slack / Discord
```

## Lingkungan

| Komponen | Versi |
|---|---|
| Sistem operasi | Windows 11, PowerShell |
| Docker Desktop / Docker Compose | 29.9.2 / v5.6.1 |
| Apache Airflow | 2.10.5 (`apache/airflow:2.10.5-python3.11`), LocalExecutor |
| Metadata database | PostgreSQL 13 |
| Notifikasi | Discord Incoming Webhook (Slack tidak dipakai) |

`docker-compose.yaml`, `Dockerfile`, dan `requirements.txt` berasal dari starter kit modul tanpa perubahan.

## Struktur Repositori

```text
.
├── docker-compose.yaml        compose dari modul: postgres, airflow-init, webserver, scheduler
├── Dockerfile, requirements.txt
├── .env.example               AIRFLOW_UID dan URL webhook (salin menjadi .env, jangan di-commit)
├── dags/
│   ├── etl_monitoring_demo.py DAG empat task, metrik, dan callback
│   └── notification_utils.py  pengirim webhook, callback, dan pencatat histori run
├── scripts/
│   └── analyze_run_history.py failure rate dari histori run
├── data/input/                transactions.csv (12 transaksi) dan run_history.csv (5 run contoh)
├── config/, plugins/          di-mount ke container
└── evidence/                  bukti eksekusi (lihat bagian Bukti)
```

## Menjalankan Ulang

Jalankan dari folder repositori di PowerShell, dengan Docker Desktop menyala dan port 8080 kosong.

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Tunggu `airflow-webserver` berstatus `(healthy)`, lalu buka `http://localhost:8080` dan login
dengan akun latihan `airflow` / `airflow`. Aktifkan DAG `etl_monitoring_demo` dan klik **Trigger**.

- Simulasi pipeline lambat: Admin → Variables, `simulate_slow_seconds` = `20`.
- Simulasi task gagal: Admin → Variables, `simulate_failure` = `true`.
- Alert Discord: isi `DISCORD_WEBHOOK_URL` di `.env`, lalu `docker compose down` dan `docker compose up -d`.
- Failure rate: `python scripts\analyze_run_history.py` (data contoh) atau
  `python scripts\analyze_run_history.py data\metrics\run_history_airflow.csv` (run asli).

Menghentikan lingkungan: `docker compose down` (riwayat run tetap di volume) atau
`docker compose down --volumes` (menghapus seluruh database latihan).

## Perubahan dari Kode Modul

Praktikum 1 sampai 9 dijalankan dengan kode asli modul. Setelah itu ada dua pengembangan:

| Berkas | Perubahan | Alasan |
|---|---|---|
| `dags/notification_utils.py` | Kegagalan kirim webhook hanya mencatat jenis error dan kode HTTP | Kode asli mencatat objek exception `requests` yang memuat URL webhook; saat diuji, host dan path URL tercetak di empat baris log |
| `dags/notification_utils.py`, `dags/etl_monitoring_demo.py` | `record_run_history` mencatat setiap run ke `data/metrics/run_history_airflow.csv` lewat callback DAG (`on_failure_callback` dan `on_success_callback`) | Failure rate bisa dihitung dari run nyata, bukan hanya data contoh |
| `scripts/analyze_run_history.py` | Argumen path opsional dan pengaman pembagian nol | Bisa membaca histori run asli dan tidak berhenti bila belum ada run sukses |

## Ringkasan Hasil (19 November 2026)

| Uji | Hasil |
|---|---|
| `airflow-init` dan layanan | Exit code 0 setelah 33 detik; webserver healthy setelah 73 detik |
| Run normal | Empat task success dalam 5 detik; 12 record, total revenue Rp7.450.000; execution duration 3,84 detik |
| Latency | Rata-rata 635 detik, maksimum 1.080 detik; lima record melewati ambang 600 detik |
| Pipeline lambat (`simulate_slow_seconds=20`) | Durasi 23,24 detik, `duration_alert` false (ambang 900 detik) |
| Ambang durasi 10 detik | Durasi 24,09 detik, `duration_alert` true |
| Failure rate data contoh | 20,00%; 40,00% bila run 2 diubah gagal; 16,67% bila run `running` ikut dihitung |
| Simulasi task gagal | `transform_data` gagal dua kali (jeda retry 30 detik); dua task hilir `upstream_failed`; `DAG failure event` di log scheduler |
| Webhook Discord | WARNING saat retry, CRITICAL saat gagal final, dan WARNING latency terkirim (HTTP 204) |
| Webhook gagal, kode asli | Host dan path URL Slack tidak valid tercetak di log task |
| Webhook gagal, setelah perbaikan | Log hanya berisi `ConnectionError`; Discord tetap HTTP 204 |
| Failure rate run asli | 3 run (1 gagal, 2 sukses): 33,33% |

## Bukti

| Folder | Isi |
|---|---|
| `evidence/ui/` | Screenshot asli Airflow UI dan kanal Discord. Nama akun Discord pada `discord_webhook.png` disamarkan |
| `evidence/raw/` | Transkrip asli perintah PowerShell beserta output-nya |
| `evidence/render/` | Gambar terminal dan kode untuk laporan, disusun ulang dari transkrip dan berkas kode; baris yang tidak ditampilkan ditandai |
| `evidence/hasil/` | Salinan output run terakhir (`.gitignore` modul mengabaikan `data/output` dan `data/metrics`) |

URL webhook Discord hanya ada di `.env` lokal dan tidak pernah di-commit; tampilan `.env` di bukti selalu tersamar.
