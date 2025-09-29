# Module 06 — AWS Medallion ETL (S3 • Glue • Athena • Redshift) với Airflow local

## 1) Mục tiêu

* Áp dụng **Medallion Architecture** (Bronze/Silver/Gold) trên S3.
* Orchestrate pipeline bằng **Airflow chạy local**; xử lý bằng **Glue**, kiểm thử bằng **Athena**, xuất bản BI trên **Redshift**.
* Chuẩn hoá dữ liệu, quản trị **schema** nhất quán, **partitioning**, **idempotency**, **data quality** có “cửa chặn”.

---

## 2) Phạm vi dữ liệu & Lưu trữ S3 (BẮT BUỘC)

* **Raw** (CSV, bất biến, đúng 5 bảng):
  `Competitions.csv`, `Users.csv`, `Datasets.csv`, `Tags.csv`, `Kernels.csv`
  Path: `s3://<bucket>/meta/raw/<table>/ingestion_date=YYYY-MM-DD/…`
* **Bronze** (Parquet + Snappy): `s3://<bucket>/meta/bronze/<table>/run_date=YYYY-MM-DD/…`
* **Silver** (Parquet + Snappy): `s3://<bucket>/meta/silver/<table>/run_date=YYYY-MM-DD/…`
* **Gold** (Parquet + Snappy):
  • Daily: `s3://<bucket>/meta/gold/<table>/run_date=YYYY-MM-DD/…`
  • Yearly: `s3://<bucket>/meta/gold/<table>/year=YYYY/…`
* **Quy ước bắt buộc:** tên cột **snake\_case**, mọi `*_ts` ở **UTC**; kiểu `*_id` **thống nhất** từ Bronze trở đi.

---

## 3) Orchestration bằng Airflow local (BẮT BUỘC)

* Lịch DAG: **02:00 Asia/Bangkok** hàng ngày; cho phép backfill.
* Chuỗi tác vụ:

  1. Kiểm tra đủ **5 bảng Raw** theo `ingestion_date`.
  2. **Bronze transform** (chuẩn hoá + validate + CSV→Parquet).
  3. **Crawler Bronze** (Glue Data Catalog).
  4. **Silver transform** (cleaning nâng cao, dedup NK).
  5. **Crawler Silver**.
  6. **Gold build** (Dims/Bridge/Facts/KPI).
  7. **Crawler Gold**.
  8. **DQ Gold** (fail ⇒ dừng publish).
  9. **Redshift publish** (DDL nếu cần → COPY dims → bridge → facts → kiểm thử hậu nạp).
* **SLA**: bước Silver→Gold < 10 phút.
* **Idempotency**: chạy lại cùng `run_date` phải ghi đè partition tương ứng.

---

## 4) Yêu cầu ETL theo lớp

### 4.1 Raw → Bronze (BẮT BUỘC)

* **Nhiệm vụ:** chuẩn hoá **snake\_case**, **xác thực schema**, **chuyển Parquet + Snappy**.
* **Hợp đồng schema tối thiểu:**

  * **Users**: `user_id` (NK), `user_name` (bắt buộc), `signup_ts?`, `country_code?`
  * **Datasets**: `dataset_id` (NK), `dataset_title` (bắt buộc), `owner_user_id` (bắt buộc), `created_ts?`, `updated_ts?`, `is_private?` (bool)
  * **Competitions**: `competition_id` (NK), `title` (bắt buộc), `category?`, `start_ts?`, `deadline_ts?`, `prize_money?` (≥0 nếu có)
  * **Tags**: `(dataset_id, tag)` (NK tổ hợp); `tag` phải `lower(trim)` và không rỗng
  * **Kernels**: `kernel_id` (NK), `author_user_id` (bắt buộc), `title` (bắt buộc), `created_ts?`, `updated_ts?`
* **Logic tối thiểu:**
  `updated_ts ≥ created_ts` (nếu cùng tồn tại); `start_ts ≤ deadline_ts` (nếu có); `country_code` dài 2; `*_id` không rỗng.
* **Reject policy:** bản ghi vi phạm **không** đi tiếp Silver; lưu tại
  `meta/bronze/_rejects/<table>/run_date=YYYY-MM-DD/…` kèm `reject_reason`.
* **Cửa chặn:** nếu **%reject > 10%** ở bất kỳ bảng nào **hoặc** thiếu cột bắt buộc trong Parquet Bronze ⇒ **fail DAG**.
* **Báo cáo Bronze:** số input/output/reject theo bảng; top-5 lý do lỗi; lưu tại
  `meta/bronze/_reports/run_date=YYYY-MM-DD/bronze_summary.json`.

### 4.2 Bronze → Silver (BẮT BUỘC)

* Tập trung vào **cleaning nâng cao** (không lặp lại chuẩn hoá đã làm ở Bronze):

  * **Dedup theo khóa tự nhiên** (`user_id`, `dataset_id`, `competition_id`, `kernel_id`, `dataset_id+tag`) chọn bản “mới nhất” (ưu tiên `updated_ts`, rồi mức độ đầy đủ).
  * **Thiếu dữ liệu:** áp dụng **tối thiểu 2 chiến lược** (drop / impute / flag) và nêu **lý do** trong báo cáo.
  * **Tags**: Silver tiêu thụ tag đã hợp lệ từ Bronze; tiếp tục explode/chuẩn hoá quan hệ N–N.
* Partition `run_date`; cập nhật Catalog Silver.

### 4.3 Silver → Gold (BẮT BUỘC)

* Xây dựng **Dims/Bridge/Facts** (đọc **chỉ từ Silver**), bổ sung `run_date`, `pipeline_run_id`, `created_at_ts`; cập nhật Catalog Gold.

---

## 5) Mô hình dữ liệu Gold (BẮT BUỘC)

### 5.1 Dimension (surrogate key, SCD)

* **SCD2:** `dim_user`, `dim_dataset`, `dim_competition`
  Thuộc tính SCD: `effective_start_ts`, `effective_end_ts`, `is_current`.
* **SCD1:** `dim_tag`, `dim_date` (`date_sk` = YYYYMMDD).
* Mỗi dim (trừ `dim_date`) phải có **hàng “Unknown”** (SK=0).

### 5.2 Bridge

* **bridge\_dataset\_tag** (N–N): PK tổng hợp (`dataset_sk`, `tag_sk`, `run_date`), có `is_current`.

### 5.3 Fact & KPI

* **fact\_dataset\_owner\_daily** (grain: **owner × run\_date**):
  `datasets_count`, `private_datasets_count`, `public_datasets_count`; **ràng buộc:** `total = private + public`.
* **fact\_competitions\_yearly** (grain: **year**):
  `competitions_count` (theo năm `start_ts`), `active_competitions_count` (hoạt động trong năm), `avg_prize`; **ràng buộc:** `competitions_count ≥ active_competitions_count`.
* **fact\_tag\_usage\_daily** (grain: **tag × run\_date**):
  `usage_count` (dataset current gắn tag), `new_usage_count` (dataset mới ngày đó có tag); **ràng buộc:** `usage_count ≥ new_usage_count`.

---

## 6) Data Quality & Governance (BẮT BUỘC)

* **Bronze:** kiểm tra schema/logic; áp dụng **cửa chặn 10%**.
* **Silver:** báo cáo dedup (trước/sau), quyết định xử lý thiếu dữ liệu và lý do.
* **Gold:**

  * Dimensions: PK/NK không NULL; SCD2 không chồng chéo; `start < end` (bản không current).
  * Bridge: FK hợp lệ; không trùng PK tổng hợp.
  * Facts: tất cả FK map được (nếu miss → SK=0, phải ghi log); các **ràng buộc cân bằng/logic** đạt 100%.
* **Fail bất kỳ rule nào ⇒ dừng publish Redshift**; xuất báo cáo DQ (JSON) vào thư mục DQ.

---

## 7) Bảo mật, Hiệu năng, Vận hành (BẮT BUỘC)

* **IAM (least privilege)** cho S3/Glue/Athena/Redshift theo prefix `meta/*`.
* **S3** bật **SSE-KMS** & **Versioning**; có **Lifecycle** tối thiểu cho Raw.
* **Hiệu năng:** file Parquet \~128–512MB; partition theo `run_date`/`year`; tránh “small files”.
* **Logging:** mọi bước ghi log JSON vào `meta/gold/_logs/<task_id>/run_date=…`.

---

## 8) Bàn giao & Tiêu chí chấp nhận

### 8.1 Deliverables (BẮT BUỘC)

* **Tài liệu thiết kế**: kiến trúc Medallion, sơ đồ DAG, quyền IAM & cấu hình bảo mật.
* **Bằng chứng Catalog**: ảnh/snapshot schema Bronze/Silver/Gold; cấu trúc partition trên S3.
* **Báo cáo Bronze**: summary, %reject, top-5 lỗi, mẫu bản ghi reject (ẩn dữ liệu nhạy cảm nếu có).
* **Báo cáo Silver**: chiến lược xử lý thiếu dữ liệu (≥2), lý do; thống kê dedup.
* **Báo cáo DQ**: JSON & tổng hợp pass/fail cho Gold; file “missing keys” (nếu có).
* **BI Warehouse**: mô tả schema Redshift (dims/bridge/facts, SCD); thống kê nạp; kết quả kiểm thử hậu nạp.

### 8.2 Kiểm thử chấp nhận (BẮT BUỘC)

* Partition pruning hoạt động khi lọc theo `run_date`/`year`.
* `fact_dataset_owner_daily`: **total = private + public** cho mọi `run_date` đã nạp.
* `fact_competitions_yearly`: **competitions\_count ≥ active\_competitions\_count** cho mọi `year`.
* `fact_tag_usage_daily`: **usage\_count ≥ new\_usage\_count** cho mọi `run_date`.
* Tỷ lệ map FK > **99%**; phần còn lại về SK=0 và có log.
* Chạy lại cùng `run_date`: **không phát sinh trùng lặp** (idempotent).

---

## 9) Chấm điểm (100 điểm)

1. **Bronze Standardization & Validation** – 25đ
2. **Airflow Orchestration (local)** – 20đ
3. **Silver Cleaning nâng cao** – 15đ
4. **Gold Data Model (Dims/Bridge/Facts)** – 20đ
5. **DQ & Stop Conditions** – 15đ
6. **Redshift & Kiểm thử BI** – 5đ

---

## 10) Gợi ý sơ bộ (không bắt buộc)

* Chuẩn bị **mapping tên cột → snake\_case** cho cả 5 bảng trước khi chạy.
* Khoá tự nhiên: `user_id`, `dataset_id`, `competition_id`, `kernel_id`, `(dataset_id, tag)`; chốt **một kiểu dữ liệu duy nhất** cho toàn pipeline.
* Đặt biến DAG `RUN_DATE` thành ngày xử lý; mọi partition S3 dùng đúng `run_date`.
* Bắt đầu với một `run_date` nhỏ để kiểm tra gate 10%, rồi mở rộng backfill.
