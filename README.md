# Youtube Analytics

Theo dõi hiệu năng kênh YouTube Sơn Tùng M-TP nhờ bộ mô hình **dbt** và pipeline **Apache Airflow** (Astro) đổ dữ liệu vào **Snowflake**.

## Kiến trúc

1. **YouTube ingestion**: script thu thập API lưu tạm vào seeds như [`dbt_youtube/seeds/video_stats.csv`](dbt_youtube/seeds/video_stats.csv) và [`dbt_youtube/seeds/video_comments.csv`](dbt_youtube_dag/dags/dbt/dbt_youtube/seeds/video_comments.csv).
2. **dbt transformations**: mô hình hoá trong [`dbt_youtube/dbt_project.yml`](dbt_youtube/dbt_project.yml) với các layer bronze → silver → gold (ví dụ cấu hình test trong [`dbt_youtube/models/gold/schema.yml`](dbt_youtube/models/gold/schema.yml)).
3. **Orchestration**: DAG Astro Airflow trong [`dbt_youtube_dag/dags`](dbt_youtube_dag/dags) (ví dụ [`example_astronauts`](dbt_youtube_dag/dags/exampledag.py)) chạy dbt và các bước kiểm tra.
4. **Warehouse**: Snowflake (tham khảo thông tin mẫu ở [`snowflake-create-wh/snowflake-account.txt`](snowflake-create-wh/snowflake-account.txt)).

## Yêu cầu hệ thống

- Python $ \geq 3.8 $ (xem [`pyproject.toml`](pyproject.toml))
- Docker + Astro CLI cho Airflow
- Quyền truy cập Snowflake & YouTube API key

## Chuẩn bị môi trường

1. Tạo `.env` dựa trên [`\.env-sample`](.env-sample) (YouTube API + thông tin Snowflake).
2. Cài dependencies Python:

   ```bash
   pip install -e .
   ```

3. Cài đặt profile dbt trong `~/.dbt/profiles.yml` hoặc dùng mẫu [`dbt_youtube/profiles.yml`](dbt_youtube/profiles.yml).

## Làm việc với dbt

```bash
cd dbt_youtube
dbt deps
dbt seed          # nạp seeds CSV
dbt run           # build models
dbt test          # chạy tests
```

Các README phụ trợ:
- [dbt_youtube/README.md](dbt_youtube/README.md): hướng dẫn starter dbt.
- [dbt_youtube_dag/dags/dbt/dbt_youtube/README.md](dbt_youtube_dag/dags/dbt/dbt_youtube/README.md): ghi chú dbt trong DAG.

## Chạy Airflow bằng Astro

```bash
cd dbt_youtube_dag
astro dev start
```

Astro dựng các service trong Docker và mở UI tại `http://localhost:8080`. Cấu hình bổ sung nằm ở [dbt_youtube_dag/README.md](dbt_youtube_dag/README.md).

## Tổ chức thư mục

| Path | Nội dung |
| --- | --- |
| [`dbt_youtube`](dbt_youtube) | Project dbt chính, seeds, models, macros. |
| [`dbt_youtube_dag`](dbt_youtube_dag) | Môi trường Astro/Airflow, DAG orchestration. |
| [`snowflake-create-wh`](snowflake-create-wh) | Script/tài liệu tạo warehouse Snowflake. |

## Kiểm thử & CI

- `dbt test` kiểm thử schema & data.
- Có thể mở rộng với Airflow sensors hoặc jobs CI (GitHub Actions) để gọi `dbt build`.

## Giấy phép

Phân phối dưới giấy phép [MIT](LICENSE).