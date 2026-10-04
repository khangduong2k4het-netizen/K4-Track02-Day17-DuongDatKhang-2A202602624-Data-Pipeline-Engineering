# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Dương Đạt Khang / 2A202602624
**Repo bài nộp:** https://github.com/khangduong2k4het-netizen/K4-Track02-Day17-DuongDatKhang-2A202602624-Data-Pipeline-Engineering
**Commit mã nguồn** `53236cb39dd05f849336831738b51b28d4e26c15`
**AI đã dùng và phạm vi hỗ trợ:** Codex hỗ trợ đọc code, sửa ba lỗi, học viên chạy kiểm tra và soạn báo cáo, đã review và hiểu các thay đổi.
**Nguồn tham khảo:** Mã nguồn, model dbt và tài liệu trong repo.

## 1. Ba lỗi

|                              | Lỗi Silver                                                                            | Lỗi late data                                                                                   | Lỗi xoá (CDC)                                                                                        |
| ---------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| **Triệu chứng**      | Nhiều hàng cùng ticket_id; T-91 có cả trạng thái cũ.                           | Feature lệch full recompute; thiếu event u05 ngày 12/08 đến ngày 15/08.                    | T-97 còn trong dữ liệu hiện tại, snapshot mới nhất và RAG.                                     |
| **Nguyên nhân gốc** | Dedup chỉ trong batch rồi INSERT nối thêm giữa các batch.                        | LOOKBACK_DAYS=0 chỉ tính lại ngày ingest.                                                    | Lấy khóa từ after=null làm CDC delete bị loại.                                                   |
| **Cách sửa**         | pipeline/silver.py: MERGE theo ticket_id; chỉ UPDATE khi LSN nguồn lớn hơn đích. | pipeline/config.py: lookback=3; giữ logic Gold gom theo event_time và ghi đè cửa sổ ngày. | pipeline/staging.py: lấy khóa lần lượt từ after, before, key; bỏ Kafka tombstone không có op. |
| **Khái niệm**        | Upsert theo khóa, CDC ordering, idempotency.                                          | Event time, ingest time, late arrival, overwrite-partition.                                      | CDC delete, tombstone, delete propagation.                                                             |

## 2. Các con số

- Bronze: 43 records; P50=0, P95=2.90, P99=3.00, max=3 ngày. Chọn LOOKBACK_DAYS=ceil(P99)=3 để không làm tròn xuống mất dữ liệu muộn.
- u05 / 2026-08-12: 5 events, 3 clicks, 1 feedback down; T-91: high / closed / bug.
- Rerun PASS: C0=C1=C2=C3=`39e115c510ecdf526800eac227158a4f`.
- Verify 18/18; pytest 34 passed; dbt PASS=19; parity PARITY cho cả hai bảng.

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE duy trì một trạng thái mỗi ticket; overwrite-partition tính lại toàn bộ aggregate trong cửa sổ nên không cộng lặp khi replay.
- Silver giữ tombstone cùng LSN để batch cũ không hồi sinh ticket; user_id, subject, body null, không sao chép PII từ before.
- Snapshot lấy Bronze as-of ngày đó để tái lập dữ liệu huấn luyện và giữ priority tại thời điểm tạo; feedback muộn tạo phiên bản mới.
- DuckDB phù hợp dữ liệu nhỏ chạy cục bộ; dbt cung cấp model, test và parity, chưa cần chi phí vận hành Spark.
- dbt unique_key=ticket_id xác định khóa MERGE; merge_update_condition chỉ cho LSN mới hơn cập nhật; batch_size=day chia feature theo ngày sự kiện, lookback=3 tính lại ba batch trước. Biên kết thúc 17/08 không bao gồm nên phủ đủ ngày 16/08.

## 4. Hai câu hỏi suy ngẫm

1. Lab giữ snapshot cũ bất biến nên v2026-08-12..14 vẫn có văn bản T-97; chỉ snapshot mới nhất và RAG loại ticket. Production cần quy trình xoá theo định danh xuyên Bronze, transcripts, snapshots, cache, index và backup theo chính sách lưu giữ; thu hồi bản cũ, tạo phiên bản đã xoá và ghi audit không chứa PII. Bất biến không thay thế nghĩa vụ xoá.
2. Đặt chốt PII trước khi văn bản rời Bronze sang Silver, trước training/embedding; kết hợp regex với NER nhận diện tên tiếng Việt và kiểm tra đầu ra. Đo precision/recall trên tập gán nhãn, tỷ lệ PII lọt và false positive; quarantine trường hợp không chắc chắn, kiểm tra lại khi đổi model.

## 5. Output thực tế

PowerShell: pytest dùng --basetemp trong workspace vì thư mục Temp mặc định báo WinError 5; không sửa tests. Output dưới đây được lấy từ lần chạy thật. Bản log riêng nằm trong submission/evidence/.

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

```

```text
PS> .\.venv\Scripts\python.exe -m pytest --basetemp .pytest_tmp_final
..................................                                       [100%]
34 passed in 3.53s

```

```text
PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

```

```text
PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

```

```text
PS> .\.venv\Scripts\python.exe main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)

```

```text
PS> $env:DO_NOT_TRACK = '1'; Push-Location dbt_project; try { ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
03:17:23  Running with dbt=1.12.5
03:17:23  Registered adapter: duckdb=1.11.0
03:17:24  Unable to do partial parsing because saved manifest not found. Starting full parse.
03:17:27  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:17:27
03:17:27  Concurrency: 1 threads (target='dev')
03:17:27
03:17:29  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:17:29  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
03:17:29  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:17:29  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
03:17:29  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:17:29  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.09s]
03:17:29  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:17:29  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.12s]
03:17:29  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:17:30  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.09s]
03:17:30  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:17:30  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
03:17:30  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:17:30  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
03:17:30  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:17:30  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
03:17:30  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:17:30  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
03:17:30  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:17:30  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
03:17:30  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:17:30  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
03:17:30  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:17:30  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
03:17:30  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:17:30  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
03:17:30  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:17:30  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
03:17:30  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:17:30  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
03:17:30  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:17:30  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
03:17:30  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.05s]
03:17:30  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
03:17:30  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
03:17:30  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
03:17:30  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
03:17:30  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:17:30  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
03:17:30  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.25s]
03:17:30  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:17:30  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
03:17:30  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:17:30  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
03:17:30  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:17:30  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
03:17:30
03:17:30  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.19 seconds (3.19s).
03:17:30
03:17:30  Completed successfully
03:17:30
03:17:30  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

```

```text
PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

```

## 6. Bonus B1

Cache theo SHA-256 của prompt đầu vào + model + prompt_version; cache cả phản hồi lỗi để replay không gọi lại. JSON phải chỉ có label thuộc bug/billing/other; phản hồi sai vào llm_label_quarantine, Gold chỉ chứa nhãn hợp lệ của ticket còn sống. Model và prompt_version lưu trên mỗi hàng.

```text
PS> .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS

```

OpenAI tùy chọn: cài `requirements-openai.txt`, đặt `OPENAI_API_KEY` trong `.env`, tùy chọn `LLM_MODEL` (mặc định gpt-4o-mini), rồi chạy `python -m pipeline.openai_label`. Provider dùng Responses API với strict JSON schema; đọc key bằng dotenv, không lưu key trong cache/log. Lệnh chạy trên warehouse hiện có, không fresh-build để giữ cache. Ước lượng giá trong checker là giá giả lập, không phải giá OpenAI. Đã chạy API thật sau khi học viên cho phép gửi seed giả lập: 11 nhãn hợp lệ, 11 calls, 1319 tokens; replay 0 calls. Provider chuẩn hoá tiền tố openai/ khi endpoint là api.openai.com.

Nguồn API: https://developers.openai.com/api/docs/guides/structured-outputs

Commit mã nguồn B1 đã kiểm tra: `f47540e3204f43cbe45c7099052c4aeeefb29e2a`. Toàn bộ pytest sau B1: 34 passed in 3.99s.

### OpenAI thực tế

```text
PS> .\.venv\Scripts\python.exe -m pipeline.openai_label
OpenAI model=gpt-4o-mini; live tickets=11; estimated tokens~484
Token estimate is approximate; FakeLLM pricing does not apply to OpenAI.
First run: {'labeled': 11, 'calls': 11}; usage tokens=1319
Replay: {'labeled': 11, 'calls': 0}
OPENAI CACHE PASS

```
