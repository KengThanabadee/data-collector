# Data Collector (DC)

GitHub: [KengThanabadee/option-collector](https://github.com/KengThanabadee/option-collector) (private)

ขอบเขต v2 ปัจจุบันคือ Deribit BTC/ETH inverse options (book 20 levels, ticker, trades และ index), Binance USDⓈ-M ตามรายชื่อที่ตั้งค่า และ Bybit options (ticker และ publicTrade) เก็บ raw bytes, เวลา, metadata และช่วงข้อมูลขาด ตาม [collection scope](docs/v2/COLLECTION_SCOPE.md)

สถานะและงานถัดไปอยู่ที่ [PLANS.md](PLANS.md): v1 เป็น Deribit REST collector ส่วน v2 มี `discover`, `capture` และ `verify-run` พร้อม live option rediscovery โดย raw mode มี duration, frame และ byte limits เป็น unbounded ตามค่าเริ่มต้น Snapshot mode เดิมยังจำกัด 120 วินาที ผลรันหนึ่งชั่วโมงเดิมเป็น profile เก่า; เกณฑ์รับงานใหม่ที่ครบสาม venues และคร่อม 08:00 UTC ยังไม่เสร็จ ดูวิธีรันที่ [docs/v2/RUNNING.md](docs/v2/RUNNING.md) และ [ประวัติการพัฒนา](docs/v2/history/IMPLEMENTATION.md)

- [Specification v2: ขอบเขตและ contracts](SPEC.md)
- [Feed capability/coverage และข้อจำกัดแต่ละตลาด](docs/v2/FEEDS.md)
- [กฎปิด snapshot, replay และแก้ validity ย้อนหลัง](docs/v2/SNAPSHOTS.md)
- [เกณฑ์รับงานและงบทรัพยากรสำหรับทดลอง](docs/v2/ACCEPTANCE.md) · [ค่าตั้งต้นที่ยังไม่เปิดใช้](docs/v2/pilot-budget.toml)
- [Specification v1 ต้นฉบับ](docs/SPEC_V1.md)
- [ผล agent review ของ spec v1](docs/SPEC_REVIEW.md)
- [ความคืบหน้า implementation](PLANS.md)
- [เริ่มใช้และดูแล collector](docs/OPERATIONS.md)
- [ผลทดสอบ implementation บนเครื่อง](docs/LOCAL_VALIDATION.md)
- [ข้อมูลเพียงพอสำหรับ BSAD/ML หรือไม่: ผลจาก dataset และการฟิตจริง](docs/RESEARCH_DATA_SUFFICIENCY.md)
- [ผลการทดลองและตาราง TF](docs/EXPERIMENT_REPORT.md)
- [ตัวเลขจากหลักฐานที่เก็บจริง](docs/experiment_metrics.json)
- [วิธีรันการทดลองซ้ำ](experiments/README.md)

มี collector สำหรับรัน local แล้ว: async collection, lossless gzip JSONL, frozen-universe coverage, verified publication/recovery, delivery pagination, CLI/reader/export และ monitoring พร้อม systemd units สำหรับทดสอบบน Linux ยังไม่ได้ติดตั้งหรือเปิดเก็บต่อเนื่องบน VPS

ใช้ Python3.12 แล้วติดตั้ง dependencies ตาม `requirements.lock` ใน `.venv` จากนั้นรัน:

```sh
.venv/bin/python -m option_collector status --json
.venv/bin/python -m option_collector collect-once
.venv/bin/python -m unittest discover -s tests -v
```

ข้อมูลจริงอยู่ `data/production/` (ไม่เข้า Git) คำสั่ง collect-once เป็น manual run หนึ่งรอบ การเปิดเก็บจริงยังต้องผ่าน Linux checks/Telegram/24-hour VPS soak ตาม spec Optional candle backfill ยังแยกจาก core rollout นี้
