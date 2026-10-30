# Data Collector (DC)

GitHub: [KengThanabadee/option-collector](https://github.com/KengThanabadee/option-collector) (private)

เป้าหมาย v2 คือสะสม public market data ของ BTC/ETH จาก Deribit, Binance และ Bybit ผ่าน WebSocket พร้อม REST เสริม ครอบคลุม spot, perpetuals, dated futures, options และ combos/spreads ตาม feed ที่มี เก็บต้นฉบับ เวลา metadata และช่วงข้อมูลขาด เพื่อ replay และสร้างภาพตลาด ณ เวลาอ้างอิงร่วมกัน

สถานะปัจจุบันและงานถัดไปอยู่ที่ [PLANS.md](PLANS.md) โดยย่อ: v1 คือ Deribit REST collector ที่ใช้งานได้ ส่วน v2 รัน `discover`, `capture` และ `verify-run` สำหรับ Deribit option books ได้แล้ว แต่ `capture` ยังจำกัดที่ 120 วินาที งานถัดไปคือทำให้เก็บต่อเนื่องได้ ดูวิธีรันที่ [docs/v2/RUNNING.md](docs/v2/RUNNING.md) ประวัติการพัฒนาทีละ chunk อยู่ใน [docs/v2/history/](docs/v2/history/IMPLEMENTATION.md)

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
