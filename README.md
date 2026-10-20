# option-collector

GitHub: [KengThanabadee/option-collector](https://github.com/KengThanabadee/option-collector) (private)

โปรเจกต์สะสม raw BTC/ETH options data จาก Deribit สำหรับงานวิจัย เช่น IV surface โดยเก็บต้นฉบับพร้อมเวลาและ metadata เพื่อย้อนตรวจและประมวลผลใหม่

- [Specification v1 และลำดับ implementation](SPEC.md)
- [ผล agent review ของ spec](docs/SPEC_REVIEW.md)
- [ความคืบหน้า implementation](PLANS.md)
- [เริ่มใช้และดูแล collector](docs/OPERATIONS.md)
- [ผลทดสอบ implementation บนเครื่อง](docs/LOCAL_VALIDATION.md)
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
