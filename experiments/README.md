# การทดลอง Deribit collector

โค้ดในโฟลเดอร์นี้ใช้ทดสอบสมมติฐานก่อนทำ collector สำหรับ VPS ยังไม่ได้ติดตั้ง service หรือเปิดเก็บข้อมูลต่อเนื่อง

ใช้เฉพาะ Python standard library (ทดลองบน Python 3.9.6 ของเครื่องนี้; เป้าหมาย production ยังเป็น Python 3.12) ไม่มี API key หรือ private/trading endpoints

## การทดลอง

1. `live_probe.py`: discovery และข้อมูลอ้างอิง, pilot 2/5/8/10 requests/second ประมาณ 30 วินาทีต่อระดับ, full-universe sweeps สองรอบ, ตัวอย่าง depth 20/10000 และ pagination ของ delivery history
2. `history_probe.py`: TF ทั้ง 12 ค่าที่ API รองรับ, หน้าต่างที่ขอจำนวนแท่งต่างกัน, หน้าต่างก่อนข้อมูลที่ได้รับ และช่วงใกล้วันที่สัญญาถูกสร้าง เปรียบเทียบ perpetual กับ option ตัวอย่างของ BTC/ETH
3. `test_archive_probe.py`: kill subprocess ณ checkpoint ระหว่างเขียน/บีบอัด/commit และทดสอบ checksum กับการกู้ซ้ำ
4. `test_live_probe.py`: จำลอง rate-limit response, deadline และการนับแท่งเมื่อมีช่องว่าง โดยไม่เรียกเครือข่าย

## รันซ้ำ

ใช้ output directory ใหม่ทุกครั้ง และรัน live กับ history ตามลำดับเพื่อไม่ให้สอง process ใช้โควตา IP พร้อมกัน

```sh
python3 -m unittest discover -s experiments -p 'test_*.py' -v
python3 -u experiments/live_probe.py --output experiments/results/my-live-run
python3 -u experiments/history_probe.py --source experiments/results/my-live-run --output experiments/results/my-history-run
python3 experiments/summarize_probe.py --live experiments/results/my-live-run --history experiments/results/my-history-run --output experiments/results/my-metrics.json
```

โค้ดทดลองหยุดเมื่อพบ throttling หรือ network failure และไม่ retry เพื่อไม่เพิ่มโหลดหรือซ่อน failure ข้อผิดพลาดจากการจงใจขอช่วงข้อมูลที่ไม่รองรับใน history probe เก็บเป็นผลทดลองแยกต่างหาก

## หลักฐานและขอบเขตข้อสรุป

- Raw response พร้อมเวลาและ HTTP/API status อยู่ใน gzip JSONL; แต่ละ stage มี context, manifest, checksum และ summary
- เก็บ response body เดิมแบบ lossless ไม่ผ่านการ parse ตัวเลขแล้วเขียนกลับ
- `summary.complete` ของ book sweep ตรวจทุกงานใน frozen universe; book ว่างนับว่า capture สำเร็จ คุณภาพ quote เป็นอีกเรื่องหนึ่ง
- เวลาเก็บทั้งตลาดต้องรวม discovery/reference phase กับ book phase การทดลองนี้ไม่ใช่การทดสอบ systemd scheduler
- 240 วินาทีเป็น admission deadline ของตัวทดลอง socket timeout ไม่ใช่ hard wall-clock timeout; production ต้องมีตัวคุมเวลาภายนอก
- ตัวเลขพื้นที่สูงสุดใน manifest รุ่นแรกเป็นสูตรสำหรับ normal path ไม่ใช่ค่าที่วัด peak จริง การกู้หลัง crash ต้องเผื่อ journal, archive, temporary gzip และ tail evidence อยู่พร้อมกัน
- HTTP body bytes ไม่รวม headers/TLS/connection overhead จึงเป็นค่าต่ำกว่าการใช้ bandwidth จริง
- จำนวนแท่งคูณ TF เป็นเวลาตามทฤษฎี ส่วนเวลาที่มีข้อมูลจริงให้อ่านจาก timestamp แรก/สุดท้ายและจำนวนช่องว่าง
- หน้าต่างข้อมูลตัวอย่างพิสูจน์ได้เฉพาะช่วงที่ขอ ไม่ใช่สัญญาว่าประวัติทุก TF/ทุกสัญญาครบต่อเนื่อง
- สอง full sweeps บน local ไม่ใช่ 24-hour soak test และไม่รับรองโควตาหรือความเร็วจาก VPS IP
- SIGKILL tests พิสูจน์การกู้หลัง process crash ภายใต้ไฟล์ที่ fsync แล้ว ไม่ใช่การทดสอบตัดไฟเครื่องจริง

Raw results ถูก ignore จาก Git เพื่อไม่ให้ commit ข้อมูลทดลองจำนวนมากโดยอัตโนมัติ รายงานสรุปและ spec เก็บแยกในโปรเจกต์
