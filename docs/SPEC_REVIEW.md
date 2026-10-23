> Historical review of [Specification v1](SPEC_V1.md). Current v2 contracts and verification gates are in [SPEC.md](../SPEC.md); this review does not approve the v2 implementation.

# Spec review — 19 ต.ค. 2026 (เวลาไทย)

ผู้ตรวจ: agent `/root/review_spec_v1` แบบ read-only แยกจากผู้เขียน อ่าน [SPEC.md](../SPEC.md) ทั้งฉบับ เทียบ [รายงานทดลอง](EXPERIMENT_REPORT.md), [metrics](experiment_metrics.json) และ [experiment README](../experiments/README.md) พร้อมตรวจเอกสารทางการของ discovery/order book/delivery ไม่มีการแก้ไฟล์หรือเรียก live market experiments โดย reviewer

**ผลสุดท้าย: ผ่านสำหรับเริ่ม implementation ไม่มี findings ค้าง** Reviewer ตรวจฉบับแก้ซ้ำแล้ว ไม่ใช่การรับรองว่า collector ผ่าน production gate

## Findings และการแก้

| ระดับ | สิ่งที่ reviewer พบ | การแก้ใน spec |
|---|---|---|
| P2 | Delivery ที่ตั้งใจ checkpoint/yield ถูก recovery ตีเป็น interrupted ขัดกับการ resume | แยก working lifecycle `in_progress`/`yielded` จาก terminal archive status กำหนด durable clean-yield checkpoint และ active marker; crash ที่ไม่มี checkpoint ที่เชื่อถือได้เป็น interrupted attempt และเริ่ม run ใหม่ |
| P2 | Deadline ยังไม่ระบุ monotonic และการหักเวลาที่เริ่มช้า | แปลง slot+240/270 เป็น monotonic deadlines ครั้งเดียวก่อน network; hard stop รวม termination graceและหัก start delay ไม่รีเซ็ตตาม wall clock |
| P2 | Preflight ล้มเหลวอาจไม่มี archive/manifest ทำให้ reconcile เป็นทั้ง failed และ missing | เพิ่ม terminal slot event แบบไม่มี archive และลำดับ reconcile ต่อ slot/currency นับผลเดียว แยก skip/duplicate และรายงาน conflicting evidence |
| P2 — พบตอนตรวจซ้ำ | การแก้ clean-yield ทำให้เหลือหนึ่งหน้าต่อ tick ช่วงว่างประมาณ48ticksใน2ชั่วโมงไม่พอ workload52หน้า | อนุญาตหลายหน้าต่อ invocationภายใน20วินาที checkpointทุกหน้าโดยถือlockต่อ แล้ว clean-yieldตอนคืนlock เพิ่ม required fake-clock/fake-server test เทียบ52หน้าและsnapshot184วินาทีทุก5นาที |

ทั้งสามข้อได้รับการยืนยันว่าเป็นประเด็นจริงโดย reviewer ผู้เขียนชี้ประเด็นเดียวกันระหว่างรอผล ไม่อ้างว่าเป็นข้อค้นพบอิสระทั้งหมด รอบแรกไม่พบประเด็นเพิ่มเติมนอกสามข้อนี้

Reviewer ตรวจ final specification แล้ว: findings เดิมทั้งสามและ regression ของ delivery ปิดครบ ไม่พบข้อขัดแย้งใหม่ ข้อกำหนดให้ทดสอบ52หน้ายังเป็น acceptance test ที่ต้องทำระหว่าง implementation ไม่ใช่ test ใหม่ที่รันผ่านแล้วในงานเขียน spec นี้

## Fingerprints

- ฉบับรอบแรก: SHA-256 `32d20e24711d76a48623d751d39d5fd3a07dc2ef61c0a9775f5b20328a52b456`
- ฉบับตรวจซ้ำที่พบ delivery regression: SHA-256 `b6fb20c464b329bc9390525a50053e1d08a3e01fbd37faedc05874e4edce1d5b`
- **ฉบับสุดท้ายที่ reviewer ยืนยันผ่าน:** SHA-256 `ff908b923069d7bd213d1065a073e5d62edf412576946fd22b811d6e0020ec34`

## สิ่งที่การ review นี้ยังพิสูจน์ไม่ได้

การ review ยืนยันความสอดคล้องของข้อกำหนด ไม่ได้ทดสอบ implementation ใหม่ ผลทดลองเดิมบน Mac และ fault tests เป็นหลักฐานตั้งต้น ต้องทดสอบ Linux/systemd, VPS IP, deadline/cancellation, recovery, disk/clock, Telegram และ soak บน VPS จริงอย่างน้อย24ชั่วโมงตามเกณฑ์ใน spec ก่อนเรียก production-ready ข้อจำกัด rolling sweep, public quota ที่ไม่รับรอง และ VPS-only storage ยังมีอยู่ตามที่เลือก
