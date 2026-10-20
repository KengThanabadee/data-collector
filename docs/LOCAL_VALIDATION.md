# Local implementation validation — 19 ต.ค. 2026 (เวลาไทย)

สร้าง collector ใน `option_collector/` แยกจากตัวทดลองเดิม ใช้ Python3.12.14/aiohttp3.14.5 ตาม `requirements.lock` ไม่มี API key/private endpoints และยังไม่มี timer/VPS ที่เปิดใช้งาน

## สิ่งที่ทำแล้ว

Validated TOML config และ data-root environment binding, shared async quota/concurrency/retries/cooldown, total attempt/acquisition deadlines พร้อม external process hard stop, frozen discovery, raw text/base64/decimal-token preservation, per-currency completeness, journals/fsync/verified no-clobber publication และ idempotent recovery

มี lifecycle/slot/gap events, preflight disk/NTP/clock-step checks, delivery checkpoints/resume และ rebuild cursor จาก raw, reader/verify/export, monitoring หลัง commit, source snapshots ตาม fingerprint, CLI และเครื่องมือคำนวณ soak data metrics นอกจากนี้เตรียม systemd units/installer/log namespace และ [คำสั่งดูแล](OPERATIONS.md) ไว้สำหรับตรวจบน Linux

## Automated tests

**27 tests ผ่าน** ใช้เวลา 8.036 วินาทีในชุดสุดท้าย ดู [บันทึกผล](local_test_result.json)

- Raw decimal lexemes, binary/non-UTF8 และ unknown fields ย้อนเป็น bytes เดิมได้; verify/export round-trip
- SIGKILL จริง 5publication checkpoints, recovery ซ้ำ, truncatedtail, checksum/context/count conflicts และ orphanarchive ไม่ถูกเขียนทับ
- HTTP server จำลองที่ 127.0.0.1: drip-feed ที่ส่งข้อมูลเรื่อยๆยังถูก totaltimeout, cancellation เก็บ attempt, bodycap แม้เกินใน chunk สุดท้าย, retries, API10028 ใน HTTP200 และ HTTP429 ที่ไม่ใช่ JSON
- Frozenuniverse, emptybook/IVzero, unknownstate/duplicatenames, missingtarget ไม่กลายเป็น complete, processlock, clockstep/reboot, late/duplicate/preflightfailure slots
- Delivery52 หน้าจาก pages ที่ถูก clamp100 รายการ จำลอง 2rps/latency0.5 วินาทีพร้อม snapshot184 วินาทีทุก 5 นาที จบก่อน grace โดยไม่เลื่อน snapshot รวมลบ cursor หลัง yield แล้ว resume จาก raw ได้; total เปลี่ยน/วันซ้ำ/emptypage ไม่ถูกประกาศ complete
- Process ที่จงใจ ignoreSIGTERM ยังถูก externalhardstop ฆ่า; emptysoak มี 288missing slots/currency และไม่ผ่าน gate

Fixtures ทั้งหมดเป็น **ข้อมูลสังเคราะห์** ไม่ใช่ตัวอย่างราคาจริง ผล SIGKILL พิสูจน์ process-crash recovery ไม่ใช่การตัดไฟเครื่องจริง

## เก็บข้อมูลจริง

Snapshot หลักฐานและจำนวน requests/bytes/hash อยู่ใน [local_snapshot_metrics.json](local_snapshot_metrics.json) Raw อยู่ใน `data/production/` และไม่เข้า Git แต่ละ manifest ถูก verify และ source ที่ใช้จริงถูกเก็บใน `provenance/<source_sha256>/` ด้วย จึงตรวจได้แม้มีการปรับ implementation ต่อหลัง capture

| รอบ | BTC options | ETH options | Requests รวม references | ช่วงรับข้อมูล | Errors/missing |
|---|---:|---:|---:|---:|---:|
| แรก | 884/884 | 766/766 | 1,660 | 174.33 วินาที | 0 |
| source ฉบับสุดท้าย | 884/884 | 766/766 | 1,660 | 173.39 วินาที | 0 |

รอบสุดท้ายใช้ source fingerprint ตรงกับโค้ดก่อนแก้ implementation review เมื่อ 20 ต.ค. 2026 ไม่ใช่โค้ดฉบับปัจจุบัน ทั้งสองรอบ verifyarchive/context/raw-body/coverage ผ่าน Export ข้อมูลจริงหนึ่ง archive ผ่าน CLI แล้ว verify สำเนาผ่าน และเรียก recover หลังจบได้รายการว่าง ไม่มี workingjournal ค้าง จำนวนสัญญาต่างจากการทดลองก่อนหน้าเพราะใช้ discovery ใหม่ ไม่ hard-code ตัวเลข

Complete ในที่นี้หมายถึง capture ครบ ไม่รับรองว่าทุก quote เหมาะซื้อขาย Book ว่าง/IVzero ยังเก็บค่าเดิมและ qualityflags ไว้

Delivery ทดสอบแยกใน `data/validation-delivery/` ด้วย productionmodules แต่ไม่เปิด timer/ส่ง ping ใช้ไม่เกิน 3chunks ที่มี acquisitionbudget15 วินาที/chunk:

| Currency | Rows ครบตาม API | Successful pages | Attempts | Status |
|---|---:|---:|---:|---|
| BTC | 2,643 | 27 | 27 | complete |
| ETH | 2,483 | 25 | 26 | complete |

จบรวม 21.31 วินาที ETH มี timeout หนึ่ง attempt ตรงขอบ chunk จากนั้น resume/retry สำเร็จโดยเก็บ failedattempt ไว้ ไม่ลบทิ้งหรือเพิ่ม offset ข้ามหน้าที่ไม่สำเร็จ ดู [รายละเอียดและ hash](local_delivery_metrics.json)

จำนวนแท่งย้อนหลังและข้อจำกัด OHLC ยังอ้าง [history experiments](EXPERIMENT_REPORT.md) เดิม Optional candle-backfill CLI ยังไม่รวมใน core rollout นี้

## การเตรียม deploy และสิ่งที่ยังไม่ยืนยัน

`bash -n deploy/install.sh` ผ่าน, unit files ผ่านการอ่าน INI syntax และ `pip check` ไม่พบ brokenrequirements ผลนี้ **ยังไม่ใช่** `systemd-analyze verify` หรือการทดสอบ Linux จริง Installer ไม่เปิด timer หรือเปลี่ยน current release ที่มีอยู่

ยังต้องมี VPS เพื่อทดสอบ serviceaccount/permissions, NTP, hardtimeout/reboot, diskpause-resume, Telegram รับ alert จริง, export ผ่าน SSH และวัด network/temporarydisk จากนั้น soak≥24 ชั่วโมงตาม [spec](../SPEC.md) Audit เครื่องมือคำนวณ data metrics แยกจากหลักฐาน operations และจะไม่ประกาศ production-ready ให้เอง

ไม่มีการ commit/push หรือเปิดเก็บต่อเนื่อง งานที่ยังเหลือแสดงใน [PLANS.md](../PLANS.md) Spec ที่ agentreview เก็บไว้ไม่เปลี่ยนแปลง

## หลังแก้ implementation review — 20 ต.ค. 2026 (เวลาไทย)

แก้ 6 findings และเพิ่ม 8 regression tests รวมชุดปัจจุบัน **35 tests ผ่าน** ดู [รายละเอียดการแก้](IMPLEMENTATION_REVIEW_FIXES.md) และ [ผลรันทดสอบพร้อม source fingerprint](review_fix_test_result.json) ผล 27 tests ด้านบนเป็นหลักฐานของเวอร์ชันก่อนแก้ ไม่ได้เขียนทับบันทึกเดิม

ตรวจด้วยโค้ดใหม่แล้ว archives เดิม 6 ชุด (4 snapshot และ 2 delivery) ยัง verify ผ่าน ไม่แก้ raw/context/manifest เดิม รอบแก้นี้ใช้ fixtures, localhost HTTP และ subprocess เท่านั้น ยังไม่ได้จับข้อมูลจริงรอบใหม่หรือทดสอบ VPS
