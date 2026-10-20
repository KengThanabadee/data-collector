# ผลการทดลอง Deribit collector — 19 ต.ค. 2026 (เวลาไทย)

ทดลองจาก Mac/IP ปัจจุบัน ไม่ใช้ API key และไม่ได้ติดตั้ง collector บน VPS ผลนี้เป็น local feasibility evidence ไม่ใช่ผลทดสอบ 24 ชั่วโมงหรือ SLA

หลักฐานตัวเลข: [experiment_metrics.json](experiment_metrics.json); raw responses, timestamps, source snapshots และ checksums อยู่ใน `experiments/results/`

## เก็บทั้งตลาด

| Depth | รอบ | BTC + ETH ที่เก็บได้ | เวลารวม discovery/ข้อมูลประกอบ | gzip รวมต่อรอบ | errors/throttles |
|---|---|---:|---:|---:|---:|
| 20 | round-1-full | 1,724/1,724 | 182.20 วินาที | 0.590 MiB | 0 |
| 20 | round-2-full | 1,724/1,724 | 181.30 วินาที | 0.591 MiB | 0 |
| 10,000 | round-1-full | 1,724/1,724 | 183.51 วินาที | 0.589 MiB | 0 |

ใช้เพดาน 10 requests/second และ 8 workers หลัง pilot 2/5/8/10 requests/second ประมาณ 30 วินาทีต่อระดับ ผ่านโดยไม่พบ error/throttling ทุกระดับ จำนวนนี้เป็นค่าที่ทดลองผ่านบน IP นี้ ไม่ใช่ public quota ที่ exchange รับรอง

สองตลาดเก็บแบบสลับกัน แต่หนึ่งรอบยังเป็นข้อมูลคนละเวลาประมาณสามนาที ต้องใช้เวลารายสัญญาตอนวิเคราะห์ ไม่ใช้เวลารอบแทนเวลา observation

## พื้นที่และคุณภาพข้อมูล

- ประเมินจาก 1 รอบของ depth ที่เลือก: gzip ประมาณ **169.6 MiB/วัน**, **60.4 GiB/ปี** เมื่อเก็บทุก 5 นาที รวม delivery รายวัน แต่ยังไม่รวม filesystem overhead, manifests/context และ logs
- ประมาณ **499,444 คำขอ/วัน**; response body ประมาณ **1.09 GiB/วัน** เป็นค่าต่ำกว่า network traffic จริงเพราะไม่รวม HTTP/TLS overhead
- การคูณต่อวัน/ปีเป็นประมาณการจากตัวอย่างสั้น จำนวนสัญญาและสภาพคล่องเปลี่ยนได้ SSD 80 GB ใช้เริ่มทดลองได้ แต่ต้องวัดจำนวนวันที่เหลือและวางแผนเพิ่มพื้นที่ก่อนเก็บระยะยาว
- รอบ depth20 แรกมี 79 สัญญาที่ไม่มี bid หรือ ask อย่างน้อยหนึ่งฝั่ง และ 410 สัญญาส่ง bid IV เป็นศูนย์ ทั้งหมดถูกเก็บตามต้นฉบับ ความครบของ capture ไม่เท่ากับคุณภาพ quote
- ความต่าง local receive time กับ exchange timestamp รวม clock offset และ network delay จึงไม่ตีความเป็น latency หรือ freshness เพียงอย่างเดียว

ตัวอย่างเทียบ depth20/10000 จำนวน 23 สัญญา (จงใจเพิ่มสัญญาที่ book หนา): พบ 8 สัญญามีเกิน 20 ระดับ สูงสุด 28 ระดับ ขนาด response body รวมเพิ่มจาก 25,678 เป็น 26,010 bytes การขอทั้งสอง depth เกิดคนละเวลา จึงไม่ใช้เทียบราคาว่าเท่ากันและไม่ใช่ตัวอย่างสุ่มทั้งตลาด

Full sweep depth10000 พบ 10 สัญญาจาก 1724 สัญญาที่มีเกิน20ระดับ สูงสุด 27 ระดับในรอบนั้น ขนาดและเวลาอยู่ระดับใกล้กับสองรอบ depth20 แต่เป็นคนละเวลา ไม่ใช่ randomized performance comparison

## TF: จำนวนแท่งต่อคำขอกับระยะเวลาย้อนหลัง

API รองรับ TF 1, 3, 5, 10, 15, 30 นาที, 1, 2, 3, 6, 12 ชั่วโมง และ 1 วัน ทดสอบ 135 คำขอ chart บน BTC/ETH perpetual และ options ตัวอย่างสองตัว

ขอ BTC perpetual TF1m 1,000/5,000/10,000 intervals ได้ 1,000/5,000/5,001 แท่งตามลำดับ เมื่อขอช่วงกว้าง 20,000 intervals ก็ได้ 5,001 แท่งเช่นเดิม แต่ขอช่วงก่อนหน้าและใกล้วันเปิดสัญญาได้ จึงเป็นหลักฐาน cap-like ต่อคำขอ ไม่ใช่เก็บย้อนหลังได้แค่ 5,000 แท่งทั้งหมด

คำขอช่วงกว้างใช้ cutoff `2026-10-17T23:59:59.999Z` ตารางนี้นับทุกแท่งที่ API คืนมา รวมแท่งคร่อม cutoff; การใช้ backtest ต้องกรองตามเวลาปิดแท่งอีกครั้ง

| TF | BTC perpetual ที่ได้ต่อคำขอช่วงกว้าง | ระยะเวลาตามจำนวนแท่ง × TF | ETH perpetual |
|---|---:|---:|---:|
| 1 นาที | 5,001 | 3.47 วัน | 5,001 |
| 3 นาที | 5,001 | 10.42 วัน | 5,001 |
| 5 นาที | 5,001 | 17.36 วัน | 5,001 |
| 10 นาที | 5,001 | 34.73 วัน | 5,001 |
| 15 นาที | 5,001 | 52.09 วัน | 5,001 |
| 30 นาที | 5,001 | 104.19 วัน | 5,001 |
| 1 ชั่วโมง | 5,001 | 208.38 วัน | 5,001 |
| 2 ชั่วโมง | 5,001 | 416.75 วัน | 5,001 |
| 3 ชั่วโมง | 5,001 | 625.12 วัน | 5,001 |
| 6 ชั่วโมง | 5,001 | 1,250.25 วัน | 5,001 |
| 12 ชั่วโมง | 5,001 | 2,500.50 วัน | 5,001 |
| 1 วัน | 2,987 | 2,987.00 วัน | 2,775 |

TF รายวันได้ประวัติ BTC perpetual ตั้งแต่ `2018-08-14T08:00Z` และ ETH perpetual ตั้งแต่ `2019-03-14T08:00Z` ถึง `2026-10-17T08:00Z` ไม่พบช่องว่าง timestamp ใน daily responses ที่ทดสอบ ส่วน TF ย่อยทดสอบได้ถึงช่วงใกล้วันเปิดสัญญาเช่นกัน แต่ยังไม่ได้ดาวน์โหลดทุกนาทีตลอดประวัติเพื่อตรวจความต่อเนื่อง

### Options ตัวอย่าง

| สัญญา | TF1m ที่ได้ | TF1D ที่ได้ | จุดเริ่ม daily ที่พบ |
|---|---:|---:|---|
| BTC-30OCT26-95000-C | 5,001 (4,945 แท่ง volume=0) | 80 | 2026-07-30T08:00:00+00:00 |
| ETH-25DEC26-3200-C | 5,001 (4,994 แท่ง volume=0) | 297 | 2025-12-25T08:00:00+00:00 |

เลือกหนึ่ง option ต่อเหรียญจากกลุ่ม OI สูงและอายุสัญญามากกว่า 30 วัน ผลนี้ไม่รับรองทุกสัญญาหรือสัญญาที่หมดอายุแล้ว แท่ง volume=0 มีมาก จึงห้ามถือว่าทุกแท่งมีการซื้อขายหรือมี bid/ask พร้อมใช้

### ขอบเวลาและการ backfill

- แท่งรายวันเริ่ม 08:00 UTC (15:00 ไทย); TF3h/6h/12h ก็มี phase ตามที่ exchange ส่ง ต้องเก็บ timestamp จริง ไม่สร้างตารางโดยสมมติว่าทุก TF เริ่มเที่ยงคืน UTC
- แท่งท้ายของ TF3h/6h ปิดหลัง cutoff ที่ขอ 2 ชั่วโมง และ TF12h/1D ปิดหลัง cutoff 8 ชั่วโมง แม้ตอนเรียกจริงแท่งเหล่านี้ปิดแล้ว การใช้กับ historical as-of ต้องตรวจ `bar_open + interval <= analysis_asof`
- แบ่งคำขอ backfill ไม่เกิน 4,000 nominal intervals ต่อครั้ง ใช้ timestamp แรก/สุดท้ายที่ได้จริงตรวจความคืบหน้า เก็บ raw ของแต่ละหน้า และ deduplicate เฉพาะตารางที่สร้างภายหลัง
- เก็บ `no_data`, zero volume, ค่าเดิม และหน้าที่ได้ไม่ครบไว้ ห้ามเติมข้อมูลให้ดูต่อเนื่องใน raw archive
- Candle history เป็นราคา/volume ไม่ใช่ประวัติ bid/ask, IV และ Greeks ทั้ง chain จึงยังต้องสะสม snapshots ของเราเอง
- ทดลอง mark-price history 7 วันของ options ตัวอย่างทั้งสองแล้วได้รายการว่าง เอกสารระบุว่ามีเฉพาะ subset ของ options ที่ร่วมคำนวณ volatility index จึงไม่ใช้ผลนี้สรุปว่า options ทั้งหมดไม่มี mark history

## Delivery history และการกู้ไฟล์

- BTC: เก็บครบ 2,642/2,642 รายการที่ API แจ้ง จาก 2016-03-18 ถึง 2026-10-18 (รายการ settlement ไม่จำเป็นต้องมีทุกวันปฏิทิน)
- ETH: เก็บครบ 2,482/2,482 รายการที่ API แจ้ง จาก 2019-03-29 ถึง 2026-10-18 (รายการ settlement ไม่จำเป็นต้องมีทุกวันปฏิทิน)
- Production API คืน 100 รายการเมื่อขอ count=1000 แม้เอกสารบอก maximum1000 ตัวทดลองรุ่นแรกจึงหยุดเร็วไป; raw เดิมยังอยู่ และมี rerun ที่แก้ pagination แล้วครบตามยอดรวม ใช้ offset เพิ่มตามจำนวนที่คืนจริงและตรวจ records_total แทนการหยุดเพราะหน้าสั้น
- Offline tests 12 ข้อผ่าน รวม SIGKILL 7 checkpoints, กู้ซ้ำ, ท้าย JSONL ขาด, gzip/manifest/checksum เสีย, tail evidence หาย, HTTP429 ที่ไม่ใช่ JSON, JSON-RPC10028 ภายใต้ HTTP200 และการแบ่งหน้าที่ถูก server clamp
- มีการแก้จุดที่ agent รีวิวพบ: ตรวจ checksum ของ tail evidence, cleanup หลัง commit ให้ idempotent และไม่ให้รอบขาดถูกติดสถานะ captured
- ทดสอบ process crash ภายใต้ fsync ไม่ใช่การตัดไฟจริง Peak temporary disk ใน manifest เป็นแบบจำลอง ไม่ใช่ผลวัด high-water mark ของ filesystem

## สิ่งที่ยังต้องยืนยัน

Actual VPS IP throughput และ 24-hour soak, Linux/systemd hard timeout/reboot/clock sync, การเตือน Telegram เมื่อเครื่องดับ, disk-full pause/resume และการ export ข้อมูลจริง ยังไม่ได้ทดสอบเพราะยังไม่มี VPS/บัญชี monitoring ที่ตั้งค่าไว้

## เอกสารต้นทาง

- [Rate limits](https://docs.deribit.com/articles/rate-limits)
- [Order book](https://docs.deribit.com/api-reference/market-data/public-get_order_book)
- [TradingView chart data](https://docs.deribit.com/api-reference/market-data/public-get_tradingview_chart_data)
- [Delivery prices](https://docs.deribit.com/api-reference/market-data/public-get_delivery_prices)
- [Mark-price history](https://docs.deribit.com/api-reference/market-data/public-get_mark_price_history)
