# Deribit Option Collector — Specification v1

ปรับปรุง 19 ต.ค. 2026 (เวลาไทย) จาก [ผลทดลอง](docs/EXPERIMENT_REPORT.md) และ [ตัวเลขหลักฐาน](docs/experiment_metrics.json)

สถานะ: specification สำหรับ implementation มีตัวทดลองที่รันกับ production public API และ fault tests บน local แล้ว ยังไม่มี VPS หรือ service ที่ deploy ไว้ ข้อ 1–5 เป็นภาพรวม ส่วนข้อ 6–12 กำหนด contracts และเกณฑ์รับงาน ผล review บันทึกแยกใน `docs/SPEC_REVIEW.md`

## 1. ขอบเขตที่เลือก

- สะสม BTC/ETH options ที่ชำระเป็นเหรียญ ทุก strike/expiry ในสถานะ open, settlement, locked และ halted เริ่มรอบทุก 5 นาทีตาม UTC
- **ปรับ default จาก depth 20 เป็น depth 10,000** เพื่อเก็บระดับราคาที่ endpoint เปิดให้ได้มากที่สุดในหนึ่งคำขอ พบการตัดระดับที่ depth 20 จริงและมี full-universe followup สำหรับวัดผล รักษา depth เป็น configuration และ flag หาก response ชนเพดาน 10000; ไม่อ้างว่าเป็น unlimited-depth book
- เก็บทุก field ของ response พร้อม metadata ของ options/futures (`extended=true`), option/futures summary และ index price ทุกครั้ง ส่วน delivery history เป็น job แยกเวลา 09:00 UTC รายวัน
- ใช้ public API โดยไม่ต้องมี key เริ่มจากเพดานรวม 10 requests/second,8 concurrent requests ซึ่งเป็นค่าทดลอง ไม่ใช่สิทธิ์โควตาที่ exchange รับรอง Discovery ใช้ limiter แยกไม่เกิน 1 request/second และยังนับในเพดานรวม
- แยกความครบของการเก็บออกจากคุณภาพ quote: book ว่าง, ไม่มีราคาอีกฝั่ง หรือ IV=0 ต้องเก็บค่าต้นฉบับและ quality flags โดยไม่เติม/กรองทิ้งใน raw
- คง VPS-only storage และไม่ลบ raw อัตโนมัติ การเก็บนี้ยังไม่ป้องกันดิสก์เสียหรือการลบ VPS

## 2. Raw archive และความถูกต้อง

- แบ่ง gzip JSONL ตามวัน UTC, scheduled slot, run_id และ currency ใช้ `schema_version` พร้อม code/config version; manual runs แยกชนิดจาก scheduled runs
- Envelope เก็บ method/params, attempt, เวลาเริ่ม/รับ, monotonic latency, HTTP/API status, error และ source timestamp ที่มี ส่วน body เก็บข้อความ UTF-8 เดิมหรือ base64 เมื่อ decode ไม่ได้ พร้อม SHA-256 ห้าม parse decimal แล้ว serialize กลับเป็นต้นฉบับ
- Freeze รายชื่อเป้าหมายจาก discovery ณ ต้นรอบ เก็บ response discovery ทั้งหมด รวม inactive records แต่เรียก book ตามสถานะที่เลือก รายชื่อที่เข้าใหม่ในรอบถัดไปไม่ย้อนเปลี่ยน completeness ของรอบเดิม
- `complete` ต่อ currency ต้อง discovery สำเร็จ, ข้อมูลประกอบที่กำหนดสำเร็จ และ book response ที่ถูกต้องครบทุกเป้าหมาย Discovery ที่ว่างผิดปกติหรือล้มเหลวไม่เป็น complete ถ้าสัญญาหายระหว่างเก็บให้เก็บ error และ mark partial
- ใช้ journal ที่ fsync ก่อนนับ record ว่า durable จากนั้น gzip, ตรวจ round-trip/hash, atomic publish archive และ manifest แล้วจึงลบ journal ตรวจทั้ง tail evidence และ checksum ก่อน cleanup เสมอ รองรับ recovery ซ้ำและห้ามเขียนทับ archive ที่ไม่ตรงกับ journal
- สถานะมี complete/partial/failed/interrupted; มี gap ledger แยกสำหรับช่วงที่ไม่มีการเก็บจริง ไม่สร้างข้อมูลปัจจุบันแทน snapshot ย้อนหลัง
- รายงานช่วงเวลาจริงทั้งรอบและต่อสัญญา หนึ่งรอบเป็น rolling sweep ประมาณ 3 นาที ไม่ใช่ภาพตลาดพร้อมกัน เก็บ source timestamp, server timing และ local times แยกกัน

## 3. การรันและ backfill

- Python 3.12, dependency lock และ TOML configuration; CLI หลักคือ collect-once, delivery, backfill-candles, status และ verify พร้อม reader สำหรับนำข้อมูลไปประมวลผล
- systemd oneshot + UTC 5-minute timer, Persistent=false, ไม่ Restart รอบเก่าอัตโนมัติ มี process lock ร่วมกับ manual runs เรียกใหม่ตาม timer รอบถัดไปหลัง failure/reboot
- งบ acquisition รวม discovery/reference/books ถึง scheduled slot +240 วินาที และ process ต้องจบภายใน slot +270 วินาที รวม termination grace; manual run นับจากเวลาเริ่ม Cancel/หยุดส่งงานเมื่อหมดงบ พร้อมปิดรอบ partial; recovery ใช้ journal กรณีถูก terminate การทดสอบ stdlib ปัจจุบันมี admission deadline แต่ production ต้องเพิ่ม total-deadline/cancellation และตรวจด้วย Linux hard timeout จริง
- สลับ BTC/ETH และหมุนลำดับสัญญา จำกัดทุกคำขอ/retry ด้วย global limiter, bounded retry สำหรับ timeout/connection/429/5xx/10028 และ global cooldown ที่เคารพ Retry-After จำกัดไม่เกิน 3 attempts ต่อคำขอและไม่เลยงบรอบ ไม่เปลี่ยน rate/depth/universe เงียบๆเมื่อเก็บไม่ทัน
- Delivery pagination เพิ่ม offset ตามจำนวนที่คืนจริงและตรวจ records_total ไม่หยุดเพียงเพราะ page สั้น ตรวจวันซ้ำ/ไม่มีความคืบหน้า และ mark partial หากยังไม่ครบ Delivery กับ snapshot ใช้ lock และนโยบายโควตาร่วมกัน ห้ามแต่ละ process เรียก 10 rps พร้อมกัน
- ให้ snapshot มี priority: delivery ที่ lock ไม่ว่างเลื่อนไปลองในช่วงว่างก่อนรอบถัดไป แบ่งหน้าให้หยุดก่อน snapshot ถัดไปอย่างน้อย 10 วินาที เก็บ progress และพยายามให้จบภายใน grace 2 ชั่วโมง โดยไม่ขวางรอบ options
- Candle backfill เป็น optional one-off แยกจาก snapshot timer ปิดไว้เป็นค่าเริ่มต้นและไม่เป็นเงื่อนไขก่อนเปิดสะสม snapshots หากเลือกทำบน VPS ให้จัดช่วงแยก ใช้ lock เดียวกัน และบันทึกช่วงที่เว้นเก็บ ห้ามรันแย่งโควตาใน background อัตโนมัติ
- รองรับ 12 TF ที่ทดสอบ: 1/3/5/10/15/30 นาที, 1/2/3/6/12 ชั่วโมง, 1 วัน ระบุ instrument และช่วงเวลาอย่างชัดเจน ไม่ backfill ทุก TF ของทุก options อัตโนมัติ
- ขอไม่เกิน 4,000 nominal intervals ต่อหน้า ตรวจความคืบหน้าจาก timestamp จริง เก็บทุก raw หน้า รวม no_data/ขอบซ้อน; deduplicate เฉพาะ derived view การได้ 5,001 แท่งจากช่วงกว้างไม่ใช่ขีดจำกัดย้อนหลังทั้งหมด
- ใช้ phase ของแท่งที่ exchange ส่ง โดย daily เริ่ม 08:00 UTC ตามที่ทดลอง Derived backtest view ต้องกรอง `bar_open + interval <= analysis_asof` และเก็บ zero-volume flags ห้ามถือว่าแท่งที่มีอยู่ทุกแท่งมีการซื้อขายหรือมีประวัติ IV/quotes

## 4. VPS และการดูแลแบบสั้น

- เริ่มทดสอบ Ubuntu 24.04 LTS, 2 vCPU, RAM 2 GB, SSD 80 GB; ความจุดิสก์ต้องอิงผลจริงและการเติบโต ไม่รับรองเก็บได้หลายปีโดยไม่ขยาย ข้อมูลที่ทดลองชี้ระดับประมาณ 170 MiB/วันก่อน overhead
- ใช้บัญชี service ที่ไม่ใช่ root,SSH key,ไม่เปิด application port,ซิงก์นาฬิกา เก็บข้อมูลใน `/var/lib/option-collector` และ config/secrets ใน `/etc/option-collector` จำกัดขนาด journald logs
- Healthchecks.io แพ็กเกจฟรี → Telegram: BTC/ETH checks เป็น UTC cron ทุก 5 นาที grace 5 นาที, delivery รายวัน grace 2 ชั่วโมง และ disk check แยกจาก collector Success หลัง durable commit; partial/failed/interrupted แจ้ง failure การส่ง notification ล้มเหลวไม่เปลี่ยน archive ที่สำเร็จและ URL ต้องไม่อยู่ใน logs/config provenance
- เตือนดิสก์เมื่อเหลือ <20%; หยุดรอบใหม่เมื่อเหลือ <5% หรือ <2 GiB และให้ disk monitoring ทำงานต่อ Resume ต้องผ่าน preflight พื้นที่และเผื่อ journal+archive+gzip.tmp+tail evidence พร้อมกัน
- คู่มือเอาเฉพาะติดตั้ง/เริ่ม/พัก/สถานะ/logs/ตำแหน่งข้อมูล/export และสิ่งที่ต้องทำเมื่อแจ้งเตือน ไม่ต้องทำคู่มือ Linux ยาว ผู้ใช้ถาม agent ต่อได้
- ผู้ใช้เลือกงบและสร้าง VPS/เชื่อม Healthchecks กับ Telegram ส่วน agent เตรียมโค้ดและคำสั่งติดตั้ง พร้อมช่วย deploy/ตรวจเมื่อมีเครื่อง ไม่ต้องใช้บัญชีหรือ credentials เพื่อทำ local implementation

## 5. ลำดับ implementation และเกณฑ์ผ่าน

1. แยก experimental transport/archive เป็น production modules, config/CLI/reader และ schema contracts พร้อม tests ของ completeness, frozen universe และ validation
2. ทำ total deadline, bounded retries/global cooldown, lock, lifecycle/gap ledger, crash-safe publication และ recovery โดยใช้ fault cases ที่ทดลองเป็นฐาน
3. ทำ delivery ที่มี pagination และ timestamp validation พร้อม fixtures/negative tests ส่วน candle backfill เป็นความสามารถเสริมแยกจากเกณฑ์เปิดสะสม snapshots
4. เพิ่ม systemd units, health/disk monitoring และคำสั่งดูแลขั้นต่ำ ทดสอบ Linux behavior ก่อนติดตั้งบน VPS
5. Actual-VPS soak อย่างน้อย 24 ชั่วโมง:แต่ละตลาด complete≥99%,P95 ช่วงเก็บรวม≤240 วินาที,ไม่มี overwrite/corruption,ทุกช่วงขาดตรวจพบได้ แยกทดสอบ reboot/hard timeout/disk pause-resume/Telegram จากช่วง soak
6. สรุปจำนวนคำขอ,latency/observation span,compressed bytes/วัน,bandwidth จริง,พื้นที่ชั่วคราวและวันที่ดิสก์รองรับ หากเก็บไม่ทันจาก VPS IP ให้ทบทวนวิธีรับข้อมูลก่อนเปิดเก็บจริง โดยไม่ลดรายละเอียดโดยไม่แจ้ง

ผล local รองรับความเป็นไปได้ของแนวทาง REST periodic sweeps แต่ยังไม่แทนผล 24 ชั่วโมงบน VPS จริง และยังไม่ได้พิสูจน์ power-loss durability ด้วยการตัดไฟเครื่อง

## 6. API และ universe contract

ใช้ HTTPS REST ที่ `https://www.deribit.com/api/v2/` ตรวจ TLS certificate ใช้เฉพาะ allowlist ของ public methods ต่อไปนี้ Production/testnet ต้องระบุใน provenance และแยก data root

| Method | Parameters หลัก | บทบาท |
|---|---|---|
| `public/get_instruments` | `currency=BTC/ETH`, `kind=option/future`, `expired=false`, `extended=true` | 4 คำขอ/รอบ; options discovery และ futures metadata |
| `public/get_book_summary_by_currency` | `currency=BTC/ETH`, `kind=option/future` | 4 คำขอ/รอบ; required references |
| `public/get_index_price` | `index_name=btc_usd/eth_usd` | 2 คำขอ/รอบ; required references |
| `public/get_order_book` | frozen instrument name, `depth=10000` | successful response ครบทุก frozen target; retries เก็บแยก |
| `public/get_delivery_prices` | `index_name=btc_usd/eth_usd`, `offset`, `count` | daily pagination |
| `public/get_tradingview_chart_data` | instrument, resolution, start/end timestamp | optional manual backfill |

แต่ละ currency มี required references 5 คำขอ Futures เป็นข้อมูลประกอบ ไม่เรียก order book ของ futures ทุกตัว ไม่รวม USDC/linear options, spot/combos, continuous trades/WebSocket events, IV calculation, dashboard, database, offsite backup หรือ trading API ใน v1

### Discovery

1. ตรวจ HTTP/JSON-RPC success และ result shape; instrument names ต้องไม่ซ้ำ Target ต้องมีชื่อ, kind, settlement currency, state และ metadata ที่ระบุสัญญาได้
2. เลือก `kind=option`, settlement currency ตรงเหรียญ และ state อยู่ใน 4 สถานะที่กำหนด ไม่ใช้ `is_active` เพียงตัวเดียวหรือใช้ summary แทน discovery
3. Persist frozen target list พร้อม hash และ raw discovery ก่อนเริ่ม books ของ currency นั้น เก็บ excluded names/reasons; unknown state หรือ field ที่จำเป็นหายทำให้ universe ไม่น่าเชื่อถือและไม่ complete
4. Empty universe ถือว่าผิดปกติ ไม่ complete และแจ้งเตือน จำนวนเปลี่ยนมากเป็น warning ไม่ใช้เปอร์เซ็นต์เปลี่ยนจำนวนตัดสัญญาทิ้งอัตโนมัติ
5. Discovery failure ไม่ใช้รายชื่อเก่าให้รอบ complete อีก currency เก็บต่อใน budget เดิมได้
6. รายชื่อจาก discovery อาจ cache/ล่าช้าจาก live book ไม่รับรองรู้ทุกสัญญาที่เพิ่งเปิดระหว่างรอบ สัญญาใหม่เข้า universe รอบถัดไป; สัญญาหายคงอยู่ใน expected list พร้อม error/missing

เก็บ units, contract size และ settlement/base/quote currencies ตามต้นฉบับ ไม่แปลงทุก field เป็น USD หรือถือว่า metadata quote_currency เป็นหน่วยของทุก price field Derived adapter ต้องระบุวิธีตีความหน่วยและเวอร์ชัน

## 7. Scheduling และ transport contract

- UTC slots นาที 00/05/10/.../55 มี `scheduled_slot_utc` และ UUID `run_id` Manual run ระบุชนิด `manual` ไม่นับเป็น scheduled slot
- Timer ใช้ `AccuracySec=1s`, ไม่มี randomized delay เริ่ม scheduled run ช้าได้ไม่เกิน 30 วินาที ถ้าเลยบันทึก `late_start` เป็น gap ไม่เก็บข้อมูลปัจจุบันไปติดป้ายอดีต ห้ามรัน slot เดิมซ้ำหลังเริ่ม acquisition แล้ว
- ก่อน network แปลงเวลาคงเหลือถึง slot+240/270 เป็น monotonic deadlines ครั้งเดียว Acquisition, cancellation และ external hard stop ใช้ deadlines นี้ ไม่คำนวณ budget ใหม่เมื่อ wall clock เปลี่ยน ถ้าเริ่มช้า 20 วินาทีจะเหลือ acquisition 220 และรวมถึง hard stop 250 วินาทีจาก start การตั้ง timeout ของ supervisor/systemd ต้องหักความล่าช้าและรวม SIGTERM/SIGKILL grace ให้จบที่ slot+270 เสมอ
- ทุกคำสั่งที่เรียก API หรือแก้ archive ใช้ process lock เดียวกัน Manual runs จึงไม่แย่ง timer แต่ยังทำให้ slot ถูก skip ได้ ต้องบันทึกเหตุการณ์
- Shared limiter เริ่ม 10 requests/second, concurrency 8 และ discovery ≤1 request/second ทุก attempts/retries/references/jobs นับรวม Persist throttle cooldown ข้าม process ไม่ให้ restart หลบ cooldown โปรแกรมอื่นที่ใช้ IP เดียวกันต้องเผื่อโควตาด้วย
- Total wall-clock ต่อ attempt ≤15 วินาทีและ remaining budget รวม connect/TLS/receive ไม่ใช้ read timeout ที่ต่ออายุได้ทุก chunk Cancel in-flight/ปิด connections เมื่อ deadline ถึง ไม่เหลือ worker เขียน journal หลัง commit
- Response body cap เริ่ม 16 MiB ถ้าเกินให้เก็บ prefix ที่รับจริง พร้อม `body_complete=false`, reason และ byte count ห้าม hash prefix แล้วอ้างว่าเป็น full response Request นั้นไม่ successful
- Retry timeout/connection, HTTP429/5xx, JSON-RPC10028 รวมไม่เกิน 3 attempts ใช้ exponential backoff เริ่ม 1 วินาทีพร้อม jitter และ global cooldown เมื่อ throttle เคารพ `Retry-After` แบบ seconds/HTTP-date ถ้ารอเกิน budget ให้เก็บ failed attempt และจบรอบตาม coverage
- HTTP200 ที่มี JSON-RPC error ไม่ใช่ success HTTP error ที่ไม่ใช่ JSON ยังเก็บ body Invalid JSON/wrong identity/shape/nonretryable API error เก็บไว้แต่ไม่นับสำเร็จ TLS failure ห้ามปิด certificate verification

## 8. Raw schema, manifest และสถานะ

ใช้ gzip JSONL แต่ละบรรทัดเป็น envelope `schema_version=1` โดยไม่จำกัด schema ของ exchange ให้เหลือเฉพาะ field ที่ใช้อยู่

| กลุ่ม | Required fields/หลักการ |
|---|---|
| Identity | schema_version, record_id, run_id, job_kind, environment, currency, scheduled_slot_utc/null |
| Request | logical_request_id, attempt, method, parameters, instrument_name/null, effective URL ที่ไม่มี secret |
| Local timing | request_started_utc, response_received_utc/null, Unix milliseconds และ monotonic elapsed พร้อม unit |
| Source timing | คง timestamp/usIn/usOut/usDiff ใน raw; extraction แยกพร้อม source path/unit ไม่มีให้ null ไม่สร้างเวลาทดแทน |
| Result | HTTP status/null, selected headers, API/transport error, validation result, retry reason |
| Body | body_encoding=utf8/base64, raw_body, body_sha256, body_byte_count, body_complete |
| Quality | empty_side, iv_zero, iv_missing, depth_cap_reached, clock_anomaly ฯลฯ แยกจากค่าต้นฉบับ |

Raw body คือ HTTP entity body หลังถอด transfer framing ขอ `Accept-Encoding: identity` และเก็บ Content-Encoding ถ้ามี ไม่อ้างว่าเป็น TLS/wire packets UTF-8 ต้อง decode strict และ encode กลับได้ bytes เดิม มิฉะนั้นใช้ base64 Hash คำนวณบน bytes ที่เก็บจริง ห้าม parse ตัวเลขแล้ว serialize กลับเป็น raw Decimal ใช้ใน derived view เมื่อต้องการความแม่นยำ Transport ล้มเหลวก่อนมี response ใช้ body/hash=null พร้อม error; body ขาดเก็บ prefix พร้อม flag ตามข้อ 7

เก็บทุก attempt รวม errors/duplicates Manifest ชี้ successful record ที่เลือกด้วย record_id ไม่ลบ failed attempt

Layout เป้าหมายใต้ data root:

```text
snapshots/YYYY/MM/DD/<slot-or-manual>/<run_id>/<BTC-or-ETH>/
  context.json          # frozen targets, required jobs, code/config/schema versions
  responses.jsonl.gz    # discovery/references/books/attempts
  manifest.json         # commit marker ของ currency นี้
delivery/YYYY/MM/DD/<run_id>/<BTC-or-ETH>/...
candles/<instrument>/<resolution>/<run_id>/...    # optional
work/<run_id>/...       # journal, tmp, recovery/tail evidence
events/YYYY/MM/DD/<event_id>.json                # immutable lifecycle/gap events
state/...              # locks, cursor, cooldown; rebuild ตามหลักฐานเมื่อทำได้
```

Manifest ระบุ identity/versions, status/reasons, discovery/reference record IDs, frozen universe hash, expected/successful/failed/missing targets, attempt counts, selected success IDs, quality counts, capture span, archive compressed hash/size, uncompressed hash/record count และ context hash Code version ใช้ commit SHA เมื่อมี Git; dirty/noGit ใช้ reproducible source fingerprint พร้อม dependency lock hash Config provenance เก็บ effective public config ที่ redactsecret แล้วพร้อม hash Healthchecks URL ห้ามอยู่ใน raw/context/log/exception

| Status ต่อ currency/job | ความหมาย |
|---|---|
| `complete` | trusted nonempty discovery, references ครบ, valid book ครบทุก frozen target และ commit ตรวจผ่าน |
| `partial` | จบ lifecycle ปกติ มี usable successful data แต่ required jobs/targets ไม่ครบ |
| `failed` | จบ lifecycle ปกติ ไม่มี usable successful data หรือ preflight ทำให้ไม่ได้เริ่ม; กรณีไม่มี archive ใช้ terminal slot event ตามข้อ 9 |
| `interrupted` | มี durable start/journal แต่ไม่มี final commit; recovery รายงาน counts ที่ตรวจได้ |

Discovery invalid/empty ห้าม complete แม้ references ใช้ได้ก็เป็น partial ห้ามลด denominator เมื่อ instrument หาย Success ต้อง identity ถูกต้องและโครงสร้างขั้นต่ำอ่านได้ Unknown extra fields ไม่ทำให้ fail

Book ว่าง, IV=0/null, ขาด quote อีกด้าน, crossedbook หรือ state เปลี่ยน เป็น quality flags ถ้า identity/timestamp/bid-ask arrays อ่านได้ยังนับ capture สำเร็จ ไม่เติมหรือกรอง raw ชน depth cap ยัง complete ภายใต้ depth ที่กำหนด พร้อม flag ว่าอาจมีระดับถูกตัด

Reader อ่านเฉพาะ committed และ verify ผ่านโดย default ต้องเลือก explicitly เพื่ออ่าน partial/failed/interrupted ส่ง coverage/timing/flags ให้ผู้ใช้เสมอ ไม่ซ่อน gaps ไม่รวม manual เป็น scheduled และไม่ใช้ slot แทน observation time

## 9. Publication, recovery และ gaps

1. Durable start/context และ frozen targets ก่อน books ทุก record append ลง journal และ fsync ก่อนนับ durable
2. สร้าง gzip.tmp บน filesystem เดียวกับปลายทาง flush/fsync ตรวจ decompress round-trip, body hashes, count และ coverage
3. Publish archive แบบไม่ overwrite และ fsync parent directory จากนั้น publish manifest แบบ atomic และ fsync directory Manifest เป็น commit marker Archive ไม่มี manifest ยังไม่ committed
4. ก่อน cleanup ตรวจ archive/context/manifest/tail evidence ว่าตรง journal Recovery ใช้ compressed hash ของไฟล์ที่ publish จริง ไม่ recompress แล้วเทียบ gzip header ที่อาจเปลี่ยน
5. ลบเฉพาะ journal/tmp ที่พิสูจน์ว่าซ้ำกับ commit แล้ว ข้อมูลขัดกันให้เก็บหลักฐาน/quarantine ห้าม overwrite หรือ bless corruption Recovery error ไม่ส่ง success ping

Recovery ถือ lock และไม่ยิง network เก็บเฉพาะบรรทัด journal ที่ครบ แยก tail ขาดเป็น evidence พร้อม hash ไม่ทิ้ง bytes ถ้าไม่มี valid final manifest สถานะเป็น interrupted ยกเว้น delivery ที่มี valid clean-yield checkpoint ตามข้อ 10; valid commit คง status เดิมแม้ process ตายก่อน cleanup/notification ห้าม upgrade สถานะเพราะกู้ไฟล์ได้

Persist `collection_enabled_from_utc` และ pause/resume/stop/config-change events Expected slots นับเฉพาะช่วงเปิดเก็บ ก่อนเปิดครั้งแรกไม่เป็น gap Claim slot ภายใต้ lock เพื่อกันซ้ำ Event เป็น immutable file มี unique ID; gap/status indexes เป็น derived view ที่ rebuild จาก events/commits

Monitor/recovery reconcile ทุก expected slot กับ per-currency manifests โดยเฉพาะหลัง reboot ช่วงไม่มี process ให้บันทึก gap ย้อนหลัง พร้อม reason machine_offline/unknown ตามหลักฐาน ห้ามเดา ต้องเห็น missing slot, partial coverage, interrupted แยกกัน Pause ที่ตั้งใจไม่นับใน availability denominator แต่ research reader ต้องเห็นช่วงนั้น

Preflight ที่ไม่เริ่ม acquisition (disk low, clock unsafe, invalid config) ใช้ terminal slot event ที่ไม่มี archive: ระบุ slot/run/job/currency, `status=failed`, reason, observed time และ `archive=null` Lock busy/late start ใช้ skip event พร้อม reason และยังถือว่า slot นั้นเก็บไม่สำเร็จ Duplicate invocation เป็น event อ้างผลเดิม ไม่สร้าง gap เพิ่ม ถ้าเขียน event ไม่ได้ให้ log และ monitor reconcile เป็น missing/unknown ภายหลัง ไม่สร้างหลักฐานว่ามี event ที่เขียนไม่ได้

การ reconcile ต่อ slot/currency ใช้ลำดับ: valid final manifest → terminal no-acquisition event → durable claim ที่เลย hard deadlineและไม่มี clean checkpoint (interrupted) → missing slot เหตุการณ์ skip เป็น reason ประกอบ gap แต่ไม่แทน complete manifest นับ outcome เดียวต่อ slot/currency ถ้าหลักฐาน terminal ขัดกันให้รายงาน conflict ไม่เลือก success เงียบๆ Automatic disk/clock pause ไม่ตัด expected slots ออกจาก denominator; ยกเว้นเฉพาะ pause ที่ผู้ใช้ตั้งใจและมี event

## 10. Delivery, candles, config และ CLI

### Delivery checkpoints และ priority

- Job date เป็น UTC due 09:00 UTC (16:00 ไทย), grace 2 ชั่วโมง Daily complete ต่อ currency ต้อง pagination ครบตาม stable advertised total ของ run, unique keys/dates, valid timestamps และ commit ตรวจผ่าน
- ถ้า empty ก่อนครบ/non-progress/total เปลี่ยนให้เก็บทุกหน้าและ partial ถ้า total เปลี่ยนให้ restart pagination เป็น run ใหม่ เก็บ run เดิม ไม่เปลี่ยน total แล้ว declare complete เงียบๆ
- Runner ตื่นทุกนาทีที่วินาที 30 เฉพาะ due window และยังไม่ complete Lock ไม่ว่างข้าม tick ไม่ถือ lock รอ จำกัด invocation รวม cleanup/termination≤20 วินาที และจบก่อน snapshot ถัดไป≥10 วินาที ไม่เริ่ม request/retry ถ้า budget ไม่พอ
- Checkpoint ทุกหน้าที่ durable โดยยังถือ lock เพื่อทำหลายหน้าต่อ invocation ได้ภายใน budget 20 วินาทีและ safe boundary ไม่จำกัดหนึ่งหน้าต่อ tick Resume cursor ผูก hash กับ journal ถ้า state หาย rebuild จาก raw pages ไม่วนเริ่มหน้าแรกทุก tick Daily ทั้งสอง currency complete แล้วส่ง success ครั้งเดียว Skip lock/ยังไม่ due ห้ามส่ง success จบ grace ยังไม่ครบให้ failure พร้อม reason ไม่ใช้ผลเมื่อวานแทน
- Delivery มี working lifecycle `in_progress`/`yielded` แยกจาก terminal archive status ก่อนคืน lock ให้ fsync journal แล้ว publish clean-yield checkpoint ที่มี run/currency, next offset, records_total, journal byte count/hash และ versions จากนั้นห้ามแก้ journal/network ต่อใน invocation นั้น Tick ถัดไปตรวจ checkpoint และทำเครื่องหมาย active แบบ durable ก่อน acquisition ต่อได้ จึงไม่ถือ clean yield เป็น interrupted และไม่ส่ง failure เพราะ yield
- ถ้า crash ขณะ active หรือ checkpoint/hash ไม่ตรง ให้เก็บ attempt เป็น interrupted/conflict ห้าม resume ต่อแล้วเปลี่ยน terminal attempt นั้นเป็น complete เริ่ม pagination เป็น run ใหม่โดยคงหลักฐานเดิมไว้ การ resume จาก yielded ปกติยังเป็น run เดิมและยังต้องตรวจ stable total; ไม่เผย working run เป็น committed dataset

### Optional candles

- รับ instrument/window UTC/TF explicit ไม่ backfill ทุก TF ทุก option โดยอัตโนมัติ เก็บ raw no_data/ขอบซ้อน/zero volume ตรวจ array lengths, bounds, duplicate/non-progress และ holes
- ใช้ timestamp/phase จริง Deduplicate เฉพาะ derived view ด้วย policy เลือก record ที่บอกได้ และกรอง bar close≤analysis_asof History ถูก fetch ตอนนี้ ไม่รับรองว่า field นั้นเผยแพร่แล้วณ historical as-of
- ถ้ารันบน VPS ให้กำหนด maintenance window และ pause snapshots พร้อม event ใช้ shared lock/quota ค่า default ไม่เปิด background backfill

### Config/CLI

Python 3.12, TOML config ที่ validate ก่อน network, dependency lock Config หลักคือ environment/base URL allowlist, data root, currencies/states, depth, rate/workers/discovery limiter, retry/timeouts, disk thresholds, notification secret file และ enabled jobs ค่า invalid ให้ fail ก่อนเริ่ม ห้าม fallback เงียบๆ

| CLI เป้าหมาย (`option-collector …`) | Contract |
|---|---|
| `collect-once` | default manual; `--scheduled-slot` ใช้โดย timer พร้อม late/duplicate guard |
| `delivery --due` | bounded chunk ของ daily job; snapshot priority |
| `backfill-candles` | optional explicit instrument/window/TF |
| `recover` | offline idempotent recovery ใต้ lock ไม่แก้ valid commits |
| `status --json` | latest status/coverage ต่อ currency,last slot,gaps,disk,clock |
| `verify PATH` | hash/context/count/coverage check; nonzero เมื่อไม่ตรง |
| `export` | ช่วงที่ระบุพร้อม manifest/checksum; ไม่แทน raw |
| `monitor-once` | disk/clock/gap reconciliation ทำงานแม้ snapshot paused |

Exit codes:0 สำเร็จหรือ valid delivery no-op;2 partial;3 failed;4 interrupted/recovery conflict;5 skipped scheduled run สถานะอ่านจาก manifest/event ห้ามตีความ exit 0 no-op เป็น delivery success

โมดูลหลัก: config/clock, transport/limiter, discovery/sweep, schema/validation, archive/recovery, lifecycle/gaps, delivery, optional candles, reader/export, monitoring/CLI ไม่มี business logic ซ้ำใน systemd ไม่ import ตัวทดลองเป็น production โดยไม่แยก contract/tests

## 11. Operational contracts และ acceptance

- ตรวจ NTP sync ก่อน enable timer Runtime ถ้า unsynced หรือ wall clock กระโดดต่าง monotonic เกิน 1 วินาทีให้ flag/pause รอบใหม่และแจ้งเตือน ไม่ rewrite เวลา raw เดิม Sync แล้ว resume ใน slot ถัดไปพร้อม event
- Disk/clock monitor แยก minute check ยังทำงานเมื่อ collector paused Success เฉพาะ healthy fail เมื่อ warning/pause Notification timeout≤5 วินาทีและไม่เกิน invocation budget ไม่ rollback archive ไม่รอเลย hard deadline ไม่ replay historical success เพื่อ reset missing ping ปัจจุบัน
- Healthchecks cron ต้องใช้ UTC BTC/ETH `*/5 * * * *` grace 5 นาที Delivery 09:00 UTC grace 2 ชั่วโมง Secret URL อยู่ในไฟล์ service อ่านได้ ไม่ส่ง raw market data ไป monitoring ต้องทดสอบ Telegram รับจริง
- Export ผ่าน SSH/SFTP หรือ CLI ส่ง manifest/checksum และตรวจปลายทาง ไม่เปิด application port และไม่ลบ source หลัง export
- Update ใช้ versioned release: pause/timer off,ตรวจไม่มี active collector,recover/verify,สลับ release,smoke test,resume หาก fail ย้อน release เดิมไม่ rewrite หรือ migrate raw เก่า

### Meaningful tests ก่อน VPS

Fixtures/fake clock/fake server ครอบคลุม raw round-trip, frozen universe, identity, empty quotes/IV zero, unknown fields, depth cap, JSON-RPC error ใน HTTP200, HTTP429 non-JSON, Retry-After, cancel in-flight/deadlines, statuses, secret redaction, duplicates/manual/late/reboot gaps, delivery short page/changing total/non-progress/cursor loss และ snapshot priority เพิ่มกรณี delivery 52 หน้าตาม workload ที่ทดลอง ต้องจบภายใน grace 2 ชั่วโมงด้วย multi-page invocations ภายใต้ simulated request latency/rate และ snapshot ที่ใช้เวลา 184 วินาที/รอบ โดยไม่มี snapshot ถูกเลื่อน

Fault tests ครอบคลุมทุก publication checkpoint, recovery ซ้ำเหมือนเดิม, checksum/count/tail เสียต้องไม่ถูก bless Linux ทดสอบ SIGTERM/SIGKILL,total hard timeout,lock,disk pause/resume,clock,Telegram และ export verify ก่อน soak

### Actual VPS gate

- กำหนด soakwindow UTC≥24 ชั่วโมงเต็ม ไม่มี intentional pause ต่อ 24 ชั่วโมงมี 288expected slots/currency นับ missing/partial/failed/interrupted ใน denominator ทั้งหมด
- `complete / expected >= 99%` ต่อ BTC/ETH ที่ 288 slots จึง complete≥286 ห้ามลบ outage ออกจาก window ถ้ามี intentional pause ให้เริ่ม window ใหม่
- P95 span จากเริ่ม discovery ถึงรับ required response สุดท้ายต่อ currency ในรอบ complete≤240 วินาที ใช้ nearest-rank พร้อมรายงาน durations/failure counts ของ all runs ไม่ซ่อนรอบไม่สำเร็จ
- Zero overwrites/corruption Verify ทุก commit ทุก missing slot/target ตรวจพบจาก gap ledger/coverage ไม่มี success ก่อน commit หรือ delivery ทำให้ snapshot late/overlap
- Reboot/hard timeout/disk pause/clock unsafe/Telegram ทดสอบแยกจาก soak แล้วเริ่ม window ใหม่ บันทึก alert ที่รับจริงและ time-to-detection ไม่ถือว่า HTTP ping สำเร็จคือ Telegram ทำงานแล้ว
- วัด compressed bytes/วัน,manifest/context/log overhead,network traffic จริง,peak temporary disk,free-space runway แยก local estimate จากผล VPS

ยังไม่เรียก production-ready จนผ่าน gate ไม่มีการซื้อ VPS/สมัครบัญชี/commit/push จากการอนุมัติ spec เพียงอย่างเดียว หาก VPS IP หรือ universe growth ทำให้ไม่ทัน ให้รายงานและทบทวนวิธีรับข้อมูลก่อนเปิดเก็บต่อเนื่อง ไม่ลดรายละเอียดให้ผ่านโดยไม่แจ้ง

## 12. เอกสารอ้างอิง

ตรวจเอกสารทางการและหลักฐานทดลอง 19 ต.ค.2026 (เวลาไทย) Parameter/limit ที่เปลี่ยนภายหลังต้อง verify ใหม่และบันทึก version

- [Instrument discovery และ settlement-currency matching](https://docs.deribit.com/api-reference/market-data/public-get_instruments)
- [Order book และ depth สูงสุด 10,000](https://docs.deribit.com/api-reference/market-data/public-get_order_book)
- [Rate limits](https://docs.deribit.com/articles/rate-limits)
- [Delivery prices](https://docs.deribit.com/api-reference/market-data/public-get_delivery_prices)
- [TradingView chart data](https://docs.deribit.com/api-reference/market-data/public-get_tradingview_chart_data)
- [Healthchecks cron](https://healthchecks.io/docs/cron/) และ [Pinging API](https://healthchecks.io/docs/http_api/)

ค่าที่สังเกต เช่นขอ count1000 ได้ page100 และ intraday ได้ 5,001 แท่ง เป็นผลทดลอง ไม่ใช่ exchange guarantee
