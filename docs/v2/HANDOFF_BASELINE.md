# Data Collector — Three-Exchange Specification Review

วันที่ตรวจ: 23 ตุลาคม 2026 (Asia/Bangkok)

สถานะ: ผ่านรีวิวขอบเขตและสถาปัตยกรรม v2 แล้ว งานส่งมอบถัดไปคือ SPEC v2 พร้อมรายการ feed และงบทรัพยากรสำหรับพัฒนา
เจ้าของ implementation: `/Users/keng/Developer/option-collector`

## เป้าหมายและงานถัดไป

เปลี่ยนเป้าหมายจาก Option Collector เป็น **Market Data Collector สำหรับ Deribit, Binance และ Bybit** เริ่มจาก BTC/ETH ใช้ WebSocket รับข้อมูลต่อเนื่อง เก็บต้นฉบับและหลักฐานความต่อเนื่อง แล้วสร้างภาพตลาด ณ เวลาอ้างอิงร่วมกันสำหรับงานวิจัยภายหลัง

งานถัดไป: จัดทำ **SPEC v2** ใน repository เจ้าของงาน โดยกำหนด feed ที่จะเปิดของแต่ละตลาด วิธีตรวจความต่อเนื่อง และค่าทรัพยากรสำหรับทดลอง เก็บสเปก v1 และ archives เดิมไว้ใช้อ้างอิง

## ขอบเขตเป้าหมายที่ตกลง

เก็บข้อมูลสาธารณะของ Deribit, Binance และ Bybit เริ่มจากสัญญาที่อ้างอิง BTC/ETH ครอบคลุม product และ currency variants ตามตาราง feed (capability matrix) ซึ่งจะระบุรายการที่เปิดเก็บจริง

- Products: spot, perpetual, dated futures, options และ combo/spread books ที่ exchange มี feed รองรับ
- Books: initial/recovery snapshots และ updates ตามความลึกสูงสุดของ feed ที่เลือก โดยบันทึก depth, aggregation interval และ access requirements
- Trades: เก็บรายการซื้อขายตามที่ feed ส่ง แยกจาก quote และ mark
- Ticker/reference: mark, index, pricing forward, IV/Greeks, OI และ funding ตาม fields ที่แต่ละ product เผยแพร่
- Instruments/lifecycle: strike, expiry, contract size, tick/quantity units, settlement currency, listings, delistings และ state changes
- Funding/settlement/delivery: ข้อมูลสาธารณะพร้อมเวลาที่เผยแพร่ โดยระบุอัตรา funding คาดการณ์และอัตราที่ใช้จริงแยกกัน

จัดเก็บแต่ละ venue แยกกัน พร้อม routing/provenance สำหรับผลิตภัณฑ์ที่ส่งต่อไปตลาดอื่น

## วิธีรับ เก็บ และสร้างภาพตลาด

1. แยก adapter และสถานะต่อ exchange/product; exchange หนึ่งหลุดหรือ overload ไม่ทำให้อีกแห่งหยุดตาม
2. Bootstrap ตาม feed: ใช้ snapshot ทาง WebSocket หรือ buffer updates ร่วมกับ REST seed ตามขั้นตอนของแต่ละตลาด
3. เก็บ raw payload พร้อม exchange/environment/native symbol/channel/feed config, connection/session ID, exchange time ถ้ามี, receive time, sequence/checksum fields และ source/config version
4. ตรวจ subscription acknowledgements, heartbeat, sequence/checksum ตามที่ feed รองรับ; เก็บ reconnect, subscription failure, disk/backpressure และ gap events
5. เมื่อ continuity ขาด ให้ book invalid จนกู้สถานะสำเร็จ การรับ snapshot ใหม่ไม่ได้เติมประวัติที่หายระหว่างทาง
6. แยก immutable raw archive ออกจาก derived books/snapshots; มี bounded durable segments, manifest/checksum และ crash recovery โดยรักษาต้นฉบับเดิม
7. REST ใช้เสริมสำหรับ discovery/reference, bootstrap/reconciliation/recovery และ historical backfill ที่รองรับ

### ภาพตลาด ณ 11:00 หมายถึงอะไร

**สร้างภาพตลาดทุกสัญญา ณ เวลาอ้างอิงเดียวกัน** เช่น 11:00 UTC โดยใช้สถานะจากข้อความที่ collector รับและประมวลผลถึง cutoff ตาม replay policy

Snapshot ระบุ last update, validity และ feed health ต่อสัญญา Book ที่ราคาไม่เปลี่ยนยังใช้ได้เมื่อ connection, subscription และ continuity สมบูรณ์ ช่วงที่ feed invalid จะแสดงเป็นข้อมูลขาด และ bid/ask ที่ขาดจะคงสถานะ missing

ใช้ receive-time เป็น profile เริ่มต้น ส่วน exchange-time reconstruction เป็น derived profile แยก พร้อมกฎจัดการข้อความมาช้า รายละเอียด cutoff, processing lag และ clock อยู่ในข้อกำหนด replay ด้านล่าง

## รายละเอียด feed ที่ต้องกำหนดใน SPEC v2

| ตลาด | ข้อกำหนดที่ตรวจพบ | ผลต่อสเปก |
|---|---|---|
| Deribit | Full-book channel มีให้; raw interval ต้อง authenticated ส่วน public ใช้ 100ms/agg2 | ไม่บังคับ raw แบบไม่ใช้ credentials และไม่เรียก aggregated stream ว่าทุก matching-engine event |
| Deribit spot | บาง instrument route ไป Coinbase; book/quote/ticker มีให้ แต่ public trades/candles ของ routed instruments ไม่รองรับผ่าน Deribit | ใช้ routing flags ใน metadata และบันทึก provenance; ระบุ unsupported feed ตรง ๆ |
| Bybit | Standard options book มีความลึกสูงสุด 100 ระดับ; full-book feed แยกสำหรับ spot/linear/inverse และต้อง REST seed; standard/full books ไม่รวม RPI orders แต่มี RPI book แยกและ public trades มี RPI/block flags | ระบุ RPI profile แยก ไม่อ้าง book กับ trade tape ครอบคลุมคำสั่งชนิดเดียวกันทั้งหมด |
| Binance spot / USDⓈ-M | Spot REST seed สูงสุด 5,000 ระดับต่อฝั่ง; USDⓈ-M seed 1,000 ระดับและกฎ sequence/เส้นทาง stream ต่างจาก spot | ระบุ seeded depth และข้อจำกัดระดับที่ยังไม่รู้จนมี update ไม่เรียก unlimited full book |
| Binance COIN-M / USDⓈ-M | เอกสาร integration ปี 2026 ระบุว่า all-market streams ของ fstream/dstream ส่งเนื้อหา UM+CM ที่รวมกัน และใช้ rate pool ร่วมกัน | แยก product identity จาก routing จริง ตรวจ st/ps และ dedup derived data ตาม native identity; ไม่เปิดสองเส้นแล้วนับ events ซ้ำหรือถือว่าเป็น universe แยกเด็ดขาด |
| Binance options | มีเอกสารการอัปเกรดปี 2026 แต่ contract stream/depth/recovery ปัจจุบันยังตรวจไม่ครบ | เป็น verification gate ก่อนเปิด adapter ไม่คัดลอก legacy endpoint หรือถือว่ารองรับ combos เหมือน Deribit |

แยก `supported`, `unsupported`, `access_required`, `not_verified` และ `not_enabled` พร้อมเหตุผล ข้อมูลที่ไม่มีให้เก็บไม่ใช่ capture failure แต่ต้องไม่ซ่อนออกจากรายงาน scope

แต่ละแถวต้องระบุ `book_model` (L2/L3), depth/seeded coverage, aggregation interval, trade granularity/IDs/side semantics, exclusions, native timestamp units/precision, bootstrap/recovery rules, subscription/connection/rate limits และเอกสารเวอร์ชันที่ใช้ แยก standard/RPI profiles เมื่อมี ไม่รวมสอง feed แล้วบวกปริมาณซ้ำ

## ข้อกำหนดสำหรับคลังวิจัย

### สิ่งที่เก็บสำหรับ order-flow research

Archive เก็บ book snapshots, updates และ trades ต่อเนื่อง เพื่อสร้างทั้งข้อมูลระดับเหตุการณ์และ snapshot ตามช่วงเวลา

Public L2 บอกการเปลี่ยนปริมาณรวมตามระดับราคา ใช้ศึกษาสภาพคล่อง, book imbalance และ order-flow imbalance ภายใต้ความละเอียดของ feed ได้ แต่แยก cancellation ออกจาก execution ทุกครั้งหรือรู้ queue position/order ID ของทุกคำสั่งไม่ได้ ไม่อ้างว่าเป็น L3, hidden liquidity หรือทุก matching-engine event

Adapter ต้องระบุ semantics ของข้อมูลดังนี้:

- **Trades:** native trade ID, timestamp, price, amount และ maker/taker/aggressor ตาม fields ต้นฉบับ พร้อมเวอร์ชัน mapping; aggressor ที่ไม่มีให้ใช้ unknown และแยกค่าที่อนุมานภายหลัง
- **Aggregation/inclusions:** ระบุ individual หรือ aggregated trades รวมข้อยกเว้น เช่น insurance/ADL ของ Binance futures feed; book updates และ trades มี cadence/coverage ของตัวเอง
- **RPI/block และ dedup:** เก็บ flags และ source; raw คงทุกข้อความ ส่วน derived view dedup ตาม native identity ที่ตรวจแล้ว Bybit อาจส่งหลาย trade messages ที่มี seq เดียวกัน จึงใช้ seq เพียงตัวเดียวเป็น dedup key ไม่ได้
- **Liquidation/insurance/public risk statistics:** มีแถวใน capability matrix พร้อม cadence, filtering และ completeness guarantee ของ feed ก่อนเลือกเปิดเก็บ

### ตัวตนสัญญาและข้อมูลที่รู้ในอดีต

Stable instrument key ประกอบด้วย exchange/environment/native product/native symbol และ listing incarnation หาก symbol ถูกนำกลับใช้ใหม่ ไม่ใช้ชื่อ BTC เพียงอย่างเดียวรวมทุก quote/settlement variant

เก็บ metadata และ lifecycle แบบ append-only versions พร้อมเวลาที่สังเกต/รับรู้และ effective time หาก exchange ให้ รวม multiplier, tick/lot, inverse/linear, expiry, settlement/quote/base currencies, combo legs และ routing venue ทุก derived row อ้าง metadata version และ raw record ที่ใช้ ห้ามนำ metadata ล่าสุดหรือรายชื่อสัญญาปัจจุบันย้อนเติมในชุดฝึกอดีต

Reference/funding/settlement histories ที่ดึงภายหลังต้องมี acquisition time, event/effective time และ revision/source แยกกัน: ใช้ศึกษาเหตุการณ์ย้อนหลังได้ แต่ไม่ยืนยันว่า collector รู้ค่านั้น ณ เวลาตัดสินใจในอดีต

### Replay และเวลาตัดสินใจที่ทำซ้ำได้

เก็บลำดับรับข้อความแบบ deterministic ต่อ connection/session, local monotonic time, UTC receive time และ clock health/offset observations พร้อม process/session identity เมื่อ restart ระบุหน่วยและ precision ที่แท้จริงของเวลาทุก field โดยไม่เพิ่มความแม่นยำปลอม ลำดับภายใน message ต้องคงเดิม กำหนด tie-break ของเวลาเท่ากันและ save replay-policy version

Receive-time snapshot เลือกข้อความที่รับก่อนหรือเท่ากับ cutoff และบันทึก applied position ที่ใช้สร้างสถานะ ข้อความที่มาถึงหลัง cutoff ใช้กับ snapshot เวลาถัดไป เมื่อ processing lag เกินเกณฑ์ให้ snapshot degraded/invalid; clock discontinuity ใช้ flags ตาม replay policy และคง raw timestamps เดิม

### Streaming recovery และ archive ที่ใช้ได้ระยะยาว

- Feed state แยก discovering/subscribing/bootstrapping/live/invalid/resyncing; เข้าสู่ `live` เมื่อผ่าน initial state และเงื่อนไขต่อเนื่องของ feed
- Adapter catalogue ต้องบันทึก subscription/rate/connection limits, planned reconnect/rotation, listing/delisting และ scope ของ stream ที่ได้รับผลตามกฎปัจจุบันของ venue รวม rate pools ที่ใช้ร่วมกัน
- บันทึกกฎ snapshot/delta, sequence scope/reset, duplicate/out-of-order/checksum และ reconnect แยกแต่ละ adapter; feed ที่ไม่มี sequence ให้บันทึก continuity confidence และวิธีตรวจสุขภาพที่รองรับ
- Segment มีเพดานเวลา/bytes และ writer queue/RAM รวมที่กำหนดก่อนรัน ประมวลผลและ verify ภายในขอบเขต memory นี้ พร้อมระบุ durable flush interval และขอบเขตข้อมูลเสี่ยงสูญหายก่อน fsync
- Publish raw segment และ manifest แบบ atomic หลัง durable write/verify; บันทึก record counts, hashes, sequence/time ranges, subscription/universe versions และ snapshot/checkpoint anchors ที่ต้องใช้ย้อนสร้างข้าม segment/session เมื่อ crash ให้คง partial tail และแยก committed prefix จากข้อมูลที่กู้ไม่ได้
- Replay เริ่มจาก anchor ที่ตรวจได้พร้อม dependencies ครบ เก็บ reset/resync และ gap ledger ร่วมกับ segment index
- Queue overflow, slow disk หรือ writer failure ต้องมี durable loss/gap evidence เท่าที่ทำได้ และ invalidate stream ที่ได้รับผล ห้ามทิ้ง updates เงียบ ๆ หากไม่รู้ว่า stream ใดสูญหายให้ invalidate ทุก stream บน connection นั้น หาก continuity ที่พิสูจน์ได้สิ้นสุดก่อนตรวจพบ ให้ mark ตั้งแต่จุดนั้นจน resync สำเร็จ
- บันทึก backup destination/cadence/RPO/RTO และ retention เป็น configuration ที่ตกลงก่อน production archive ระยะยาว; ต้องมีสำเนาอิสระนอกเครื่อง capture และทดสอบ restore+hash verification+replay จริง ห้ามลบต้นฉบับอัตโนมัติหรืออ้างว่าการทำ checksum ใน VPS เดียวป้องกันเครื่องเสีย

ตัวเลข segment, memory, lag, durability และ backup budgets ต้องบันทึกใน release configuration ก่อน capacity test/soak และใช้ค่าเดิมตัดสินผล; การเปลี่ยนค่าเพื่อรับ workload ต้องสร้าง configuration version ใหม่ ไม่มีตัวเลข production ที่รับรองแล้วจากการรีวิวเอกสารนี้

## เกณฑ์รับงาน v2 ที่เสนอ

- มี coverage matrix พร้อม official source และวันตรวจ ครบทุก product ที่ประกาศจะรองรับ
- มี capture ตัวอย่างที่แสดง bootstrap, listing changes และ reconnect/resync ของแต่ละ adapter โดยไม่ส่ง trading orders
- Replay raw แล้วได้ book และ reference-time snapshot เดิมตามกฎที่ประกาศ และอ่าน archives v1 ได้โดยไม่แก้ไฟล์เดิม
- ทดสอบ missing/duplicate/out-of-order messages ตามกฎ feed, restart, slow writer, disk full และ recovery; ทุกช่วง invalid มองเห็นได้
- วัด bandwidth, compressed bytes/day, CPU/RAM, disk/write rate, queue backlog และ receive/persist lag แยก exchange/product
- กำหนดเพดานและนโยบาย overload พร้อมบันทึกทุกการเปลี่ยน depth, cadence และ universe
- ทดลอง continuous soak อย่างน้อย 24 ชั่วโมงหลังผ่าน fault tests รายงาน outages และ valid coverage ทุก stream; กำหนดตัวเลขผ่านจากผลวัดก่อนเริ่ม soak ไม่ตั้งย้อนหลังให้ผ่าน
- กำหนด storage budget, retention/export และแผนกู้จากเครื่องเสียก่อนเรียกเป็นคลังระยะยาว นโยบายไม่ลบ raw อัตโนมัติเดิมยังคงอยู่จนมีการอนุมัติเปลี่ยน
- ตรวจ point-in-time metadata join, symbol reuse, trade side/aggregation semantics และ replay ข้าม segment/reset/clock jump ด้วย fixtures ที่ทราบผล ไม่บังคับจับคู่ trade กับ book update แบบหนึ่งต่อหนึ่ง
- ทดลอง restore จากสำเนานอกเครื่องและสร้าง derived snapshot เดิมได้; snapshot ใช้กับ research ต้องอ้าง raw durable ที่ verify แล้ว พร้อม validity ต่อ instrument/feed

## ลำดับงานและ handoff

1. จัดทำ SPEC v2 พร้อม capability matrix, feed rules และค่าทรัพยากรสำหรับทดลอง
2. ออกแบบ common raw/coverage contracts และ adapter interfaces โดยรักษาการอ่าน v1
3. ทดลองตัวอย่างครบสาม exchange โดยเริ่มจาก instruments จำนวนจำกัดเพื่อพิสูจน์แต่ละ feed
4. วัด full target universe แล้วกำหนด resource/depth/aggregation configuration
5. ทำ acceptance และเปิดเก็บต่อเนื่องเมื่อผ่านเกณฑ์

Collector รับผิดชอบ acquisition, durability, provenance และ replay ส่วน analytics/dashboard และงานวิจัยเป็นผู้ใช้ข้อมูล downstream

## ผล agent review รอบสอง

วันที่ 23 ตุลาคม 2026 ผู้รีวิวอิสระสามด้านตรวจไฟล์ฉบับแก้แล้ว:

| ด้าน | คำตัดสิน | ขอบเขตการอนุมัติ |
|---|---|---|
| Research validity | APPROVE | ฐาน requirements สำหรับ options/futures/public order-flow research |
| Archive engineering | APPROVE | สถาปัตยกรรมการเก็บ replay, gaps, durability และ restore |
| Exchange/feed semantics | APPROVE | ขอบเขตคำกล่าวอ้างตรงกับ public feeds และแยกข้อจำกัดแต่ละ venue |

รอบแรกทั้งสามให้ approve with conditions; ได้เติม metadata ตามเวลา, L2/trade semantics, deterministic replay, bounded durability, restore และแก้ feed-specific claims แล้ว ก่อนให้รีวิวไฟล์ใหม่อีกครั้ง ไม่มี blocker เพิ่มต่อ baseline นี้

ขอบเขตการตรวจ: source/document review; สถานะ implementation คงเดิม การทดสอบ capture, fault recovery และ soak อยู่ในเกณฑ์รับงาน

Pink-elephant review: PASS หลังจัดให้เป้าหมาย ขอบเขต และงานถัดไปอยู่ต้นเอกสาร รวมคำอธิบายซ้ำและย้ายประวัติรีวิวไว้ท้ายเอกสาร โดยคงข้อกำหนดความถูกต้องของข้อมูลไว้

## ผลรีวิวตามความสำคัญ

| ระดับ | ช่องว่างในปัจจุบัน | สิ่งที่ v2 ต้องระบุ |
|---|---|---|
| P1 | [SPEC v1](/Users/keng/Developer/option-collector/SPEC.md:61) กำหนด Deribit REST และตัดผลิตภัณฑ์ใหม่ออกที่บรรทัด 72 | Coverage matrix แยก exchange, underlying, product, contract family, settlement currency และ feed |
| P1 | [Collector](/Users/keng/Developer/option-collector/option_collector/collector.py:25) เป็น HTTP sweep; book targets มีเฉพาะ options | Collector ต่อเนื่องแยก exchange พร้อม subscription, reconnect และ recovery ตามข้อกำหนดของ feed |
| P1 | [Coverage ปัจจุบัน](/Users/keng/Developer/option-collector/option_collector/schema.py:96) วัดคำตอบครบตามรายการต้นรอบ | แยกไฟล์สมบูรณ์, subscription ครบ, book สร้างย้อนกลับได้ และ snapshot ใช้ได้ |
| P1 | ยังไม่มี contract ของภาพตลาด ณ เวลาเดียวกันจาก stream | กำหนด as-of time, เวลาที่ตลาดส่ง/เครื่องรับ, late messages และช่วง invalid อย่างชัดเจน |
| P2 | [Config](/Users/keng/Developer/option-collector/option_collector/config.py:10) ผูกกับ Deribit และ settlement BTC/ETH | รองรับ inverse/linear และหน่วยที่ต่างกันโดยไม่ใช้ symbol หรือราคาแทนกันข้าม venue |
| P2 | งบเครื่อง พื้นที่ และเกณฑ์ผ่านเดิมอิง REST sweep | วัดใหม่ต่อ exchange/product; ไม่ยกตัวเลข MiB/วันหรือ 99% ของรอบเก่ามารับรอง streaming |

## แหล่งอ้างอิงทางการ

- [Deribit market-data collection](https://docs.deribit.com/articles/market-data-collection-best-practices)
- [Deribit options feeds](https://docs.deribit.com/articles/options-data-collection-best-practices)
- [Deribit spot routing](https://docs.deribit.com/articles/spot-trading-venues)
- [Bybit standard order book](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
- [Bybit full order book](https://bybit-exchange.github.io/docs/v5/websocket/public/full-ob)
- [Bybit RPI order book](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook-rpi)
- [Bybit trades และ native flags](https://bybit-exchange.github.io/docs/v5/websocket/public/trade)
- [Binance official spot stream documentation](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
- [Binance USDⓈ-M book synchronization](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
- [Binance USDⓈ-M stream routing](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice)
- [Binance UM/CM architecture integration](https://developers.binance.com/docs/derivatives/coin-margined-futures/Important-CM-UM-Integration-Notice)
- [Binance options upgrade](https://www.binance.com/en/support/announcement/detail/7372ff010d914e84b9fd994f849478cd)
