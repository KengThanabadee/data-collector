# เริ่มใช้และดูแล collector

ขณะนี้พัฒนา local แล้ว แต่ **ยังไม่ผ่าน Linux/VPS gate** ห้ามเปิด timer บน VPS ก่อนตรวจ units, NTP, disk, timeout/reboot และ Telegram จริงตาม [spec](../SPEC.md) คำสั่งในเอกสารนี้เป็นขั้นตอนเตรียมใช้งาน ยังไม่ได้อ้างว่ารันบน VPS แล้ว

## บนเครื่องพัฒนา

ต้องใช้ Python 3.12 และติดตั้ง dependencies ตาม lock ลง virtual environment:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m option_collector status --json
.venv/bin/python -m option_collector collect-once
```

`collect-once` โดยไม่ระบุ slot เป็น manual run ไม่ได้เริ่มเก็บต่อเนื่อง ค่าเริ่มต้นคือ BTC+ETH ทั้ง universe/depth10,000 ใช้เวลาประมาณ 3 นาทีจากการทดลองก่อนหน้า ข้อมูลอยู่ `data/production/` และไม่ถูกนำเข้า Git Local config ข้าม NTP check เพราะ macOS ไม่มี timedatectl; VPS config บังคับตรวจ

แต่ละผลลัพธ์แสดง `destination`, `status`, missing jobs และจำนวน attempts ตรวจหรือ export archive โดยแทน path จากผลจริง:

```sh
.venv/bin/python -m option_collector verify /ABSOLUTE/PATH/TO/ARCHIVE
.venv/bin/python -m option_collector recover
.venv/bin/python -m option_collector export /ABSOLUTE/PATH/TO/ARCHIVE /ABSOLUTE/PATH/TO/NEW-EXPORT
```

Export อ่าน archive ที่ complete โดย default ถ้าตั้งใจอ่าน partial/interrupted ให้เพิ่ม `--include-incomplete` Python reader คือ `option_collector.archive.read_archive(path, include_incomplete=False)` และ `option_collector.common.body_bytes(record)` ให้ original response bytes

## เตรียม VPS ก่อนเปิดเก็บ

แผนเครื่อง: Hetzner CX23 (สำรอง CAX11/CX33) ใช้ Ubuntu 24.04 และเข้าด้วย SSH key เท่านั้น ใช้เครื่องร่วมกับ trading-bot โดยแยก repo, service และ user ส่วนขนาด disk ให้คำนวณจาก GiB/day รวม WAL ที่วัดได้จาก acceptance run

ใช้ Ubuntu24.04/Python3.12 บัญชี service `option-collector` วาง release ใหม่ใน `/opt/option-collector/releases/<VERSION>` และ symlink `/opt/option-collector/current` ไป release นั้น สร้าง `.venv` บน VPS ใหม่ ไม่คัดลอก `.venv` จาก Mac

ติดตั้ง source, `requirements.lock`, config และ unit files จากโปรเจกต์ ใช้ `config/vps.toml` เป็น `/etc/option-collector/collector.toml` ข้อมูล `/var/lib/option-collector` ต้องเป็นของ service user รักษา code/release แยกจาก data และไม่ใช้ root รัน collector ตรวจสิทธิ์ด้วย `sudo -u option-collector test -w /var/lib/option-collector`

เมื่อ source อยู่ใน release directory แล้ว ใช้ `sudo bash /opt/option-collector/releases/<VERSION>/deploy/install.sh /opt/option-collector/releases/<VERSION>` เพื่อเตรียม user/venv/config/units Script นี้ไม่เปิด timer และไม่เปลี่ยน current release ที่มีอยู่ ถ้า VPS ยังไม่มี venv ให้ติดตั้ง `python3.12-venv` ด้วย package manager ก่อน

ตั้ง Healthchecks timezone เป็น UTC: BTC/ETH cron `*/5 * * * *` grace5 นาที, delivery `0 9 * * *` grace2 ชั่วโมง, health ทุกนาที grace2 นาที เชื่อม Telegram และทดลองรับ alert จริง นำ URL จริงใส่ `/etc/option-collector/healthchecks.json` จาก example ให้ service อ่านได้เท่านั้น (`0600`, owner `option-collector`) อย่า paste URL ลง command line/logs

```sh
timedatectl show --property=NTPSynchronized --value
sudo systemd-analyze verify /etc/systemd/system/option-collector.service /etc/systemd/system/option-delivery.service /etc/systemd/system/option-monitor.service
sudo systemd-analyze calendar '*-*-* *:00/5:00 UTC'
sudo systemctl daemon-reload
```

NTP ต้องได้ `yes` ก่อนเริ่ม หลังตรวจ Linux gate ครบแล้วจึงเปิด schedule และ timers:

```sh
cd /opt/option-collector/current
sudo -u option-collector .venv/bin/python -m option_collector --config /etc/option-collector/collector.toml resume
sudo systemctl enable --now option-monitor.timer option-delivery.timer option-collector.timer
sudo systemctl list-timers 'option-*'
```

## สถานะ พัก และเริ่มต่อ

```sh
sudo systemctl status option-collector.service
sudo journalctl --namespace=option-collector -u option-collector.service -n 50 --no-pager
sudo -u option-collector /opt/option-collector/current/.venv/bin/python -m option_collector --config /etc/option-collector/collector.toml status --json
```

การพักตามตั้งใจ: หยุด timer ก่อน รอ service ปัจจุบันจบ แล้วบันทึก pause อย่าหยุด monitor ตามไปด้วย Delivery timer หยุดแยกด้วยเมื่อทำ maintenance

```sh
sudo systemctl stop option-collector.timer option-delivery.timer
sudo systemctl is-active option-collector.service option-delivery.service
sudo -u option-collector /opt/option-collector/current/.venv/bin/python -m option_collector --config /etc/option-collector/collector.toml pause
```

รอให้ `is-active` เป็น inactive ก่อน pause/อัปเดต ถ้าต้องหยุดทันทีใช้ `systemctl stop option-collector.service` แล้ว `recover` ก่อนเริ่มต่อ เปิดต่อด้วย `resume` และ `systemctl start option-collector.timer option-delivery.timer` ต้องมี pause/resume events เพื่อแยก downtime ที่ตั้งใจจากข้อมูลหาย

## เมื่อแจ้งเตือน

- Snapshot missing/partial: ดู status/logs และ missing jobs ข้อมูลอดีตที่พลาดไม่สามารถเอาข้อมูลตอนนี้ไปแทนได้ รอบถัดไปเริ่มใหม่เอง
- Disk warning: export/เพิ่มพื้นที่ก่อนเต็ม Collector ไม่ลบ raw เอง ถ้าพื้นที่ต่ำกว่าเกณฑ์จะปฏิเสธรอบใหม่ Monitor ยังทำงาน หลังเพิ่มพื้นที่ preflight ผ่านจึงเก็บใน slot ถัดไป
- Clock unsafe: แก้ NTP และตรวจ `timedatectl` ไม่แก้ timestamp ใน raw เดิม
- Interrupted: รัน `recover` ตอน collector ไม่มี lock รันซ้ำได้ ถ้า conflict/checksum เสียให้เก็บไฟล์เดิมทั้งหมดไว้และตรวจต่อ ห้ามลบ journal เพื่อทำให้สถานะดูสำเร็จ
- Telegram/missing ping ทั้งที่ archivecomplete: ตรวจ network/Healthchecks แยก ไม่เปลี่ยนสถานะ archive และไม่ส่ง success ย้อนหลังเพื่อกลบช่วงขาด

Export ผ่าน CLI หรือ SFTP พร้อม context/archive/manifest/tail ถ้ามี ตรวจ `verify` ที่ปลายทางก่อนใช้งาน และไม่ลบต้นฉบับบน VPS

## อัปเดตและย้อนกลับ

พัก timer/รอ active service จบ บันทึก pause รัน recover/verify เตรียม release ใหม่พร้อม venv/lockeddependencies และ tests ก่อนสลับ current ทำ manualsmoke capture และ verify แล้ว resume เปิด timer หาก fail สลับ symlink กลับ release เดิม ข้อมูล raw ไม่ต้อง migrate/เขียนทับ ห้ามย้อน release ที่อ่าน schema ของข้อมูลปัจจุบันไม่ได้

Optional candle backfill ยังไม่รวมใน core rollout นี้ ดูผล history experiments ได้จาก [รายงาน](EXPERIMENT_REPORT.md); การติดตั้ง VPS และ 24-hour soak ยังเป็นงานที่ต้องทำกับเครื่องจริง

Logs อยู่ใน journald namespace `option-collector` จำกัด persistent64MiB/runtime16MiB และ retention30 วัน แยกจาก raw ที่ไม่ลบอัตโนมัติ อ้างอิง [systemd journal namespaces](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#LogNamespace=) และ [journald.conf](https://www.freedesktop.org/software/systemd/man/latest/journald.conf.html)

หลังมี window24 ชั่วโมงจริง ใช้ `audit-soak --start 2026-10-20T00:00:00Z --end 2026-10-21T00:00:00Z` (เปลี่ยนเป็นช่วงจริง) เพื่อคำนวณ denominator/P95/verify ข้อมูลครบ Audit นี้ไม่รับรอง production readiness แทนหลักฐาน Linux/Telegram/reboot
