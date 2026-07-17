from pathlib import Path
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "figuers" / "architecture"
SVG = OUT / "streaming-lakehouse-fireworks.svg"
PNG = OUT / "streaming-lakehouse-fireworks.png"

lines = []
lines.append('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1600 900" width="1600" height="900">')
lines.append('<style>text{font-family:"Microsoft YaHei","PingFang SC","SimHei",Arial,sans-serif}.title{font-size:24px;font-weight:700;fill:#111827}.sub{font-size:12px;fill:#6b7280}.section{font-size:13px;font-weight:700;fill:#374151;letter-spacing:.08em}.name{font-size:14px;font-weight:600;fill:#111827}.body{font-size:11px;fill:#374151}.small{font-size:10px;fill:#6b7280}.label{font-size:10px;fill:#374151}</style>')
lines.append('<defs><marker id="arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0,8 3,0 6" fill="#374151"/></marker><marker id="arrow-light" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0,8 3,0 6" fill="#9ca3af"/></marker></defs>')
lines.append('<rect width="1600" height="900" fill="#ffffff"/>')


def section(x, y, w, h, index, title, fill="#ffffff"):
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" fill="{fill}" stroke="#d1d5db"/>')
    lines.append(f'<text x="{x+16}" y="{y+24}" class="section">{index}  {title}</text>')


def node(x, y, w, h, title, body=(), fill="#ffffff", dashed=False):
    dash = ' stroke-dasharray="5,4"' if dashed else ''
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" fill="{fill}" stroke="#9ca3af"{dash}/>')
    lines.append(f'<text x="{x+w/2}" y="{y+25}" text-anchor="middle" class="name">{title}</text>')
    for i, item in enumerate(body):
        lines.append(f'<text x="{x+w/2}" y="{y+46+i*17}" text-anchor="middle" class="body">{item}</text>')


def cylinder(x, y, w, h, title, body=()):
    lines.append(f'<rect x="{x}" y="{y+10}" width="{w}" height="{h-20}" fill="#f3f4f6"/>')
    lines.append(f'<ellipse cx="{x+w/2}" cy="{y+10}" rx="{w/2}" ry="10" fill="#f9fafb" stroke="#9ca3af"/>')
    lines.append(f'<line x1="{x}" y1="{y+10}" x2="{x}" y2="{y+h-10}" stroke="#9ca3af"/>')
    lines.append(f'<line x1="{x+w}" y1="{y+10}" x2="{x+w}" y2="{y+h-10}" stroke="#9ca3af"/>')
    lines.append(f'<ellipse cx="{x+w/2}" cy="{y+h-10}" rx="{w/2}" ry="10" fill="#e5e7eb" stroke="#9ca3af"/>')
    lines.append(f'<text x="{x+w/2}" y="{y+34}" text-anchor="middle" class="name">{title}</text>')
    for i, item in enumerate(body):
        lines.append(f'<text x="{x+w/2}" y="{y+54+i*16}" text-anchor="middle" class="body">{item}</text>')


def arrow(path, label, lx, ly, secondary=False):
    stroke = "#9ca3af" if secondary else "#374151"
    dash = ' stroke-dasharray="5,4"' if secondary else ''
    marker = "arrow-light" if secondary else "arrow"
    lines.append(f'<path d="{path}" fill="none" stroke="{stroke}" stroke-width="1.5"{dash} marker-end="url(#{marker})"/>')
    width = max(42, len(label) * 11)
    lines.append(f'<rect x="{lx-width/2}" y="{ly-12}" width="{width}" height="16" rx="2" fill="#ffffff" opacity=".96"/>')
    lines.append(f'<text x="{lx}" y="{ly}" text-anchor="middle" class="label">{label}</text>')


lines.append('<text x="800" y="38" text-anchor="middle" class="title">广告流批一体湖仓系统总体架构</text>')
lines.append('<text x="800" y="60" text-anchor="middle" class="sub">主数据流从左向右；Paimon 内部统一 ODS、DWD、DWS、DIM 与 ADS 表模型</text>')

section(32, 90, 220, 570, "01", "DATA SOURCES")
section(272, 90, 245, 570, "02", "INGESTION")
section(537, 90, 245, 570, "03", "STREAM &amp; BATCH")
section(802, 90, 470, 570, "04", "PAIMON LAKEHOUSE", "#f9fafb")
section(1292, 90, 276, 570, "05", "SERVING")

cylinder(62, 160, 160, 135, "业务系统 OLTP", ("MySQL / PostgreSQL", "广告、计划、订单", "Demo：MySQL"))
node(62, 420, 160, 130, "用户行为事件", ("Web / App 埋点 SDK", "曝光、点击、转化、下单", "Demo：Python Generator"))

node(302, 175, 185, 110, "Flink CDC 3.x", ("YAML 声明式流水线", "全量 + 增量", "Schema 自动演化"), dashed=True)
node(302, 430, 185, 110, "埋点采集服务 / API", ("HTTP / gRPC", "校验、鉴权、限流", "Demo：直写 Kafka"), dashed=True)

cylinder(567, 415, 185, 105, "Apache Kafka", ("Topic：ods_log", "实时事件消息总线"))
node(567, 540, 185, 85, "Flink Streaming", ("长期运行、秒级处理", "实时写入与窗口聚合"))
node(567, 205, 185, 85, "Flink Batch", ("历史重算与数据回溯", "DWS / ADS 专题计算"))

node(832, 145, 410, 65, "ADS 应用层", ("留存分析  |  归因分析  |  反作弊",), "#e5e7eb")
node(832, 245, 410, 78, "DWS 汇总层", ("消耗、GMV、曝光、点击、转化、订单", "CTR、CVR、ROI"))
node(832, 365, 250, 82, "DWD 明细层", ("清洗、去重、维度补全", "dwd_ad_events_di"))
node(1112, 365, 130, 82, "DIM 维度层", ("广告主、计划", "单元、创意"), "#f3f4f6")
node(832, 505, 410, 76, "ODS 原始层", ("ods_ad_events_di",), "#f3f4f6")
lines.append('<text x="1037" y="625" text-anchor="middle" class="small">统一 Schema · 主键语义 · Snapshot · Time Travel · Compaction</text>')

node(1325, 155, 210, 88, "Paimon External Catalog", ("目录已接入", "直读受本地版本兼容限制"), dashed=True)
cylinder(1325, 325, 210, 105, "StarRocks 内部 OLAP", ("快照表与查询视图", "当前稳定查询路径"))
node(1325, 510, 210, 82, "Apache Superset", ("指标看板", "交互式多维分析"))

arrow('M 222 225 H 302', '业务变更', 262, 215, True)
arrow('M 487 230 H 790 V 406 H 1112', '维表同步', 650, 220, True)
arrow('M 222 485 H 302', '行为事件', 262, 475, True)
arrow('M 487 485 H 567', '服务端写入', 527, 475, True)
arrow('M 660 520 V 540', '消费', 682, 536)
arrow('M 752 582 H 790 V 543 H 832', '实时写入', 790, 570)
arrow('M 957 505 V 447', '清洗', 978, 478)
arrow('M 1112 406 H 1082', '维度关联', 1097, 395)
arrow('M 957 365 V 323', '聚合', 978, 347)
arrow('M 1037 245 V 210', '专题加工', 1065, 230)
arrow('M 752 247 H 832', '历史回溯', 792, 237)
arrow('M 1242 177 H 1325', '目录直读', 1283, 167, True)
arrow('M 1242 284 H 1280 V 377 H 1325', '同步脚本', 1280, 310)
arrow('M 1430 430 V 510', 'SQL 查询', 1458, 474)

section(32, 690, 1536, 145, "06", "GOVERNANCE &amp; OPERATIONS", "#f9fafb")
node(72, 742, 320, 62, "Apicurio Schema Registry", ("Schema 注册、版本查询与治理",))
node(440, 742, 320, 62, "DolphinScheduler", ("批任务编排、调度与失败重试",))
node(808, 742, 320, 62, "Prometheus + Grafana", ("指标采集、运行状态与趋势监控",))
node(1176, 742, 320, 62, "Loki", ("集中日志查询与故障定位",))

lines.append('<line x1="480" y1="867" x2="530" y2="867" stroke="#374151" stroke-width="1.5" marker-end="url(#arrow)"/>')
lines.append('<text x="540" y="871" class="small">当前稳定运行路径</text>')
lines.append('<line x1="735" y1="867" x2="785" y2="867" stroke="#9ca3af" stroke-width="1.5" stroke-dasharray="5,4" marker-end="url(#arrow-light)"/>')
lines.append('<text x="795" y="871" class="small">设计或配置路径</text>')
lines.append('<rect x="965" y="854" width="32" height="20" rx="3" fill="#f3f4f6" stroke="#9ca3af"/>')
lines.append('<text x="1007" y="870" class="small">持久化或状态组件</text>')
lines.append('</svg>')

OUT.mkdir(parents=True, exist_ok=True)
SVG.write_text("\n".join(lines), encoding="utf-8")
ET.parse(SVG)
print(SVG)
