from pathlib import Path
import xml.etree.ElementTree as ET
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "figuers" / "architecture"
SVG = OUT / "streaming-lakehouse-fireworks-vertical.svg"
PNG = OUT / "streaming-lakehouse-fireworks-vertical.png"
W, H = 1100, 1500
FONT = "C:/Windows/Fonts/msyh.ttc"
BOLD = "C:/Windows/Fonts/msyhbd.ttc"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


svg = []
svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">')
svg.append('<style>text{font-family:"Microsoft YaHei","PingFang SC","SimHei",Arial,sans-serif}.t{font-size:25px;font-weight:700}.h{font-size:17px;font-weight:700}.n{font-size:15px;font-weight:600}.b{font-size:12px}.s{font-size:11px;fill:#4b5563}.l{font-size:11px}</style>')
svg.append('<defs><marker id="up" markerWidth="13" markerHeight="12" refX="6.5" refY="1" orient="auto"><path d="M1,11 L6.5,1 L12,11 Z" fill="#fff" stroke="#111" stroke-width="1.2"/></marker><marker id="thin" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0,0 8,3 0,6" fill="#111"/></marker></defs>')
svg.append('<rect width="1100" height="1500" fill="#fff"/>')


def group(x, y, w, h, title):
    svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="#fff" stroke="#111" stroke-width="1.3" stroke-dasharray="8,5"/>')
    svg.append(f'<text x="{x+14}" y="{y+28}" class="h">{esc(title)}</text>')


def box(x, y, w, h, title, body=(), gray=False, dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ''
    fill = "#f3f4f6" if gray else "#fff"
    svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" stroke="#111" stroke-width="1.5"{dash}/>')
    svg.append(f'<text x="{x+w/2}" y="{y+25}" text-anchor="middle" class="n">{esc(title)}</text>')
    for i, v in enumerate(body):
        svg.append(f'<text x="{x+w/2}" y="{y+47+i*18}" text-anchor="middle" class="b">{esc(v)}</text>')


def cylinder(x, y, w, h, title, body=()):
    svg.append(f'<rect x="{x}" y="{y+13}" width="{w}" height="{h-26}" fill="#f3f4f6"/>')
    svg.append(f'<ellipse cx="{x+w/2}" cy="{y+13}" rx="{w/2}" ry="13" fill="#fff" stroke="#111" stroke-width="1.5"/>')
    svg.append(f'<line x1="{x}" y1="{y+13}" x2="{x}" y2="{y+h-13}" stroke="#111" stroke-width="1.5"/><line x1="{x+w}" y1="{y+13}" x2="{x+w}" y2="{y+h-13}" stroke="#111" stroke-width="1.5"/>')
    svg.append(f'<ellipse cx="{x+w/2}" cy="{y+h-13}" rx="{w/2}" ry="13" fill="#e5e7eb" stroke="#111" stroke-width="1.5"/>')
    svg.append(f'<text x="{x+w/2}" y="{y+42}" text-anchor="middle" class="n">{esc(title)}</text>')
    for i, v in enumerate(body):
        svg.append(f'<text x="{x+w/2}" y="{y+64+i*18}" text-anchor="middle" class="b">{esc(v)}</text>')


def arrow(path, label, x, y, thick=False, dashed=False):
    width = 2.6 if thick else 1.5
    dash = ' stroke-dasharray="6,4"' if dashed else ''
    marker = "up" if thick else "thin"
    svg.append(f'<path d="{path}" fill="none" stroke="#111" stroke-width="{width}"{dash} marker-end="url(#{marker})"/>')
    if label:
        bw = max(52, len(label) * 13)
        svg.append(f'<rect x="{x-bw/2}" y="{y-14}" width="{bw}" height="19" fill="#fff"/>')
        svg.append(f'<text x="{x}" y="{y}" text-anchor="middle" class="l">{esc(label)}</text>')


svg.append('<text x="550" y="38" text-anchor="middle" class="t">广告流批一体湖仓系统总体架构</text>')

group(40, 65, 1020, 190, "数据应用平台")
box(145, 105, 810, 55, "Apache Superset", ("广告经营、归因、反作弊与质量分析看板",))
cylinder(370, 165, 360, 85, "StarRocks 内部 OLAP", ("快照表 · 查询视图 · 即席分析",))

group(40, 280, 1020, 705, "流批一体湖仓")
cylinder(160, 330, 780, 535, "Apache Paimon", ("统一 Schema · 主键语义 · Snapshot · Time Travel · Compaction",))
box(220, 405, 660, 65, "ADS 应用层", ("留存分析  |  归因分析  |  反作弊  |  数据质量",), gray=True)
box(220, 500, 660, 75, "DWS 汇总层", ("消耗 · GMV · 曝光 · 点击 · 转化 · 订单", "CTR · CVR · ROI"))
box(220, 610, 430, 85, "DWD 明细层", ("清洗 · 去重 · 维度补全", "dwd_ad_events_di"))
box(690, 610, 190, 85, "DIM 维度层", ("广告主 · 计划", "单元 · 创意"), gray=True)
box(220, 735, 660, 70, "ODS 原始层", ("ods_ad_events_di",), gray=True)
box(100, 885, 360, 70, "Flink Streaming", ("实时写入 · 10 秒窗口聚合",))
box(640, 885, 360, 70, "Flink Batch", ("历史重算 · DWS/ADS 专题计算",))

group(40, 1010, 1020, 180, "数据采集")
box(95, 1060, 270, 85, "Flink CDC 3.x", ("YAML：全量 + 增量", "Schema 自动演化"), dashed=True)
box(415, 1060, 270, 85, "埋点采集服务 / API", ("校验 · 鉴权 · 限流", "Demo：Generator 直写"), dashed=True)
cylinder(735, 1055, 270, 95, "Apache Kafka", ("Topic：ods_log", "实时事件消息总线"))

group(40, 1215, 1020, 150, "数据源")
cylinder(130, 1260, 340, 80, "业务系统 OLTP", ("MySQL / PostgreSQL · Demo 使用 MySQL",))
box(630, 1260, 340, 80, "用户行为事件", ("Web/App 埋点 SDK", "曝光 · 点击 · 转化 · 下单"))

group(40, 1390, 1020, 80, "治理与运维")
svg.append('<text x="550" y="1438" text-anchor="middle" class="b">Apicurio Schema Registry　｜　DolphinScheduler　｜　Prometheus + Grafana　｜　Loki</text>')

arrow('M550,165 V160', '', 590, 170, True)
arrow('M550,330 V250', '同步 / 直读', 615, 290, True, True)
arrow('M550,500 V470', '', 602, 489)
arrow('M435,610 V575', '', 465, 598)
arrow('M690,652 H650', '维度关联', 670, 640)
arrow('M435,735 V695', '', 465, 720)
arrow('M280,885 V805', '实时写入', 335, 855, True)
arrow('M820,885 H930 V537 H880', '历史回溯', 955, 735, True)
arrow('M870,1055 V985 H280 V955', '消费事件', 860, 1015, True)
arrow('M300,1260 V1145', 'CDC / JDBC', 345, 1215, True, True)
arrow('M230,1060 V1000 H1025 V652 H880', '维表同步', 1035, 825, False, True)
arrow('M800,1260 V1145 H550', '行为上报', 760, 1205, True, True)
arrow('M685,1102 H735', '服务端写入', 710, 1090, True, True)

svg.append('</svg>')
OUT.mkdir(parents=True, exist_ok=True)
SVG.write_text("\n".join(svg), encoding="utf-8")
ET.parse(SVG)


def ff(size, bold=False):
    return ImageFont.truetype(BOLD if bold else FONT, size)


img = Image.new("RGB", (W, H), "white")
d = ImageDraw.Draw(img)


def pgroup(x, y, w, h, title):
    for xx in range(x, x+w, 14):
        d.line((xx, y, min(xx+8, x+w), y), fill="black", width=1)
        d.line((xx, y+h, min(xx+8, x+w), y+h), fill="black", width=1)
    for yy in range(y, y+h, 14):
        d.line((x, yy, x, min(yy+8, y+h)), fill="black", width=1)
        d.line((x+w, yy, x+w, min(yy+8, y+h)), fill="black", width=1)
    d.text((x+14, y+10), title, font=ff(17, True), fill="black")


def pbox(x, y, w, h, title, body=(), gray=False, dashed=False):
    d.rectangle((x, y, x+w, y+h), fill="#f1f1f1" if gray else "white", outline="black", width=2)
    d.text((x+w/2, y+12), title, font=ff(15, True), fill="black", anchor="ma")
    for i, v in enumerate(body):
        d.text((x+w/2, y+38+i*18), v, font=ff(12), fill="black", anchor="ma")


def pcyl(x, y, w, h, title, body=()):
    d.rectangle((x, y+12, x+w, y+h-12), fill="#f1f1f1")
    d.ellipse((x, y, x+w, y+25), fill="white", outline="black", width=2)
    d.line((x, y+12, x, y+h-12), fill="black", width=2)
    d.line((x+w, y+12, x+w, y+h-12), fill="black", width=2)
    d.ellipse((x, y+h-25, x+w, y+h), fill="#e5e5e5", outline="black", width=2)
    d.text((x+w/2, y+28), title, font=ff(15, True), fill="black", anchor="ma")
    for i, v in enumerate(body):
        d.text((x+w/2, y+52+i*18), v, font=ff(12), fill="black", anchor="ma")


d.text((550, 15), "广告流批一体湖仓系统总体架构", font=ff(25, True), fill="black", anchor="ma")
for args in [(40,65,1020,190,"数据应用平台"),(40,280,1020,705,"流批一体湖仓"),(40,1010,1020,180,"数据采集"),(40,1215,1020,150,"数据源"),(40,1390,1020,80,"治理与运维")]: pgroup(*args)
pbox(145,105,810,55,"Apache Superset",("广告经营、归因、反作弊与质量分析看板",))
pcyl(370,165,360,85,"StarRocks 内部 OLAP",("快照表 · 查询视图 · 即席分析",))
pcyl(160,330,780,535,"Apache Paimon",("统一 Schema · 主键语义 · Snapshot · Time Travel · Compaction",))
pbox(220,405,660,65,"ADS 应用层",("留存分析 | 归因分析 | 反作弊 | 数据质量",),True)
pbox(220,500,660,75,"DWS 汇总层",("消耗 · GMV · 曝光 · 点击 · 转化 · 订单","CTR · CVR · ROI"))
pbox(220,610,430,85,"DWD 明细层",("清洗 · 去重 · 维度补全","dwd_ad_events_di"))
pbox(690,610,190,85,"DIM 维度层",("广告主 · 计划","单元 · 创意"),True)
pbox(220,735,660,70,"ODS 原始层",("ods_ad_events_di",),True)
pbox(100,885,360,70,"Flink Streaming",("实时写入 · 10 秒窗口聚合",))
pbox(640,885,360,70,"Flink Batch",("历史重算 · DWS/ADS 专题计算",))
pbox(95,1060,270,85,"Flink CDC 3.x",("YAML：全量 + 增量","Schema 自动演化"))
pbox(415,1060,270,85,"埋点采集服务 / API",("校验 · 鉴权 · 限流","Demo：Generator 直写"))
pcyl(735,1055,270,95,"Apache Kafka",("Topic：ods_log","实时事件消息总线"))
pcyl(130,1260,340,80,"业务系统 OLTP",("MySQL / PostgreSQL · Demo 使用 MySQL",))
pbox(630,1260,340,80,"用户行为事件",("Web/App 埋点 SDK","曝光 · 点击 · 转化 · 下单"))
d.text((550,1425), "Apicurio Schema Registry ｜ DolphinScheduler ｜ Prometheus + Grafana ｜ Loki", font=ff(12), fill="black", anchor="ma")


def parr(points, label=""):
    d.line(points, fill="black", width=3, joint="curve")
    x,y=points[-1]; px,py=points[-2]
    if abs(x-px)>abs(y-py):
        s=1 if x>px else -1; tip=[(x,y),(x-11*s,y-6),(x-11*s,y+6)]
    else:
        s=1 if y>py else -1; tip=[(x,y),(x-6,y-11*s),(x+6,y-11*s)]
    d.polygon(tip, fill="black")
    if label:
        mx,my=points[len(points)//2]; d.text((mx+8,my-18),label,font=ff(11),fill="black")


parr([(550,165),(550,160)])
parr([(550,330),(550,250)],"同步 / 直读")
parr([(550,500),(550,470)])
parr([(435,610),(435,575)])
parr([(690,652),(650,652)],"关联")
parr([(435,735),(435,695)])
parr([(280,885),(280,805)],"实时写入")
parr([(820,885),(930,885),(930,537),(880,537)],"历史回溯")
parr([(870,1055),(870,985),(280,985),(280,955)],"消费事件")
parr([(300,1260),(300,1145)],"CDC/JDBC")
parr([(230,1060),(230,1000),(1025,1000),(1025,652),(880,652)],"维表同步")
parr([(800,1260),(800,1180),(550,1180),(550,1145)],"行为上报")
parr([(685,1102),(735,1102)],"写入")
img.save(PNG)
print(SVG)
print(PNG)
