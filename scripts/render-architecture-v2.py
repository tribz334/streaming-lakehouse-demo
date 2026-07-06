from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "figuers" / "architecture" / "streaming-lakehouse-architecture-v2.png"
REG = "C:/Windows/Fonts/msyh.ttc"
BOLD = "C:/Windows/Fonts/msyhbd.ttc"


def f(size, bold=False):
    return ImageFont.truetype(BOLD if bold else REG, size)


img = Image.new("RGB", (1900, 1050), "white")
d = ImageDraw.Draw(img)


def rect(x, y, w, h, title, lines=(), gray=False, dashed=False, fs=14):
    fill = "#eeeeee" if gray else "white"
    if dashed:
        for xx in range(x, x + w, 16):
            d.line((xx, y, min(xx + 9, x + w), y), fill="black", width=2)
            d.line((xx, y + h, min(xx + 9, x + w), y + h), fill="black", width=2)
        for yy in range(y, y + h, 16):
            d.line((x, yy, x, min(yy + 9, y + h)), fill="black", width=2)
            d.line((x + w, yy, x + w, min(yy + 9, y + h)), fill="black", width=2)
    else:
        d.rectangle((x, y, x + w, y + h), fill=fill, outline="black", width=2)
    d.text((x + w / 2, y + 10), title, font=f(fs + 2, True), fill="black", anchor="ma")
    for i, line in enumerate(lines):
        d.text((x + w / 2, y + 38 + i * 22), line, font=f(fs), fill="black", anchor="ma")


def zone(x, y, w, h, title, gray=False):
    d.rectangle((x, y, x + w, y + h), fill="#f7f7f7" if gray else "white", outline="black", width=2)
    d.text((x + 14, y + 10), title, font=f(18, True), fill="black")


def arrow(points, label="", dashed=False):
    for a, b in zip(points, points[1:]):
        d.line((*a, *b), fill="black", width=2)
    x, y = points[-1]
    px, py = points[-2]
    if abs(x - px) > abs(y - py):
        s = 1 if x > px else -1
        tip = [(x, y), (x - 11 * s, y - 6), (x - 11 * s, y + 6)]
    else:
        s = 1 if y > py else -1
        tip = [(x, y), (x - 6, y - 11 * s), (x + 6, y - 11 * s)]
    d.polygon(tip, fill="black")
    if label:
        mx, my = points[len(points) // 2]
        d.text((mx + 5, my - 20), label, font=f(12), fill="black")


d.text((950, 25), "广告流批一体湖仓系统总体架构", font=f(28, True), fill="black", anchor="ma")
d.text((950, 62), "主数据流从左向右；Paimon 内部按 ODS → DWD → DWS → ADS 分层", font=f(14), fill="#444444", anchor="ma")

zone(35, 100, 270, 560, "数据源")
rect(70, 170, 200, 145, "业务系统 OLTP", ("MySQL / PostgreSQL", "广告主 · 计划 · 单元", "创意 · 订单 · 预算", "Demo：MySQL"), gray=True, fs=12)
rect(70, 440, 200, 145, "用户行为事件", ("Web / App 埋点 SDK", "曝光 · 点击 · 转化 · 下单", "Demo：Python Generator"), fs=12)

zone(335, 100, 290, 560, "数据接入")
rect(375, 190, 210, 105, "Flink CDC 3.x", ("YAML 声明式流水线", "全量 + 增量 + Schema 演化", "Demo 稳定链路：JDBC"), dashed=True, fs=11)
rect(375, 460, 210, 105, "埋点采集服务 / API", ("HTTP / gRPC", "校验 · 鉴权 · 限流", "Demo：Generator 直写 Kafka"), dashed=True, fs=11)

zone(655, 100, 290, 560, "消息与统一计算")
rect(695, 235, 210, 85, "Flink Batch", ("历史重算 · 数据回溯", "DWS / ADS 专题计算"), fs=12)
rect(695, 445, 210, 85, "Apache Kafka", ("Topic：ods_log", "事件消息总线"), gray=True, fs=12)
rect(695, 555, 210, 80, "Flink Streaming", ("长期运行 · 秒级处理", "实时写入与窗口聚合"), fs=12)

zone(975, 100, 500, 560, "Apache Paimon 流批一体湖仓")
rect(1015, 165, 420, 60, "ADS 应用层", ("留存 | 归因 | 反作弊 | 数据质量",), gray=True, fs=12)
rect(1015, 255, 420, 75, "DWS 汇总层", ("消耗 · GMV · 曝光 · 点击 · 转化 · 订单", "CTR · CVR · ROI"), fs=12)
rect(1015, 370, 260, 80, "DWD 明细层", ("清洗 · 去重 · 维度补全", "dwd_ad_events_di"), fs=12)
rect(1305, 370, 130, 80, "DIM 维度层", ("广告主 · 计划", "单元 · 创意"), gray=True, fs=11)
rect(1015, 505, 420, 75, "ODS 原始层", ("ods_ad_events_di",), gray=True, fs=12)
d.text((1225, 625), "统一 Schema · 主键语义 · Snapshot · Time Travel · Compaction", font=f(12), fill="#444444", anchor="ma")

zone(1505, 100, 280, 560, "查询与应用")
rect(1540, 170, 210, 85, "External Catalog", ("目录已接入", "直读受版本兼容限制"), dashed=True, fs=11)
rect(1540, 325, 210, 95, "StarRocks 内部 OLAP", ("快照表 · 查询视图", "当前稳定查询路径"), gray=True, fs=12)
rect(1540, 495, 210, 85, "Apache Superset", ("指标看板", "交互式多维分析"), fs=12)

zone(35, 700, 1750, 200, "横向治理与运维能力（不参与主数据流）", gray=True)
rect(90, 765, 340, 85, "Apicurio Schema Registry", ("Schema 注册 · 版本查询 · 治理",), fs=12)
rect(520, 765, 340, 85, "DolphinScheduler", ("批任务编排 · 定时调度 · 失败重试",), fs=12)
rect(950, 765, 340, 85, "Prometheus + Grafana", ("指标采集 · 状态与趋势监控",), fs=12)
rect(1380, 765, 340, 85, "Loki", ("集中日志查询 · 故障定位",), fs=12)

arrow([(270, 242), (375, 242)], "业务变更")
arrow([(585, 242), (960, 242), (960, 410), (1305, 410)], "维表同步")
arrow([(270, 512), (375, 512)], "生产上报")
arrow([(585, 512), (695, 487)], "服务端写入")
arrow([(800, 530), (800, 555)], "消费")
arrow([(905, 595), (960, 595), (960, 542), (1015, 542)], "实时写入")
arrow([(1145, 505), (1145, 450)], "清洗")
arrow([(1305, 410), (1275, 410)], "关联")
arrow([(1145, 370), (1145, 330)], "聚合")
arrow([(1225, 255), (1225, 225)], "专题加工")
arrow([(905, 277), (1015, 292)], "历史回溯")
arrow([(1435, 195), (1540, 212)], "目录直读")
arrow([(1435, 292), (1490, 292), (1490, 372), (1540, 372)], "同步脚本")
arrow([(1645, 420), (1645, 495)], "SQL 查询")

d.text((910, 945), "实线：当前稳定运行路径　　虚线边框：设计或配置路径　　灰底：持久化/状态组件", font=f(13), fill="black", anchor="ma")
img.save(OUT)
print(OUT)
