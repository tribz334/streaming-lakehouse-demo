from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "figuers" / "architecture" / "streaming-lakehouse-architecture-preview.png"
FONT = "C:/Windows/Fonts/msyh.ttc"


def font(size, bold=False):
    path = "C:/Windows/Fonts/msyhbd.ttc" if bold else FONT
    return ImageFont.truetype(path, size)


img = Image.new("RGB", (1600, 1080), "white")
d = ImageDraw.Draw(img)


def box(x, y, w, h, title, lines=(), gray=False, dashed=False):
    fill = "#eeeeee" if gray else "white"
    if dashed:
        for xx in range(x, x + w, 14):
            d.line((xx, y, min(xx + 8, x + w), y), fill="black", width=2)
            d.line((xx, y + h, min(xx + 8, x + w), y + h), fill="black", width=2)
        for yy in range(y, y + h, 14):
            d.line((x, yy, x, min(yy + 8, y + h)), fill="black", width=2)
            d.line((x + w, yy, x + w, min(yy + 8, y + h)), fill="black", width=2)
    else:
        d.rectangle((x, y, x + w, y + h), fill=fill, outline="black", width=2)
    d.text((x + w / 2, y + 9), title, fill="black", font=font(18, True), anchor="ma")
    for i, line in enumerate(lines):
        d.text((x + w / 2, y + 38 + i * 23), line, fill="black", font=font(15), anchor="ma")


def layer(y, h, label):
    d.rectangle((35, y, 1190, y + h), outline="black", width=2)
    d.text((50, y + 8), label, fill="black", font=font(18, True))


def arrow(x1, y1, x2, y2, label="", dashed=False):
    d.line((x1, y1, x2, y2), fill="black", width=2)
    d.polygon([(x2, y2), (x2 - 7, y2 + 12), (x2 + 7, y2 + 12)], fill="black")
    if label:
        d.text(((x1 + x2) / 2 + 8, (y1 + y2) / 2), label, fill="black", font=font(13))


d.text((800, 25), "广告流批一体湖仓系统总体架构", fill="black", font=font(27, True), anchor="ma")

layer(70, 125, "05  查询与应用")
box(95, 112, 275, 62, "Apache Superset", ("指标看板 · 交互分析",))
box(455, 107, 300, 72, "StarRocks 内部 OLAP", ("快照表 · 查询视图 · 即席分析",), gray=True)
box(840, 112, 285, 62, "Paimon External Catalog", ("已配置；直读受版本兼容限制",), dashed=True)

layer(215, 330, "04  Apache Paimon 流批一体湖仓")
box(95, 258, 1030, 48, "ADS 应用层", ("留存分析 | 归因分析 | 反作弊",), gray=True)
box(95, 326, 1030, 52, "DWS 汇总层", ("消耗 · GMV · 曝光 · 点击 · 转化 · 订单 · CTR · CVR · ROI",))
box(95, 402, 650, 70, "DWD 明细层", ("清洗 · 去重 · 维度补全", "dwd_ad_events_di"))
box(785, 402, 340, 70, "DIM 维度层", ("广告主 · 计划 · 单元 · 创意",))
box(95, 492, 1030, 38, "ODS 原始层", ("ods_ad_events_di",), gray=True)

layer(565, 130, "03  统一计算引擎：Apache Flink")
box(140, 608, 390, 65, "Flink Streaming", ("实时写入 · 10 秒窗口聚合",))
box(690, 608, 390, 65, "Flink Batch", ("历史重算 · DWS/ADS 专题计算",))

layer(715, 125, "02  数据接入")
box(75, 758, 300, 63, "Flink CDC 3.x Pipeline", ("全量 + 增量 + Schema 演化",), dashed=True)
box(450, 758, 300, 63, "埋点采集服务 / API", ("校验 · 鉴权 · 限流 · 批量上报",), dashed=True)
box(825, 753, 300, 73, "Apache Kafka", ("ods_log · 实时事件消息总线",), gray=True)

layer(860, 135, "01  数据源")
box(95, 905, 410, 72, "业务系统 OLTP 数据库", ("MySQL / PostgreSQL", "广告主 · 计划 · 单元 · 创意 · 订单"), gray=True)
box(700, 905, 410, 72, "用户行为事件", ("Web/App 埋点 SDK", "曝光 · 点击 · 转化 · 下单"))

d.rectangle((1220, 70, 1560, 995), outline="black", width=2)
d.text((1238, 82), "治理与运维", fill="black", font=font(18, True))
box(1260, 150, 260, 70, "Apicurio Registry", ("Schema 注册与版本治理",))
box(1260, 345, 260, 65, "DolphinScheduler", ("批任务编排与调度",))
box(1260, 545, 260, 65, "Prometheus + Grafana", ("指标采集与监控",))
box(1260, 650, 260, 60, "Loki", ("集中日志查询",))

arrow(610, 107, 610, 80)
arrow(610, 258, 610, 215)
arrow(610, 326, 610, 306)
arrow(420, 402, 420, 378)
arrow(955, 402, 745, 437, "维度关联")
arrow(610, 492, 420, 472, "清洗")
arrow(335, 608, 335, 545, "实时写入")
arrow(885, 608, 885, 545, "历史回溯")
arrow(975, 753, 335, 695, "事件流")
arrow(300, 905, 225, 821, "CDC/JDBC")
arrow(905, 905, 600, 821, "生产上报")
arrow(750, 790, 825, 790, "服务端写入")

img.save(OUT)
print(OUT)
