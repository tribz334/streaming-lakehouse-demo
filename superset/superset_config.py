SECRET_KEY = "ustc-streaming-lakehouse-demo-secret"
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
BABEL_DEFAULT_TIMEZONE = "Asia/Shanghai"
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
