import json, os

env = os.getenv("SUPERSET_ENV", "development")
if env == "development":
    env_file = "config-local.json"
else:
    assert False

config_json = {}
with open(env_file, "r", encoding="utf-8") as f:
    config_json = json.load(f)

SECRET_KEY = config_json["SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{config_json['MYSQL_USER']}:{config_json['MYSQL_PASSWORD']}@{config_json['MYSQL_HOST']}:{config_json['MYSQL_PORT']}/{config_json['MYSQL_DB']}{config_json['MYSQL_PARAMS']}"
ENABLE_PROXY_FIX = True

SESSION_COOKIE_SAMESITE = None
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = False

WTF_CSRF_ENABLED = False
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

GUEST_ROLE_NAME = "Gamma"
GUEST_TOKEN_JWT_SECRET = "superset-cpocloud-dashboard"
GUEST_TOKEN_JWT_ALGO = "HS256"
GUEST_TOKEN_HEADER_NAME = "X-GuestToken"
GUEST_TOKEN_JWT_EXP_SECONDS = 3600

PUBLIC_ROLE_LIKE_GAMMA = True
WTF_CSRF_ENABLED = False

TALISMAN_ENABLED = True

# WTF_CSRF_ENABLED = True
# WTF_CSRF_TIME_LIMIT = 300

HTML_SANITIZATION = True
HTML_SANITIZATION_SCHEMA_EXTENSIONS = {
    "attributes": {
        "*": ["className"],
    },
    "tagNames": ["style"],
}

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "CLIENT_CACHE": True,
    "DASHBOARD_FILTERS_EXPERIMENTAL": True,
    "DASHBOARD_CACHE": True,
    "EMBEDDED_SUPERSET": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ESCAPE_MARKDOWN_HTML": False,
}

TALISMAN_CONFIG = {
    "content_security_policy": {
        "default-src": ["'self'"],
        "img-src": ["'self'", "blob:", "data:", "*"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": [
            "'self'",
            "https://api.mapbox.com",
            "https://events.mapbox.com",
        ],
        "object-src": "'none'",
        "style-src": [
            "'self'",
            "'unsafe-inline'",
        ],
        "script-src": ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": False,
    "frame_options": "ALLOWFROM",
    "frame_options_allow_from": "*",
}

OVERRIDE_HTTP_HEADERS = {"X-Frame-Options": "ALLOWALL"}
HTTP_HEADERS = {"X-Frame-Options": "ALLOWALL"}

ENABLE_CORS = True

CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "allow_methods": ["*"],
    "origins": ["*"],
    "allow_origins": ["*"],
}

EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "guage_chart",
        "description": "",
        "label": "Colors of Guage Chart",
        "isDefault": False,
        "colors": ["#F65050", "#F6A050", "#00A743", "#00A743", "#68A4FF"],
    }
]
