# 标准库：base64 用来解 Pub/Sub 的 data；datetime 用来生成时间字段；json 解析/生成 JSON；os 读环境变量；random/time 做退避重试；typing 做类型提示
import base64
import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

# 第三方：requests 用于拉取视频流；Flask 用来做 Cloud Run 的 HTTP 服务入口
import requests
from flask import Flask, request, jsonify

# GCP SDK：storage 操作 GCS；bigquery 操作 BQ；exceptions 用于捕获幂等/限流等异常
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import PreconditionFailed, TooManyRequests, ServiceUnavailable

# 创建 Flask 应用对象（Cloud Run 通过 HTTP 请求调用它）
app = Flask(__name__)

# ---------------------------
# Env config (Cloud Run)
# ---------------------------

# 目标 GCS bucket 名称（必须配置，否则服务启动就会 KeyError）
GCS_BUCKET = os.environ["GCS_BUCKET"]

# GCS 路径前缀（可选；比如 raw/ 或 videos/；默认 raw/）
GCS_PREFIX = os.getenv("GCS_PREFIX", "raw/")

# BigQuery 项目 ID（必须；用于定位表）
BQ_PROJECT = os.environ["BQ_PROJECT"]

# BigQuery 数据集名称（必须）
BQ_DATASET = os.environ["BQ_DATASET"]

# BigQuery 表名（必须）
BQ_TABLE = os.environ["BQ_TABLE"]

# BigQuery 的主键字段名（可选；默认 uuid；用于 MERGE 的 ON 条件）
BQ_KEY_FIELD = os.getenv("BQ_KEY_FIELD", "uuid")

# 可选：简单共享密钥，用于防止别的来源直接调用你的 endpoint（生产建议用 OIDC/IAM 更标准）
SHARED_SECRET = os.getenv("SHARED_SECRET")

# 下载时使用的 User-Agent（有些源站会根据 UA 返回不同结果；默认给一个常见浏览器 UA）
DEFAULT_UA = os.getenv(
    "DOWNLOADER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

# 下载超时时间（秒）；大文件一般不会在 handshake 上太久，这里控制请求层超时
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))

# 下载/上传重试次数（包含网络波动、429/503 等）
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# GCS 可恢复上传分块大小（MB）；越大上传效率可能更高，但内存/网络波动风险也更大
GCS_CHUNK_MB = int(os.getenv("GCS_CHUNK_MB", "8"))

# 是否允许覆盖同名对象；默认 false 表示幂等：如果对象已存在就跳过
OVERWRITE = os.getenv("OVERWRITE", "false").lower() == "true"

# 创建 GCS 客户端（使用 Cloud Run 的服务账号身份或 GOOGLE_APPLICATION_CREDENTIALS 指定的身份）
storage_client = storage.Client()

# 创建 BigQuery 客户端（同上）
bq_client = bigquery.Client(project=BQ_PROJECT)

# 定义“可重试”的 HTTP 状态码：这些通常是暂时性问题（超时/限流/服务端错误）
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}

# 定义“不可重试”的 HTTP 状态码：这些多为永久性失败（权限/不存在等）
NONRETRYABLE_STATUS = {400, 401, 403, 404, 410}


def _now_iso() -> str:
    # 返回 UTC 当前时间的 ISO 字符串（带时区），用于健康检查或日志输出
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()


def _sleep_backoff(attempt: int) -> None:
    # 指数退避 + 抖动：attempt=1 等 ~1s；attempt=2 等 ~2s；……最大 60s
    delay = min(60.0, (2 ** (attempt - 1)) + random.random())
    # 休眠，用于等待下一次重试
    time.sleep(delay)


def _safe_author(author: str) -> str:
    # author 为空时给默认值 unknown
    author = (author or "unknown").strip()
    # 替换路径分隔符，防止生成非法/危险的 GCS 路径
    return author.replace("/", "_").replace("\\", "_")


def _date_folder(ts: Any) -> str:
    # 如果没有 timestamp，就放到 unknown-date 目录
    if ts is None:
        return "unknown-date"
    try:
        # 如果 timestamp 是 epoch 秒（数字），用 utcfromtimestamp 解析日期
        if isinstance(ts, (int, float)):
            d = dt.datetime.utcfromtimestamp(ts).date()
        else:
            # 如果是 ISO 字符串，兼容 Z 结尾
            d = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
        # 返回 YYYY-MM-DD
        return d.isoformat()
    except Exception:
        # 解析失败则使用 unknown-date
        return "unknown-date"


def _infer_ext(content_type: Optional[str]) -> str:
    # 没有 content-type 时默认 mp4
    if not content_type:
        return ".mp4"
    # 取主类型，忽略 charset 之类的参数
    ct = content_type.split(";")[0].strip().lower()
    # 常见 mp4
    if ct == "video/mp4":
        return ".mp4"
    # 可能是 webm
    if ct == "video/webm":
        return ".webm"
    # 可能是 mov
    if ct in ("video/quicktime",):
        return ".mov"
    # 其他不认识的也先用 mp4
    return ".mp4"


def _gcs_public_url(bucket: str, object_name: str) -> str:
    """
    生成 GCS 的公网 URL 形式（不保证一定可访问，取决于 bucket/object ACL/IAM）。
    使用 storage.googleapis.com 形式，并对 object_name 做 URL 编码，但保留路径分隔符。
    """
    return f"https://storage.googleapis.com/{bucket}/{quote(object_name, safe='/')}"


def _get_key_val(payload: Dict[str, Any], key_field: str) -> str:
    """
    统一取主键值：优先 payload[key_field]，再 fallback 到 uuid/shortcode/id，再兜底 url hash。
    """
    key_val = payload.get(key_field) or payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not key_val:
        key_val = f"noid-{abs(hash(payload.get('video_url', '')))}"
    return str(key_val)


def build_object_name(payload: Dict[str, Any]) -> str:
    """
    根据 payload 构造 GCS 对象路径（核心：幂等 + 可追踪）。
    """
    # 优先使用 uuid/shortcode/id 作为唯一标识，方便去重
    vid = payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    # 如果完全没有唯一 ID，则用 URL 的 hash 做兜底（不完美，但保证稳定）
    if not vid:
        vid = f"noid-{abs(hash(payload.get('video_url', '')))}"

    # author 用来分目录（方便按账号管理）
    author = _safe_author(payload.get("author") or "unknown")
    # timestamp 用来按日期分目录（方便生命周期策略和分析）
    date_str = _date_folder(payload.get("timestamp"))

    # 默认文件名先用 .mp4，后面会根据 Content-Type 调整后缀
    filename = f"{vid}.mp4"

    # 规范化前缀：去掉开头的 / 防止变成绝对路径；并确保以 / 结尾
    prefix = (GCS_PREFIX or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # 返回最终对象路径（示例：raw/instagram/huawei/2025-12-12/Cxyz.mp4）
    return f"{prefix}instagram/{author}/{date_str}/{filename}"


def http_get_stream(url: str) -> Tuple[requests.Response, str]:
    """
    发起可流式读取的 GET 请求；返回 (response, content_type)。
    注意：调用方必须 r.close() 释放连接。
    """
    # 设置请求 header，尽量模拟正常浏览器行为
    headers = {"User-Agent": DEFAULT_UA, "Accept": "*/*", "Connection": "keep-alive"}
    # 用 Session 复用连接（对多次请求更友好；这里每次调用会创建一个 session）
    session = requests.Session()

    # 记录最后一个异常，便于报错时定位
    last_err = None
    # 进行最多 MAX_RETRIES 次重试
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # stream=True 表示不要一次性把视频读进内存
            r = session.get(url, headers=headers, stream=True, timeout=HTTP_TIMEOUT)
            # 如果是可重试状态码（比如 429/503），关闭响应并退避后重试
            if r.status_code in RETRYABLE_STATUS:
                r.close()
                _sleep_backoff(attempt)
                continue
            # 返回响应对象和 Content-Type（供后续选择后缀）
            return r, (r.headers.get("Content-Type", "video/mp4") or "video/mp4")
        except (requests.Timeout, requests.ConnectionError) as e:
            # 网络超时/连接问题属于暂时性，保存错误并退避重试
            last_err = e
            _sleep_backoff(attempt)

    # 多次尝试仍失败则抛异常，让上层决定是否让 Pub/Sub 重试
    raise RuntimeError(f"Failed GET after {MAX_RETRIES} attempts. last_err={last_err!r}")


def upload_stream_to_gcs(video_url: str, object_name: str) -> Dict[str, Any]:
    """
    从 video_url 以流方式下载，并以流方式上传到 GCS。
    返回 {ok, gcs_uri, skipped, object_name, ...}。
    """
    # 获取 bucket 对象
    bucket = storage_client.bucket(GCS_BUCKET)
    # 获取 blob（对象）句柄
    blob = bucket.blob(object_name)

    # 配置分块大小，帮助 resumable upload 稳定传大文件
    if GCS_CHUNK_MB > 0:
        blob.chunk_size = GCS_CHUNK_MB * 1024 * 1024

    # 请求视频流（注意：r 需要关闭）
    r, content_type = http_get_stream(video_url)

    # 如果是不可重试（比如 403/404），直接返回失败（并不触发 Pub/Sub 重试）
    if r.status_code in NONRETRYABLE_STATUS:
        r.close()
        return {"ok": False, "error": f"HTTP {r.status_code}", "video_url": video_url}

    try:
        # 其他状态码若不是 2xx 会在这里抛异常
        r.raise_for_status()
    except Exception as e:
        # 关闭响应，避免连接泄漏
        r.close()
        # 返回错误给上层
        return {"ok": False, "error": f"bad_status: {e}", "video_url": video_url}

    # 取主 Content-Type（去掉 charset 等参数）
    content_type = content_type.split(";")[0].strip()
    # 根据 Content-Type 推断文件后缀
    ext = _infer_ext(content_type)

    # 如果对象名后缀不匹配真实类型，调整对象名（例如从 .mp4 改 .webm）
    if not object_name.lower().endswith(ext):
        # 如果文件名带后缀，替换后缀
        if "." in object_name.split("/")[-1]:
            object_name = object_name.rsplit(".", 1)[0] + ext
        else:
            # 否则直接追加后缀
            object_name = object_name + ext
        # 重新创建 blob（因为路径变了）
        blob = bucket.blob(object_name)
        # 重新设置 chunk size
        if GCS_CHUNK_MB > 0:
            blob.chunk_size = GCS_CHUNK_MB * 1024 * 1024

    # 给 GCS 对象标注 content type，方便后续使用（浏览器、CDN、分析）
    blob.content_type = content_type

    # 默认做幂等：只在对象不存在时创建（if_generation_match=0）
    try:
        if not OVERWRITE:
            # r.raw 是底层流，upload_from_file 会边读边写，不落盘
            blob.upload_from_file(r.raw, rewind=False, if_generation_match=0)
            skipped = False
            reason = None
        else:
            # 允许覆盖：直接上传（同名会覆盖旧对象）
            blob.upload_from_file(r.raw, rewind=False)
            skipped = False
            reason = None
    except PreconditionFailed:
        # 说明对象已经存在（幂等跳过），这通常发生于重复消息/重试
        skipped = True
        reason = "already_exists"
    except (TooManyRequests, ServiceUnavailable) as e:
        # GCS 侧的限流/服务不可用：属于暂时性，抛给上层，让上层返回 500 触发 Pub/Sub 重试
        r.close()
        raise e
    finally:
        # 无论成功/失败，都要关闭响应，释放连接
        r.close()

    # 返回上传结果，包含 gs:// 路径 + 最终 object_name（用于生成公网 URL）
    return {
        "ok": True,
        "skipped": skipped,
        "reason": reason,
        "gcs_uri": f"gs://{GCS_BUCKET}/{object_name}",
        "object_name": object_name,
        "content_type": content_type,
    }


def upsert_bigquery(
    payload: Dict[str, Any],
    gcs_uri: str,
    status: str,
    video_public_url: str,
    error: Optional[str] = None,
) -> None:
    """
    将处理结果写回 BigQuery（BQ_TABLE）：
    - 有记录：UPDATE
    - 无记录：INSERT
    用 MERGE 实现幂等与“可重试”。

    新增字段：video_public_url（按你的字段名拼写）
    """
    key_val = _get_key_val(payload, BQ_KEY_FIELD)

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    query = f"""
    MERGE `{table_id}` T
    USING (
      SELECT
        @key_val AS {BQ_KEY_FIELD},
        @raw_video_url AS raw_video_url,
        @gcs_uri AS gcs_uri,
        @status AS status,
        @author AS author,
        @src_timestamp AS src_timestamp,
        @video_public_url AS video_public_url,
        @error AS error,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.{BQ_KEY_FIELD} = S.{BQ_KEY_FIELD}
    WHEN MATCHED THEN
      UPDATE SET
        raw_video_url = S.raw_video_url,
        gcs_uri = S.gcs_uri,
        status = S.status,
        author = S.author,
        src_timestamp = S.src_timestamp,
        video_public_url = S.video_public_url,
        error = S.error,
        updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT ({BQ_KEY_FIELD}, raw_video_url, gcs_uri, status, author, src_timestamp, video_public_url, error, updated_at)
      VALUES (S.{BQ_KEY_FIELD}, S.raw_video_url, S.gcs_uri, S.status, S.author, S.src_timestamp, S.video_public_url, S.error, S.updated_at)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("key_val", "STRING", str(key_val)),
            bigquery.ScalarQueryParameter(
                "raw_video_url", "STRING", str(payload.get("video_url") or payload.get("videoUrl") or "")
            ),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri or ""),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("author", "STRING", str(payload.get("author") or "")),
            bigquery.ScalarQueryParameter("src_timestamp", "STRING", str(payload.get("timestamp") or "")),
            bigquery.ScalarQueryParameter("video_public_url", "STRING", video_public_url or ""),
            bigquery.ScalarQueryParameter("error", "STRING", error or ""),
        ]
    )

    bq_client.query(query, job_config=job_config).result()


def parse_pubsub_push(req_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    解析 Pub/Sub push body，把 message.data 的 base64 JSON 解出来。
    """
    msg = (req_json or {}).get("message") or {}
    data_b64 = msg.get("data")
    if not data_b64:
        raise ValueError("Missing message.data")

    decoded = base64.b64decode(data_b64).decode("utf-8")
    payload = json.loads(decoded)

    if "video_url" not in payload:
        if "videoUrl" in payload:
            payload["video_url"] = payload["videoUrl"]

    return payload


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "time": _now_iso()}), 200


@app.post("/pubsub/push")
def pubsub_push():
    if SHARED_SECRET:
        got = request.headers.get("X-Shared-Secret")
        if got != SHARED_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    req_json = request.get_json(silent=True) or {}
    try:
        payload = parse_pubsub_push(req_json)
    except Exception as e:
        return jsonify({"ok": False, "error": f"bad_payload: {e}"}), 204

    video_url = payload.get("video_url")
    if not video_url:
        return jsonify({"ok": False, "error": "missing video_url"}), 204

    object_name = payload.get("gcs_object") or build_object_name(payload)

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            up = upload_stream_to_gcs(video_url, object_name)

            # 永久失败：写入 BQ_TABLE failed，并 ack
            if not up.get("ok"):
                upsert_bigquery(payload, gcs_uri="", status="failed", video_public_url="", error=up.get("error"))
                return jsonify({"ok": True, "status": "failed", "error": up.get("error")}), 200

            gcs_uri = up["gcs_uri"]
            final_object_name = up.get("object_name") or object_name

            # 新增：生成公网 URL（格式）
            video_public_url = _gcs_public_url(GCS_BUCKET, final_object_name)

            status = "done" if not up.get("skipped") else "done_skipped"

            # 1) 写回 BQ_TABLE：新增 video_public_url 字段
            upsert_bigquery(payload, gcs_uri=gcs_uri, status=status, video_public_url=video_public_url, error="")

            return jsonify(
                {"ok": True, "status": status, "gcs_uri": gcs_uri, "video_public_url": video_public_url}
            ), 200

        except (TooManyRequests, ServiceUnavailable) as e:
            last_err = e
            _sleep_backoff(attempt)

    return jsonify({"ok": False, "error": f"transient_failure: {last_err!r}"}), 500
