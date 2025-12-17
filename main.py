# 标准库：base64 用来解 Pub/Sub 的 data；datetime 用来生成时间字段；json 解析/生成 JSON；os 读环境变量；random/time 做退避重试；typing 做类型提示
import base64
import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

# 第三方：requests 用于拉取媒体流；Flask 用来做 Cloud Run 的 HTTP 服务入口
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
    time.sleep(delay)


def _safe_author(author: str) -> str:
    author = (author or "unknown").strip()
    return author.replace("/", "_").replace("\\", "_")


def _date_folder(ts: Any) -> str:
    if ts is None:
        return "unknown-date"
    try:
        if isinstance(ts, (int, float)):
            d = dt.datetime.utcfromtimestamp(ts).date()
        else:
            d = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
        return d.isoformat()
    except Exception:
        return "unknown-date"


def _infer_ext(content_type: Optional[str], media_type: str) -> str:
    """
    根据 Content-Type 推断后缀：
    - video: mp4/webm/mov（保持你原逻辑）
    - image: jpg/png/gif/webp/svg
    """
    if not content_type:
        return ".mp4" if media_type == "video" else ".jpg"

    ct = content_type.split(";")[0].strip().lower()

    if media_type == "video":
        if ct == "video/mp4":
            return ".mp4"
        if ct == "video/webm":
            return ".webm"
        if ct in ("video/quicktime",):
            return ".mov"
        return ".mp4"

    # image
    if ct in ("image/jpeg", "image/jpg"):
        return ".jpg"
    if ct == "image/png":
        return ".png"
    if ct == "image/gif":
        return ".gif"
    if ct == "image/webp":
        return ".webp"
    if ct == "image/svg+xml":
        return ".svg"
    # 不认识的也给 jpg
    return ".jpg"


def _gcs_public_url(bucket: str, object_name: str) -> str:
    """
    生成 GCS 的公网 URL 形式（不保证一定可访问，取决于 bucket/object ACL/IAM）。
    """
    return f"https://storage.googleapis.com/{bucket}/{quote(object_name, safe='/')}"


def _get_key_val(payload: Dict[str, Any], key_field: str) -> str:
    """
    统一取主键值：优先 payload[key_field]，再 fallback 到 uuid/shortcode/id，再兜底 url hash。
    """
    key_val = payload.get(key_field) or payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not key_val:
        # 兼容图片/视频：优先 media_url，再退到 video_url/image_url
        u = payload.get("media_url") or payload.get("video_url") or payload.get("image_url") or ""
        key_val = f"noid-{abs(hash(u))}"
    return str(key_val)


def _normalize_prefix(prefix: str) -> str:
    prefix = (prefix or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def build_video_object_name(payload: Dict[str, Any]) -> str:
    """
    保持你原来的视频对象路径构造逻辑不变（重要：不影响已有数据路径）。
    """
    vid = payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not vid:
        vid = f"noid-{abs(hash(payload.get('video_url', '')))}"

    author = _safe_author(payload.get("author") or "unknown")
    date_str = _date_folder(payload.get("timestamp"))
    filename = f"{vid}.mp4"

    prefix = _normalize_prefix(GCS_PREFIX)
    # 原逻辑：raw/instagram/{author}/{date}/{vid}.mp4
    return f"{prefix}instagram/{author}/{date_str}/{filename}"


def build_image_object_name(payload: Dict[str, Any]) -> str:
    """
    图片单独存储：在同一 author/date 下增加 images/ 子目录。
    示例：raw/instagram/{author}/{date}/images/{uuid}.jpg
    """
    img_id = payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not img_id:
        img_id = f"noid-{abs(hash(payload.get('image_url', '') or payload.get('media_url', '') or ''))}"

    author = _safe_author(payload.get("author") or "unknown")
    date_str = _date_folder(payload.get("timestamp"))

    # 先用 jpg，后续会按 Content-Type 自动替换
    filename = f"{img_id}.jpg"

    prefix = _normalize_prefix(GCS_PREFIX)
    return f"{prefix}instagram/{author}/{date_str}/images/{filename}"


def http_get_stream(url: str) -> Tuple[requests.Response, str]:
    """
    发起可流式读取的 GET 请求；返回 (response, content_type)。
    注意：调用方必须 r.close() 释放连接。
    """
    headers = {"User-Agent": DEFAULT_UA, "Accept": "*/*", "Connection": "keep-alive"}
    session = requests.Session()

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=headers, stream=True, timeout=HTTP_TIMEOUT)
            if r.status_code in RETRYABLE_STATUS:
                r.close()
                _sleep_backoff(attempt)
                continue
            return r, (r.headers.get("Content-Type", "") or "")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            _sleep_backoff(attempt)

    raise RuntimeError(f"Failed GET after {MAX_RETRIES} attempts. last_err={last_err!r}")


def upload_stream_to_gcs(media_url: str, object_name: str, media_type: str) -> Dict[str, Any]:
    """
    从 media_url 以流方式下载，并以流方式上传到 GCS。
    media_type: "video" | "image"
    返回 {ok, gcs_uri, skipped, object_name, content_type, ...}
    """
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)

    if GCS_CHUNK_MB > 0:
        blob.chunk_size = GCS_CHUNK_MB * 1024 * 1024

    r, content_type = http_get_stream(media_url)

    if r.status_code in NONRETRYABLE_STATUS:
        r.close()
        return {"ok": False, "error": f"HTTP {r.status_code}", "media_url": media_url}

    try:
        r.raise_for_status()
    except Exception as e:
        r.close()
        return {"ok": False, "error": f"bad_status: {e}", "media_url": media_url}

    content_type_main = (content_type.split(";")[0].strip() if content_type else "")
    ext = _infer_ext(content_type_main, media_type)

    # 根据真实类型修正后缀
    if not object_name.lower().endswith(ext):
        if "." in object_name.split("/")[-1]:
            object_name = object_name.rsplit(".", 1)[0] + ext
        else:
            object_name = object_name + ext
        blob = bucket.blob(object_name)
        if GCS_CHUNK_MB > 0:
            blob.chunk_size = GCS_CHUNK_MB * 1024 * 1024

    if content_type_main:
        blob.content_type = content_type_main

    try:
        if not OVERWRITE:
            blob.upload_from_file(r.raw, rewind=False, if_generation_match=0)
            skipped = False
            reason = None
        else:
            blob.upload_from_file(r.raw, rewind=False)
            skipped = False
            reason = None
    except PreconditionFailed:
        skipped = True
        reason = "already_exists"
    except (TooManyRequests, ServiceUnavailable) as e:
        r.close()
        raise e
    finally:
        r.close()

    return {
        "ok": True,
        "skipped": skipped,
        "reason": reason,
        "gcs_uri": f"gs://{GCS_BUCKET}/{object_name}",
        "object_name": object_name,
        "content_type": content_type_main or "",
    }


def upsert_bigquery(
    payload: Dict[str, Any],
    media_type: str,
    gcs_uri: str,
    status: str,
    public_url: str,
    content_type: str,
    error: Optional[str] = None,
) -> None:
    """
    将处理结果写回 BigQuery（BQ_TABLE），用 MERGE 实现幂等与可重试。

    兼容策略：
    - 通用字段：media_type/raw_url/gcs_uri/public_url/content_type/status/author/src_timestamp/error/updated_at
    - 视频专用字段：raw_video_url/video_public_url（保持兼容）
    - 图片专用字段：raw_image_url/image_public_url
    """
    key_val = _get_key_val(payload, BQ_KEY_FIELD)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # 原始 URL：兼容多种字段名
    raw_video_url = str(payload.get("video_url") or payload.get("videoUrl") or "")
    raw_image_url = str(payload.get("image_url") or payload.get("imageUrl") or payload.get("img_url") or "")
    raw_url = str(payload.get("media_url") or (raw_video_url if media_type == "video" else raw_image_url) or "")

    video_public_url = public_url if media_type == "video" else ""
    image_public_url = public_url if media_type == "image" else ""

    query = f"""
    MERGE `{table_id}` T
    USING (
      SELECT
        @key_val AS {BQ_KEY_FIELD},
        @media_type AS media_type,

        -- 通用
        @raw_url AS raw_url,
        @gcs_uri AS gcs_uri,
        @public_url AS public_url,
        @content_type AS content_type,

        -- 视频兼容字段
        @raw_video_url AS raw_video_url,
        @video_public_url AS video_public_url,

        -- 图片字段
        @raw_image_url AS raw_image_url,
        @image_public_url AS image_public_url,

        @status AS status,
        @author AS author,
        @src_timestamp AS src_timestamp,
        @error AS error,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.{BQ_KEY_FIELD} = S.{BQ_KEY_FIELD}
    WHEN MATCHED THEN
      UPDATE SET
        media_type = S.media_type,

        raw_url = S.raw_url,
        gcs_uri = S.gcs_uri,
        public_url = S.public_url,
        content_type = S.content_type,

        raw_video_url = S.raw_video_url,
        video_public_url = S.video_public_url,

        raw_image_url = S.raw_image_url,
        image_public_url = S.image_public_url,

        status = S.status,
        author = S.author,
        src_timestamp = S.src_timestamp,
        error = S.error,
        updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (
        {BQ_KEY_FIELD},
        media_type,

        raw_url, gcs_uri, public_url, content_type,

        raw_video_url, video_public_url,
        raw_image_url, image_public_url,

        status, author, src_timestamp, error, updated_at
      )
      VALUES (
        S.{BQ_KEY_FIELD},
        S.media_type,

        S.raw_url, S.gcs_uri, S.public_url, S.content_type,

        S.raw_video_url, S.video_public_url,
        S.raw_image_url, S.image_public_url,

        S.status, S.author, S.src_timestamp, S.error, S.updated_at
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("key_val", "STRING", str(key_val)),
            bigquery.ScalarQueryParameter("media_type", "STRING", media_type),

            bigquery.ScalarQueryParameter("raw_url", "STRING", raw_url),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri or ""),
            bigquery.ScalarQueryParameter("public_url", "STRING", public_url or ""),
            bigquery.ScalarQueryParameter("content_type", "STRING", content_type or ""),

            bigquery.ScalarQueryParameter("raw_video_url", "STRING", raw_video_url),
            bigquery.ScalarQueryParameter("video_public_url", "STRING", video_public_url),

            bigquery.ScalarQueryParameter("raw_image_url", "STRING", raw_image_url),
            bigquery.ScalarQueryParameter("image_public_url", "STRING", image_public_url),

            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("author", "STRING", str(payload.get("author") or "")),
            bigquery.ScalarQueryParameter("src_timestamp", "STRING", str(payload.get("timestamp") or "")),
            bigquery.ScalarQueryParameter("error", "STRING", error or ""),
        ]
    )

    bq_client.query(query, job_config=job_config).result()


def parse_pubsub_push(req_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    解析 Pub/Sub push body，把 message.data 的 base64 JSON 解出来。
    同时做字段兼容：
    - video_url/videoUrl
    - image_url/imageUrl/img_url
    - type: video/image（默认 video，保证旧消息不改也能跑）
    """
    msg = (req_json or {}).get("message") or {}
    data_b64 = msg.get("data")
    if not data_b64:
        raise ValueError("Missing message.data")

    decoded = base64.b64decode(data_b64).decode("utf-8")
    payload = json.loads(decoded)

    # type 兼容：默认 video
    media_type = (payload.get("type") or payload.get("media_type") or "video")
    media_type = str(media_type).strip().lower()
    if media_type not in ("video", "image"):
        media_type = "video"
    payload["type"] = media_type

    # 视频 URL 兼容
    if "video_url" not in payload and "videoUrl" in payload:
        payload["video_url"] = payload["videoUrl"]

    # 图片 URL 兼容
    if "image_url" not in payload:
        if "imageUrl" in payload:
            payload["image_url"] = payload["imageUrl"]
        elif "img_url" in payload:
            payload["image_url"] = payload["img_url"]

    # 统一一个 media_url 便于兜底
    if "media_url" not in payload:
        payload["media_url"] = payload.get("video_url") or payload.get("image_url") or ""

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
        # Pub/Sub push：204 代表 ack（不重试），这里保持你原风格
        return jsonify({"ok": False, "error": f"bad_payload: {e}"}), 204

    media_type = payload.get("type") or "video"
    media_type = str(media_type).strip().lower()

    # 根据 type 选择 URL
    if media_type == "image":
        media_url = payload.get("image_url") or payload.get("media_url")
        if not media_url:
            return jsonify({"ok": False, "error": "missing image_url"}), 204
        object_name = payload.get("gcs_object") or build_image_object_name(payload)
    else:
        # 默认 video：保持原逻辑
        media_url = payload.get("video_url") or payload.get("media_url")
        if not media_url:
            return jsonify({"ok": False, "error": "missing video_url"}), 204
        object_name = payload.get("gcs_object") or build_video_object_name(payload)

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            up = upload_stream_to_gcs(media_url, object_name, media_type=media_type)

            # 永久失败：写入 BQ failed，并 ack
            if not up.get("ok"):
                upsert_bigquery(
                    payload,
                    media_type=media_type,
                    gcs_uri="",
                    status="failed",
                    public_url="",
                    content_type="",
                    error=up.get("error"),
                )
                return jsonify({"ok": True, "status": "failed", "error": up.get("error")}), 200

            gcs_uri = up["gcs_uri"]
            final_object_name = up.get("object_name") or object_name
            public_url = _gcs_public_url(GCS_BUCKET, final_object_name)
            content_type = up.get("content_type") or ""

            status = "done" if not up.get("skipped") else "done_skipped"

            # 写回 BQ：同一张表，按 media_type 分别填充字段
            upsert_bigquery(
                payload,
                media_type=media_type,
                gcs_uri=gcs_uri,
                status=status,
                public_url=public_url,
                content_type=content_type,
                error="",
            )

            # 返回响应（便于你手动测试）
            return jsonify(
                {
                    "ok": True,
                    "type": media_type,
                    "status": status,
                    "gcs_uri": gcs_uri,
                    "public_url": public_url,
                    "content_type": content_type,
                }
            ), 200

        except (TooManyRequests, ServiceUnavailable) as e:
            last_err = e
            _sleep_backoff(attempt)

    return jsonify({"ok": False, "error": f"transient_failure: {last_err!r}"}), 500
