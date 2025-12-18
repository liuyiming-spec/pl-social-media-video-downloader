import base64
import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import requests
from flask import Flask, request, jsonify

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import PreconditionFailed, TooManyRequests, ServiceUnavailable

app = Flask(__name__)

# ---------------------------
# Env config (Cloud Run)
# ---------------------------
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_PREFIX = os.getenv("GCS_PREFIX", "raw/")

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE = os.environ["BQ_TABLE"]
BQ_KEY_FIELD = os.getenv("BQ_KEY_FIELD", "uuid")

SHARED_SECRET = os.getenv("SHARED_SECRET")

DEFAULT_UA = os.getenv(
    "DOWNLOADER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
GCS_CHUNK_MB = int(os.getenv("GCS_CHUNK_MB", "8"))
OVERWRITE = os.getenv("OVERWRITE", "false").lower() == "true"

storage_client = storage.Client()
bq_client = bigquery.Client(project=BQ_PROJECT)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
NONRETRYABLE_STATUS = {400, 401, 403, 404, 410}


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()


def _sleep_backoff(attempt: int) -> None:
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
    return ".jpg"


def _gcs_public_url(bucket: str, object_name: str) -> str:
    return f"https://storage.googleapis.com/{bucket}/{quote(object_name, safe='/')}"


def _normalize_prefix(prefix: str) -> str:
    prefix = (prefix or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def _get_key_val(payload: Dict[str, Any], key_field: str, fallback: Optional[str] = None) -> str:
    """
    主键：优先 payload[key_field]，再 fallback 到 uuid/shortcode/id，再兜底 fallback，再兜底 url hash。
    """
    key_val = payload.get(key_field) or payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not key_val and fallback:
        key_val = fallback
    if not key_val:
        u = payload.get("media_url") or payload.get("video_url") or payload.get("image_url") or ""
        key_val = f"noid-{abs(hash(u))}"
    return str(key_val)


def build_video_object_name(payload: Dict[str, Any]) -> str:
    vid = payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not vid:
        vid = f"noid-{abs(hash(payload.get('video_url', '')))}"

    author = _safe_author(payload.get("author") or "unknown")
    date_str = _date_folder(payload.get("timestamp"))
    filename = f"{vid}.mp4"

    prefix = _normalize_prefix(GCS_PREFIX)
    return f"{prefix}instagram/{author}/{date_str}/video/{filename}"


def build_image_object_name(payload: Dict[str, Any]) -> str:
    img_id = payload.get("uuid") or payload.get("shortcode") or payload.get("id")
    if not img_id:
        img_id = f"noid-{abs(hash(payload.get('image_url', '') or payload.get('media_url', '') or ''))}"

    author = _safe_author(payload.get("author") or "unknown")
    date_str = _date_folder(payload.get("timestamp"))
    filename = f"{img_id}.jpg"

    prefix = _normalize_prefix(GCS_PREFIX)
    return f"{prefix}instagram/{author}/{date_str}/images/{filename}"


def http_get_stream(url: str) -> Tuple[requests.Response, str]:
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
    pubsub_message_id: Optional[str] = None,
    http_status: Optional[int] = None,
) -> None:
    """
    meta 表字段建议：
      uuid, media_type, source_url, gcs_uri, public_url, content_type,
      status, author, src_timestamp, image_total, error, updated_at,
      pubsub_message_id, http_status
    """
    fallback_key = f"msg-{pubsub_message_id}" if pubsub_message_id else None
    key_val = _get_key_val(payload, BQ_KEY_FIELD, fallback=fallback_key)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    source_url = (
        payload.get("media_url")
        or (payload.get("image_url") if media_type == "image" else payload.get("video_url"))
        or ""
    )

    image_total = payload.get("image_total", payload.get("img_total"))
    try:
        image_total = int(image_total) if image_total is not None and str(image_total).strip() != "" else None
    except Exception:
        image_total = None

    query = f"""
    MERGE `{table_id}` T
    USING (
      SELECT
        @key_val AS {BQ_KEY_FIELD},
        @media_type AS media_type,
        @source_url AS source_url,
        @gcs_uri AS gcs_uri,
        @public_url AS public_url,
        @content_type AS content_type,
        @status AS status,
        @author AS author,
        @src_timestamp AS src_timestamp,
        @image_total AS image_total,
        @error AS error,
        @pubsub_message_id AS pubsub_message_id,
        @http_status AS http_status,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.{BQ_KEY_FIELD} = S.{BQ_KEY_FIELD}
    WHEN MATCHED THEN
      UPDATE SET
        media_type       = S.media_type,
        source_url       = S.source_url,
        gcs_uri          = S.gcs_uri,
        public_url       = S.public_url,
        content_type     = S.content_type,
        status           = S.status,
        author           = S.author,
        src_timestamp    = S.src_timestamp,
        image_total      = S.image_total,
        error            = S.error,
        pubsub_message_id= S.pubsub_message_id,
        http_status      = S.http_status,
        updated_at       = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (
        {BQ_KEY_FIELD},
        media_type,
        source_url,
        gcs_uri,
        public_url,
        content_type,
        status,
        author,
        src_timestamp,
        image_total,
        error,
        pubsub_message_id,
        http_status,
        updated_at
      )
      VALUES (
        S.{BQ_KEY_FIELD},
        S.media_type,
        S.source_url,
        S.gcs_uri,
        S.public_url,
        S.content_type,
        S.status,
        S.author,
        S.src_timestamp,
        S.image_total,
        S.error,
        S.pubsub_message_id,
        S.http_status,
        S.updated_at
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("key_val", "STRING", str(key_val)),
            bigquery.ScalarQueryParameter("media_type", "STRING", media_type),
            bigquery.ScalarQueryParameter("source_url", "STRING", str(source_url)),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri or ""),
            bigquery.ScalarQueryParameter("public_url", "STRING", public_url or ""),
            bigquery.ScalarQueryParameter("content_type", "STRING", content_type or ""),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("author", "STRING", str(payload.get("author") or "")),
            bigquery.ScalarQueryParameter("src_timestamp", "STRING", str(payload.get("timestamp") or "")),
            bigquery.ScalarQueryParameter("image_total", "INT64", image_total),
            bigquery.ScalarQueryParameter("error", "STRING", error or ""),
            bigquery.ScalarQueryParameter("pubsub_message_id", "STRING", pubsub_message_id or ""),
            bigquery.ScalarQueryParameter("http_status", "INT64", http_status),
        ]
    )

    bq_client.query(query, job_config=job_config).result()


def parse_pubsub_push_safe(req_json: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """
    返回：(payload_or_None, pubsub_message_id, error_or_empty)
    """
    msg = (req_json or {}).get("message") or {}
    message_id = msg.get("messageId") or ""
    data_b64 = msg.get("data")
    if not data_b64:
        return None, message_id, "Missing message.data"

    try:
        decoded = base64.b64decode(data_b64).decode("utf-8")
    except Exception as e:
        return None, message_id, f"Base64 decode error: {e!r}"

    try:
        payload = json.loads(decoded)
    except Exception as e:
        return None, message_id, f"JSON parse error: {e!r}"

    # 兼容：img_total -> image_total
    if "image_total" not in payload and "img_total" in payload:
        payload["image_total"] = payload["img_total"]

    # type 兼容：默认 video
    media_type = (payload.get("type") or payload.get("media_type") or "video")
    media_type = str(media_type).strip().lower()
    if media_type not in ("video", "image"):
        media_type = "video"
    payload["type"] = media_type

    # video_url 兼容
    if "video_url" not in payload and "videoUrl" in payload:
        payload["video_url"] = payload["videoUrl"]

    # image_url 兼容
    if "image_url" not in payload:
        if "imageUrl" in payload:
            payload["image_url"] = payload["imageUrl"]
        elif "img_url" in payload:
            payload["image_url"] = payload["img_url"]

    # 统一 media_url
    if "media_url" not in payload:
        payload["media_url"] = payload.get("video_url") or payload.get("image_url") or ""

    return payload, message_id, ""


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "time": _now_iso()}), 200


@app.post("/pubsub/push")
def pubsub_push():
    # 1) auth：无论是否授权，都尽量落库（用于观测）
    if SHARED_SECRET:
        got = request.headers.get("X-Shared-Secret")
        if got != SHARED_SECRET:
            req_json = request.get_json(silent=True) or {}
            payload, message_id, parse_err = parse_pubsub_push_safe(req_json)
            # 落库一条 unauthorized（payload 可能 None）
            upsert_bigquery(
                payload or {},
                media_type=(payload or {}).get("type") or "unknown",
                gcs_uri="",
                status="unauthorized",
                public_url="",
                content_type="",
                error=parse_err or "unauthorized",
                pubsub_message_id=message_id,
                http_status=401,
            )
            # ack（不要让 Pub/Sub 重试放大噪音）
            return jsonify({"ok": False, "error": "unauthorized"}), 200

    req_json = request.get_json(silent=True) or {}
    payload, message_id, parse_err = parse_pubsub_push_safe(req_json)

    # 2) bad payload：也落库（用 message_id 做 fallback key）
    if payload is None:
        upsert_bigquery(
            {},
            media_type="unknown",
            gcs_uri="",
            status="bad_payload",
            public_url="",
            content_type="",
            error=parse_err,
            pubsub_message_id=message_id,
            http_status=200,
        )
        return jsonify({"ok": True, "status": "bad_payload"}), 200

    media_type = (payload.get("type") or "video").strip().lower()
    if media_type not in ("video", "image"):
        media_type = "video"

    # 3) missing url：也落库（以前你是 204 直接丢）
    if media_type == "image":
        media_url = payload.get("image_url") or payload.get("media_url")
        if not media_url:
            upsert_bigquery(
                payload,
                media_type="image",
                gcs_uri="",
                status="missing_url",
                public_url="",
                content_type="",
                error="missing image_url",
                pubsub_message_id=message_id,
                http_status=200,
            )
            return jsonify({"ok": True, "status": "missing_url"}), 200
        object_name = payload.get("gcs_object") or build_image_object_name(payload)
    else:
        media_url = payload.get("video_url") or payload.get("media_url")
        if not media_url:
            upsert_bigquery(
                payload,
                media_type="video",
                gcs_uri="",
                status="missing_url",
                public_url="",
                content_type="",
                error="missing video_url",
                pubsub_message_id=message_id,
                http_status=200,
            )
            return jsonify({"ok": True, "status": "missing_url"}), 200
        object_name = payload.get("gcs_object") or build_video_object_name(payload)

    # 4) 正常下载：成功/失败都落库；transient 最终失败也落库
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            up = upload_stream_to_gcs(media_url, object_name, media_type=media_type)

            # 永久失败：落库 failed，并 ack
            if not up.get("ok"):
                upsert_bigquery(
                    payload,
                    media_type=media_type,
                    gcs_uri="",
                    status="failed",
                    public_url="",
                    content_type="",
                    error=up.get("error"),
                    pubsub_message_id=message_id,
                    http_status=200,
                )
                return jsonify({"ok": True, "status": "failed", "error": up.get("error")}), 200

            gcs_uri = up["gcs_uri"]
            final_object_name = up.get("object_name") or object_name
            public_url = _gcs_public_url(GCS_BUCKET, final_object_name)
            content_type = up.get("content_type") or ""

            status = "done" if not up.get("skipped") else "done_skipped"

            upsert_bigquery(
                payload,
                media_type=media_type,
                gcs_uri=gcs_uri,
                status=status,
                public_url=public_url,
                content_type=content_type,
                error="",
                pubsub_message_id=message_id,
                http_status=200,
            )

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

        except Exception as e:
            # 其他未知异常也算 transient
            last_err = e
            _sleep_backoff(attempt)

    # transient 最终失败：以前你这里直接 500 不落库；现在落库
    upsert_bigquery(
        payload,
        media_type=media_type,
        gcs_uri="",
        status="transient_failed",
        public_url="",
        content_type="",
        error=f"transient_failure: {last_err!r}",
        pubsub_message_id=message_id,
        http_status=200,
    )
    return jsonify({"ok": True, "status": "transient_failed"}), 200
