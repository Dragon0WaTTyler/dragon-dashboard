#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as dragon_app  # noqa: E402


REQUEST_PROFILES = (
    "default",
    "browser_ua",
    "rss_accept",
)


def safe_print(message: str) -> None:
    text = str(message)
    try:
        print(text)
    except UnicodeEncodeError:
        stream = getattr(sys, "stdout", None)
        if stream is None:
            return
        encoding = getattr(stream, "encoding", None) or "utf-8"
        stream.write(text.encode(encoding, errors="replace").decode(encoding, errors="replace") + "\n")


def load_registry_payload(registry_path: str = "") -> tuple[Path, list[dict]]:
    resolved = dragon_app._resolve_reading_sources_registry_path(registry_path=registry_path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_sources = payload.get("sources", payload.get("reading_sources", []))
    elif isinstance(payload, list):
        raw_sources = payload
    else:
        raw_sources = []
    normalized = []
    for index, source in enumerate(raw_sources or []):
        if not isinstance(source, dict):
            continue
        normalized_source = dragon_app.normalize_reading_source(source, index)
        for key in (
            "disabled_reason",
            "repair_reason",
            "repaired_at",
            "replacement_of",
            "feed_url",
            "source_id",
        ):
            if key in source and key not in normalized_source:
                normalized_source[key] = source.get(key)
        normalized.append(normalized_source)
    return resolved, normalized


def reading_source_disabled_reason(source: dict) -> str:
    return str(source.get("disabled_reason", "") or "").strip()


def select_inactive_blocked_sources(sources: list[dict], source_names: list[str] | None = None) -> list[dict]:
    selected = []
    name_filters = {str(item or "").strip().lower() for item in (source_names or []) if str(item or "").strip()}
    for source in sources:
        if not isinstance(source, dict):
            continue
        if dragon_app.reading_source_active_flag(source):
            continue
        disabled_reason = reading_source_disabled_reason(source).lower()
        blocked = dragon_app.reading_source_is_blocked(source) or "403" in disabled_reason
        if not blocked:
            continue
        if name_filters:
            source_names_blob = {
                str(source.get("name", "") or "").strip().lower(),
                str(source.get("id", "") or "").strip().lower(),
                dragon_app.normalize_reading_url(source.get("url", "") or "").lower(),
            }
            if not any(candidate in source_names_blob for candidate in name_filters):
                continue
        selected.append(source)
    return selected


def build_wordpress_fallback_urls(source: dict) -> list[str]:
    source = source if isinstance(source, dict) else {}
    urls = []
    original_url = dragon_app.normalize_reading_url(source.get("url", "") or "")
    parsed = dragon_app.urllib.parse.urlsplit(original_url)
    if not parsed.scheme or not parsed.netloc:
        return urls

    root = f"{parsed.scheme}://{parsed.netloc}"
    category_slug = ""
    path_parts = [part for part in parsed.path.split("/") if part]
    if "category" in path_parts:
        category_index = path_parts.index("category")
        if category_index + 1 < len(path_parts):
            category_slug = path_parts[category_index + 1]

    def _add(candidate: str) -> None:
        normalized = dragon_app.normalize_reading_url(candidate)
        if normalized and normalized not in urls:
            urls.append(normalized)

    _add(f"{root}/feed")
    _add(f"{root}/rss")
    if category_slug:
        _add(f"{root}/category/{category_slug}/feed")
    return urls


def build_candidate_urls(source: dict) -> list[str]:
    candidates = []
    for candidate in list(dragon_app.reading_source_feed_candidate_urls(source)) + build_wordpress_fallback_urls(source):
        normalized = dragon_app.normalize_reading_url(candidate)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def build_profile_headers(profile_name: str, source: dict, candidate_url: str) -> dict:
    profile_name = str(profile_name or "").strip().lower()
    if profile_name == "default":
        return {}
    headers = {}
    if profile_name in {"browser_ua", "rss_accept"}:
        headers["User-Agent"] = dragon_app.READING_BROWSER_USER_AGENT
        headers["Accept-Language"] = dragon_app.READING_BROWSER_ACCEPT_LANGUAGE
    if profile_name == "rss_accept":
        headers["Accept"] = "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"
        source_url = dragon_app.normalize_reading_url((source or {}).get("url", "") or candidate_url)
        parsed_source = dragon_app.urllib.parse.urlsplit(source_url)
        if parsed_source.scheme and parsed_source.netloc:
            headers["Referer"] = f"{parsed_source.scheme}://{parsed_source.netloc}/"
    return headers


def strip_content_fields(entries: list[dict]) -> list[dict]:
    sanitized = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        item = dict(entry)
        item.pop("content_html", None)
        item.pop("content_text", None)
        sanitized.append(item)
    return sanitized


def parse_probe_entries(source: dict, candidate_url: str, payload_bytes: bytes) -> tuple[bool, list[dict], str]:
    service = dragon_app._get_reading_rss_service()
    feedparser_diag = dragon_app.reading_feedparser_diagnostics(payload_bytes or b"")
    source_topic = str(source.get("topic", "") or "").strip()
    try:
        root = dragon_app.ET.fromstring(payload_bytes or b"")
    except Exception as exc:
        if feedparser_diag.get("entry_count", 0) and dragon_app.feedparser is not None:
            parsed = dragon_app.feedparser.parse(payload_bytes or b"")
            items = [
                service.build_reading_import_item_from_feedparser(
                    source,
                    candidate_url,
                    entry,
                    source_topic=source_topic,
                )
                for entry in getattr(parsed, "entries", []) or []
            ]
            return True, strip_content_fields(items), "feedparser"
        return False, [], str(exc) or exc.__class__.__name__

    items = []
    feed_kind = "unknown"
    rss_items = []
    atom_items = []
    if root.tag.endswith("rss"):
        feed_kind = "rss"
        channel = root.find(".//{*}channel")
        if channel is not None:
            rss_items = channel.findall("./item")
        if not rss_items:
            rss_items = root.findall(".//{*}item")
    elif root.tag.endswith("RDF") or root.tag.endswith("rdf"):
        feed_kind = "rdf"
        rss_items = root.findall(".//{*}item")
    elif root.tag.endswith("feed"):
        feed_kind = "atom"
        atom_items = root.findall(".//{*}entry")

    for node in rss_items:
        items.append(service.build_reading_import_item(source, candidate_url, node, source_topic=source_topic))
    for node in atom_items:
        items.append(service.build_reading_import_item(source, candidate_url, node, source_topic=source_topic))
    return bool(items), strip_content_fields(items), feed_kind


def probe_candidate(source: dict, candidate_url: str, profile_name: str, timeout_seconds: int = 8, session=None) -> dict:
    request_session = session or dragon_app.requests.Session()
    headers = build_profile_headers(profile_name, source, candidate_url)
    result = {
        "source_name": str(source.get("name", "Unknown Source") or "Unknown Source").strip(),
        "source_id": str(source.get("id", "") or "").strip(),
        "original_url": dragon_app.normalize_reading_url(source.get("url", "") or ""),
        "candidate_url": dragon_app.normalize_reading_url(candidate_url),
        "profile": profile_name,
        "status_code": 0,
        "content_type": "",
        "parsed_feed": False,
        "normalized_article_count": 0,
        "recommended_action": "keep_blocked",
        "error": "",
        "verified": False,
        "items": [],
        "feed_kind": "",
    }
    try:
        response = request_session.get(
            result["candidate_url"],
            timeout=max(int(timeout_seconds or 0), 1),
            allow_redirects=True,
            headers=headers or None,
        )
    except dragon_app.requests.RequestException as exc:
        result["error"] = str(exc) or exc.__class__.__name__
        return result

    result["status_code"] = int(getattr(response, "status_code", 0) or 0)
    result["content_type"] = str(getattr(response, "headers", {}).get("Content-Type", "") or "").strip()
    payload_bytes = getattr(response, "content", b"") or b""
    if result["status_code"] >= 400:
        result["error"] = f"HTTP {result['status_code']}"
        return result

    parsed_feed, items, feed_kind_or_error = parse_probe_entries(source, result["candidate_url"], payload_bytes)
    result["parsed_feed"] = bool(parsed_feed)
    result["normalized_article_count"] = len(items)
    result["items"] = items
    if parsed_feed:
        result["feed_kind"] = feed_kind_or_error
        result["verified"] = len(items) > 0
        if result["verified"] and result["candidate_url"] == result["original_url"]:
            result["recommended_action"] = "reactivate"
        elif result["verified"]:
            result["recommended_action"] = "replace_url"
        return result

    result["error"] = feed_kind_or_error
    return result


def choose_best_probe_result(results: list[dict]) -> dict | None:
    verified = [item for item in (results or []) if isinstance(item, dict) and item.get("verified")]
    if verified:
        return sorted(
            verified,
            key=lambda item: (
                item.get("recommended_action") != "reactivate",
                -int(item.get("normalized_article_count", 0) or 0),
                REQUEST_PROFILES.index(item.get("profile")) if item.get("profile") in REQUEST_PROFILES else 99,
                item.get("candidate_url", ""),
            ),
        )[0]
    if not results:
        return None
    return sorted(
        [item for item in results if isinstance(item, dict)],
        key=lambda item: (
            int(item.get("status_code", 0) or 0) >= 400,
            -int(item.get("status_code", 0) or 0),
            item.get("candidate_url", ""),
        ),
    )[0]


def diagnose_source(source: dict, timeout_seconds: int = 8, session=None) -> dict:
    source = dragon_app.normalize_reading_source(source or {})
    probes = []
    for candidate_url in build_candidate_urls(source):
        for profile_name in REQUEST_PROFILES:
            probes.append(probe_candidate(source, candidate_url, profile_name, timeout_seconds=timeout_seconds, session=session))
    best = choose_best_probe_result(probes) or {}
    return {
        "source": source,
        "candidate_count": len(build_candidate_urls(source)),
        "probe_results": probes,
        "best_result": best,
        "recommended_action": str(best.get("recommended_action", "keep_blocked") or "keep_blocked"),
    }


def apply_verified_repairs(registry_sources: list[dict], reports: list[dict], repaired_at: str = "") -> tuple[list[dict], list[dict]]:
    repaired_at = str(repaired_at or "").strip() or dragon_app.current_timestamp()
    report_by_source_id = {}
    for report in reports or []:
        if not isinstance(report, dict):
            continue
        source = report.get("source", {})
        if not isinstance(source, dict):
            continue
        source_id = str(source.get("id", "") or "").strip()
        if source_id:
            report_by_source_id[source_id] = report

    updated_sources = []
    applied_repairs = []
    for index, source in enumerate(registry_sources or []):
        if not isinstance(source, dict):
            continue
        normalized = dragon_app.normalize_reading_source(source, index)
        report = report_by_source_id.get(str(normalized.get("id", "") or "").strip())
        if not report:
            updated_sources.append(dict(source))
            continue
        best = report.get("best_result", {})
        if not isinstance(best, dict) or not best.get("verified"):
            updated_sources.append(dict(source))
            continue

        candidate_url = dragon_app.normalize_reading_url(best.get("candidate_url", "") or "")
        original_url = dragon_app.normalize_reading_url(normalized.get("url", "") or "")
        action = str(best.get("recommended_action", "keep_blocked") or "keep_blocked")
        updated = dict(source)
        updated["active"] = True
        updated["repaired_at"] = repaired_at
        updated["repair_reason"] = (
            f"Verified by diagnose_reading_sources.py with profile={best.get('profile', 'default')} "
            f"status={int(best.get('status_code', 0) or 0)} count={int(best.get('normalized_article_count', 0) or 0)}"
        )
        updated.pop("disabled_reason", None)
        if action == "replace_url" and candidate_url and candidate_url != original_url:
            updated["replacement_of"] = original_url
            updated["url"] = candidate_url
            updated["primary_url"] = candidate_url
        applied_repairs.append({
            "source_name": str(normalized.get("name", "") or "").strip(),
            "action": action,
            "candidate_url": candidate_url,
        })
        updated_sources.append(updated)
    return updated_sources, applied_repairs


def write_registry_payload(registry_path: Path, sources: list[dict]) -> None:
    registry_path.write_text(json.dumps(list(sources or []), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def render_probe_table(reports: list[dict]) -> str:
    headers = [
        "source",
        "profile",
        "candidate_url",
        "status",
        "content_type",
        "parsed_feed",
        "normalized_articles",
        "recommended_action",
    ]
    rows = [headers]
    for report in reports or []:
        source = report.get("source", {})
        source_name = str((source or {}).get("name", "Unknown Source") or "Unknown Source").strip()
        for probe in report.get("probe_results", []) or []:
            rows.append([
                source_name,
                str(probe.get("profile", "") or ""),
                str(probe.get("candidate_url", "") or ""),
                str(probe.get("status_code", "") or ""),
                str(probe.get("content_type", "") or ""),
                "yes" if probe.get("parsed_feed") else "no",
                str(int(probe.get("normalized_article_count", 0) or 0)),
                str(probe.get("recommended_action", "keep_blocked") or "keep_blocked"),
            ])

    widths = [max(len(str(row[column])) for row in rows) for column in range(len(headers))]
    rendered = []
    for index, row in enumerate(rows):
        rendered.append(" | ".join(str(value).ljust(widths[column]) for column, value in enumerate(row)))
        if index == 0:
            rendered.append("-+-".join("-" * width for width in widths))
    return "\n".join(rendered)


def run_diagnosis(registry_path: str = "", source_names: list[str] | None = None, timeout_seconds: int = 8, apply: bool = False) -> int:
    resolved_registry_path, registry_sources = load_registry_payload(registry_path=registry_path)
    inactive_blocked_sources = select_inactive_blocked_sources(registry_sources, source_names=source_names)
    if not inactive_blocked_sources:
        safe_print("No inactive blocked reading sources matched the diagnosis filter.")
        return 0

    reports = [diagnose_source(source, timeout_seconds=timeout_seconds) for source in inactive_blocked_sources]
    safe_print(render_probe_table(reports))
    safe_print("")
    safe_print("Best candidate summary:")
    for report in reports:
        source = report.get("source", {})
        best = report.get("best_result", {})
        safe_print(
            f"- {source.get('name', 'Unknown Source')}: "
            f"action={report.get('recommended_action', 'keep_blocked')} | "
            f"url={best.get('candidate_url', '') or source.get('url', '')} | "
            f"profile={best.get('profile', 'n/a')} | "
            f"status={int(best.get('status_code', 0) or 0)} | "
            f"count={int(best.get('normalized_article_count', 0) or 0)}"
        )

    if not apply:
        return 0

    updated_sources, applied_repairs = apply_verified_repairs(registry_sources, reports)
    if not applied_repairs:
        safe_print("")
        safe_print("No verified repairs were applied.")
        return 0

    write_registry_payload(resolved_registry_path, updated_sources)
    safe_print("")
    safe_print(f"Applied {len(applied_repairs)} verified source repair(s) to {resolved_registry_path}")
    for repair in applied_repairs:
        safe_print(
            f"- {repair['source_name']}: action={repair['action']} | candidate_url={repair['candidate_url']}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose blocked Reading RSS sources without touching reading_data.json.")
    parser.add_argument("--registry-path", default="", help="Optional path to config/reading_sources.json")
    parser.add_argument("--source", action="append", default=[], help="Limit diagnosis to a specific source name, id, or URL")
    parser.add_argument("--timeout", type=int, default=8, help="Short timeout in seconds for each HTTP request")
    parser.add_argument("--apply", action="store_true", help="Apply verified source URL repairs to config/reading_sources.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_diagnosis(
        registry_path=args.registry_path,
        source_names=args.source,
        timeout_seconds=args.timeout,
        apply=bool(args.apply),
    )


if __name__ == "__main__":
    raise SystemExit(main())
