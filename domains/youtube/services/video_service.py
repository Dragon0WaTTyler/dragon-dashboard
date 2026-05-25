import time

from flask import render_template, url_for


class YouTubeVideoService:
    def __init__(
        self,
        *,
        get_video_detail_context,
        recommendation_service,
        get_section_route,
        ai_context_for_video,
        score_display,
        score_color,
        yts_url,
        build_query_url,
    ):
        self.get_video_detail_context = get_video_detail_context
        self.recommendation_service = recommendation_service
        self.get_section_route = get_section_route
        self.ai_context_for_video = ai_context_for_video
        self.score_display = score_display
        self.score_color = score_color
        self.yts_url = yts_url
        self.build_query_url = build_query_url

    def _perf_log(self, event, **fields):
        parts = [f"event={event}"]
        for key, value in fields.items():
            if isinstance(value, float):
                value = f"{value:.2f}"
            elif isinstance(value, bool):
                value = int(value)
            if value in ("", None):
                continue
            parts.append(f"{key}={value}")
        message = "[youtube-perf] " + " ".join(parts)
        safe_message = message.encode("ascii", errors="backslashreplace").decode("ascii")
        print(safe_message)

    def _estimate_context_sizes(self, context):
        context = context if isinstance(context, dict) else {}
        entry = context.get("entry", {}) if isinstance(context.get("entry", {}), dict) else {}
        return {
            "related_page_count": len(context.get("related_entries", []) or []),
            "related_full_count": len(context.get("related_entries_full", []) or []),
            "playlist_entry_count": len(context.get("playlist_entries", []) or []),
            "entry_field_count": len(entry),
            "title_len": len(str(entry.get("title", entry.get("name", "")) or "")),
            "playlist_name_len": len(str(entry.get("playlist_name", "") or "")),
        }

    def render_video_detail(self, entry_id, query_args):
        route_started_at = time.monotonic()
        force_refresh = str(query_args.get("refresh", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
            "force",
        }
        context_started_at = time.monotonic()
        context = self.get_video_detail_context(entry_id, force_refresh=force_refresh)
        context_elapsed_ms = (time.monotonic() - context_started_at) * 1000
        if not context:
            render_started_at = time.monotonic()
            rendered = render_template(
                "video_detail.html",
                missing=True,
                entry={
                    "entry_id": entry_id,
                    "title": "Missing Video",
                    "name": "Missing Video",
                    "playlist_name": "",
                    "playlist_url": "",
                    "video_id": "",
                    "duration": "",
                    "section": "",
                    "url": "",
                },
                entry_type="youtube",
                related_title="Missing video",
                player_video_id="",
                prev_entry=None,
                next_entry=None,
                related_entries=[],
                related_total_pages=0,
                related_page=1,
                pagination_numbers=[],
                related_order="normal",
                related_seed="",
                delete_endpoint=False,
                ai_default_mode="cinematic",
                ai_page_context="general",
            )
            render_elapsed_ms = (time.monotonic() - render_started_at) * 1000
            self._perf_log(
                "video_open_missing",
                entry_id=entry_id,
                force_refresh=force_refresh,
                context_ms=context_elapsed_ms,
                render_ms=render_elapsed_ms,
                total_ms=(time.monotonic() - route_started_at) * 1000,
            )
            return (rendered, 404)

        enrich_started_at = time.monotonic()
        context = self.recommendation_service.enrich_video_detail_context(
            context,
            entry_id,
            query_args,
            get_section_route=self.get_section_route,
            ai_context_for_video=self.ai_context_for_video,
            url_for=url_for,
        )
        enrich_elapsed_ms = (time.monotonic() - enrich_started_at) * 1000
        context_shape = self._estimate_context_sizes(context)
        self._perf_log(
            "video_render_context",
            entry_id=entry_id,
            **context_shape,
            entry_type=context.get("entry_type", ""),
            section=(context.get("entry", {}) or {}).get("section", ""),
        )
        render_started_at = time.monotonic()
        rendered = render_template(
            "video_detail.html",
            missing=False,
            **context,
            score_display=self.score_display,
            score_color=self.score_color,
            yts_url=self.yts_url,
            build_query_url=self.build_query_url,
        )
        render_elapsed_ms = (time.monotonic() - render_started_at) * 1000
        self._perf_log(
            "video_open",
            entry_id=entry_id,
            force_refresh=force_refresh,
            context_ms=context_elapsed_ms,
            render_prep_ms=enrich_elapsed_ms,
            render_ms=render_elapsed_ms,
            total_ms=(time.monotonic() - route_started_at) * 1000,
            response_chars=len(rendered),
            **context_shape,
            entry_type=context.get("entry_type", ""),
            section=(context.get("entry", {}) or {}).get("section", ""),
            playlist_name=(context.get("entry", {}) or {}).get("playlist_name", ""),
        )
        return rendered
