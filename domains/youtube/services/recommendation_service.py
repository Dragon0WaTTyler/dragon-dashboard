import time


class YouTubeRecommendationService:
    def __init__(
        self,
        *,
        build_shuffled_related_entries,
        paginate_items,
        build_related_video_detail_url,
    ):
        self.build_shuffled_related_entries = build_shuffled_related_entries
        self.paginate_items = paginate_items
        self.build_related_video_detail_url = build_related_video_detail_url

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

    def enrich_video_detail_context(
        self,
        context,
        entry_id,
        query_args,
        *,
        get_section_route,
        ai_context_for_video,
        url_for,
    ):
        try:
            page = max(int(query_args.get("page", 1)), 1)
        except (TypeError, ValueError):
            page = 1

        related_order = (query_args.get("related_order") or "normal").strip().lower()
        if related_order not in ("normal", "shuffle"):
            related_order = "normal"
        related_seed = (query_args.get("related_seed") or "").strip()
        seed_value = related_seed or context["entry"].get("entry_id", entry_id)
        started_at = time.monotonic()
        related_entries_full = list(context["related_entries"])
        playlist_entries_input_count = len(context.get("playlist_entries", []) or [])

        related_build_started_at = time.monotonic()
        if context.get("entry_type") == "youtube":
            playlist_entries_full = list(context.get("playlist_entries", []))
            if related_order == "shuffle":
                playlist_entries_full = self.build_shuffled_related_entries(playlist_entries_full, seed_value)
            current_playlist_index = next(
                (
                    index
                    for index, video in enumerate(playlist_entries_full)
                    if video.get("entry_id") == context["entry"].get("entry_id")
                ),
                0,
            )
            context["prev_entry"] = (
                playlist_entries_full[current_playlist_index - 1] if current_playlist_index > 0 else None
            )
            context["next_entry"] = (
                playlist_entries_full[current_playlist_index + 1]
                if current_playlist_index < len(playlist_entries_full) - 1
                else None
            )
            related_entries_full = [
                video
                for video in playlist_entries_full
                if video.get("entry_id") != context["entry"].get("entry_id")
            ]
        elif related_order == "shuffle":
            related_entries_full = self.build_shuffled_related_entries(related_entries_full, seed_value)
        related_build_elapsed_ms = (time.monotonic() - related_build_started_at) * 1000

        paginate_started_at = time.monotonic()
        related_page = self.paginate_items(related_entries_full, page, 10)
        paginate_elapsed_ms = (time.monotonic() - paginate_started_at) * 1000
        url_build_started_at = time.monotonic()
        context["related_entries"] = []
        for item in related_page["items"]:
            item_copy = dict(item)
            item_copy["detail_url"] = self.build_related_video_detail_url(
                item_copy.get("entry_id", ""),
                related_order=related_order,
                related_seed=seed_value if related_order == "shuffle" else "",
            )
            context["related_entries"].append(item_copy)
        url_build_elapsed_ms = (time.monotonic() - url_build_started_at) * 1000

        context["related_entries_full"] = related_entries_full
        context["related_page"] = related_page["page"]
        context["related_total_pages"] = related_page["total_pages"]
        context["pagination_numbers"] = related_page["pagination"]
        context["related_order"] = related_order
        context["related_seed"] = seed_value if related_order == "shuffle" else ""
        context["related_random_entry"] = related_entries_full[0] if related_entries_full else None
        context["related_random_url"] = (
            self.build_related_video_detail_url(
                context["related_random_entry"].get("entry_id", ""),
                related_order=related_order,
                related_seed=seed_value if related_order == "shuffle" else "",
            )
            if context["related_random_entry"]
            else ""
        )
        context["prev_entry_url"] = (
            self.build_related_video_detail_url(
                context["prev_entry"].get("entry_id", ""),
                related_order=related_order,
                related_seed=seed_value if related_order == "shuffle" else "",
            )
            if context.get("prev_entry")
            else None
        )
        context["next_entry_url"] = (
            self.build_related_video_detail_url(
                context["next_entry"].get("entry_id", ""),
                related_order=related_order,
                related_seed=seed_value if related_order == "shuffle" else "",
            )
            if context.get("next_entry")
            else None
        )
        context["delete_endpoint"] = (
            url_for("delete_from_youtube", playlist_item_id=context["entry"].get("playlist_item_id") or "__lookup__")
            if context.get("entry_type") == "youtube"
            else None
        )
        context["section_route"] = get_section_route(context["entry"].get("section", ""))
        ai_context = ai_context_for_video(context.get("entry_type"), context["entry"].get("section", ""))
        context["ai_default_mode"] = ai_context["mode"]
        context["ai_page_context"] = ai_context["page_context"]
        self._perf_log(
            "video_detail_related",
            entry_id=entry_id,
            entry_type=context.get("entry_type", ""),
            related_order=related_order,
            page=page,
            playlist_entries_input_count=playlist_entries_input_count,
            related_full_count=len(related_entries_full),
            related_page_count=len(context["related_entries"]),
            prev_entry_present=bool(context.get("prev_entry")),
            next_entry_present=bool(context.get("next_entry")),
            related_build_ms=related_build_elapsed_ms,
            paginate_ms=paginate_elapsed_ms,
            url_build_ms=url_build_elapsed_ms,
            total_ms=(time.monotonic() - started_at) * 1000,
        )
        return context
