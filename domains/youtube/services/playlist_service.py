import time

from flask import Response, render_template


class YouTubePlaylistService:
    def __init__(
        self,
        *,
        load_admin_data,
        build_youtube_section_playlists,
        canonical_section_name,
        youtube_section_blueprint,
        build_youtube_channel_curation_context,
        build_youtube_section_feed_context,
        ai_context_for_section,
        build_query_url,
        build_combined_sections,
        pockettube_section_membership_context,
        normalize_youtube_section_record,
        section_slug,
        normalize_section_name,
        pockettube_latest_import_snapshot,
        iter_cached_pockettube_group_feeds,
        pockettube_display_name,
        normalize_pockettube_group_key,
    ):
        self.load_admin_data = load_admin_data
        self.build_youtube_section_playlists = build_youtube_section_playlists
        self.canonical_section_name = canonical_section_name
        self.youtube_section_blueprint = youtube_section_blueprint
        self.build_youtube_channel_curation_context = build_youtube_channel_curation_context
        self.build_youtube_section_feed_context = build_youtube_section_feed_context
        self.ai_context_for_section = ai_context_for_section
        self.build_query_url = build_query_url
        self.build_combined_sections = build_combined_sections
        self.pockettube_section_membership_context = pockettube_section_membership_context
        self.normalize_youtube_section_record = normalize_youtube_section_record
        self.section_slug = section_slug
        self.normalize_section_name = normalize_section_name
        self.pockettube_latest_import_snapshot = pockettube_latest_import_snapshot
        self.iter_cached_pockettube_group_feeds = iter_cached_pockettube_group_feeds
        self.pockettube_display_name = pockettube_display_name
        self.normalize_pockettube_group_key = normalize_pockettube_group_key

    def _trace_render(self, event, **fields):
        parts = [f"event={event}"]
        for key, value in fields.items():
            if isinstance(value, bool):
                value = int(value)
            if value in ("", None):
                continue
            parts.append(f"{key}={value}")
        message = "[youtube-render-trace] " + " ".join(parts)
        safe_message = message.encode("ascii", errors="backslashreplace").decode("ascii")
        print(safe_message)

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

    def render_section_page(self, section_name, title=None, quick_delete_enabled=False):
        route_started_at = time.monotonic()
        admin_started_at = time.monotonic()
        admin_data = self.load_admin_data()
        admin_elapsed_ms = (time.monotonic() - admin_started_at) * 1000
        projection_started_at = time.monotonic()
        playlists_with_videos, default_limit, section_channel_groups = self.build_youtube_section_playlists(
            section_name,
            admin_data=admin_data,
        )
        projection_elapsed_ms = (time.monotonic() - projection_started_at) * 1000
        section_title = self.canonical_section_name(title or section_name)
        section_profile = self.youtube_section_blueprint(section_name)
        curation_started_at = time.monotonic()
        section_curation_context = self.build_youtube_channel_curation_context(
            section_name,
            admin_data=admin_data,
        )
        curation_elapsed_ms = (time.monotonic() - curation_started_at) * 1000
        feed_started_at = time.monotonic()
        section_feed_context = self.build_youtube_section_feed_context(
            section_name,
            admin_data=admin_data,
        )
        feed_elapsed_ms = (time.monotonic() - feed_started_at) * 1000
        latest_fetch = (
            section_feed_context.get("latest_fetch_diagnostics", {})
            if isinstance(section_feed_context.get("latest_fetch_diagnostics", {}), dict)
            else {}
        )
        self._trace_render(
            "section_page",
            section_name=section_name,
            feed_context_id=id(section_feed_context),
            feed_updated_at=section_feed_context.get("updated_at", ""),
            feed_count=section_feed_context.get("feed_count", section_feed_context.get("video_count", 0)),
            channel_count=section_feed_context.get("channel_count", 0),
            feed_source_used=section_feed_context.get("feed_source_used", ""),
            latest_fetch_attempted=latest_fetch.get("attempted", False),
            latest_fetch_found=latest_fetch.get("latest_videos_found", 0),
            latest_fetch_at=latest_fetch.get("fetched_at", ""),
            pockettube_group_visible=section_feed_context.get("pockettube_group_visible", False),
        )
        section_profile.update(section_curation_context)
        section_profile["channel_groups"] = section_channel_groups
        ai_context = self.ai_context_for_section(section_name)
        render_started_at = time.monotonic()
        rendered = render_template(
            "youtube_section.html",
            title=section_title,
            playlists=playlists_with_videos,
            default_limit=default_limit,
            quick_delete_enabled=quick_delete_enabled,
            section_profile=section_profile,
            section_channel_groups=section_channel_groups,
            section_channel_curation=section_curation_context,
            section_feed_context=section_feed_context,
            build_query_url=self.build_query_url,
            ai_default_mode=ai_context["mode"],
            ai_page_context=ai_context["page_context"],
        )
        render_elapsed_ms = (time.monotonic() - render_started_at) * 1000
        self._perf_log(
            "watchlater_route" if self.normalize_section_name(section_name) == self.normalize_section_name("YouTube Watch Later") else "section_route",
            section_name=section_name,
            admin_load_ms=admin_elapsed_ms,
            projection_ms=projection_elapsed_ms,
            curation_ms=curation_elapsed_ms,
            playlist_refresh_ms=feed_elapsed_ms,
            render_ms=render_elapsed_ms,
            total_ms=(time.monotonic() - route_started_at) * 1000,
            playlist_count=len(playlists_with_videos),
            section_channel_group_count=len(section_channel_groups),
            feed_count=section_feed_context.get("feed_count", section_feed_context.get("video_count", 0)),
        )
        return rendered

    def render_library(self):
        return self.render_section_page("Library", title="Library", quick_delete_enabled=False)

    def render_watchlater(self):
        return self.render_section_page(
            "YouTube Watch Later",
            title="YouTube Watch Later",
            quick_delete_enabled=True,
        )

    def render_section_by_slug(self, section_slug_value):
        section = next(
            (item for item in self.build_combined_sections() if item.get("slug") == section_slug_value),
            None,
        )
        if not section:
            pockettube_section = self.pockettube_section_membership_context(
                section_slug_value,
                admin_data=self.load_admin_data(),
            )
            if pockettube_section.get("channel_count", 0):
                section_name = pockettube_section.get("section_name", section_slug_value) or section_slug_value
                section = self.normalize_youtube_section_record(
                    {
                        "name": section_name,
                        "slug": self.section_slug(section_name),
                        "playlists": [],
                        "section_kind": "curated",
                        "section_scope": "group",
                        "channel_group_key": pockettube_section.get("group_key", ""),
                        "channel_group_label": pockettube_section.get("group_name", ""),
                        "section_order": 500,
                    }
                )
        if not section:
            return Response("Section not found.", status=404)
        quick_delete_enabled = self.normalize_section_name(section.get("name", "")) == self.normalize_section_name(
            "YouTube Watch Later"
        )
        return self.render_section_page(
            section.get("name", ""),
            title=section.get("name", ""),
            quick_delete_enabled=quick_delete_enabled,
        )

    def render_pockettube_groups(self, query_args):
        latest, imported_sections = self.pockettube_latest_import_snapshot()
        search_query = str(query_args.get("q", "") or "").strip().lower()
        sort_key = str(query_args.get("sort", "default") or "default").strip().lower()
        cached_counts = {}
        for section_key, feed_context in self.iter_cached_pockettube_group_feeds():
            if not isinstance(feed_context, dict):
                continue
            group_name = self.pockettube_display_name(
                feed_context.get("group_name", "") or feed_context.get("name", "") or section_key
            )
            video_count = int(feed_context.get("feed_count", feed_context.get("video_count", 0)) or 0)
            cached_counts[self.normalize_pockettube_group_key(group_name)] = video_count
            cached_counts[self.normalize_pockettube_group_key(section_key)] = video_count

        pockettube_sections = []
        for section in self.build_combined_sections():
            if str(section.get("source", "") or "").strip().lower() != "pockettube":
                continue
            section_name = section.get("name", "")
            section_key = self.normalize_pockettube_group_key(section_name)
            group_label = section.get("channel_group_label", "") or section.get("channel_group_key", "")
            video_count = int(
                section.get("pockettube_video_count", 0)
                or section.get("video_count", 0)
                or cached_counts.get(section_key, 0)
                or cached_counts.get(self.normalize_pockettube_group_key(group_label), 0)
                or 0
            )
            item = {
                "name": section_name,
                "slug": section.get("slug", self.section_slug(section.get("name", ""))),
                "channel_count": int(section.get("pockettube_channel_count", 0) or 0),
                "video_count": video_count,
                "section_kind": section.get("section_kind", ""),
                "section_scope": section.get("section_scope", ""),
                "group_label": group_label,
            }
            haystack = " ".join(
                [
                    item["name"],
                    item["group_label"],
                    item["section_kind"],
                    item["section_scope"],
                ]
            ).lower()
            if search_query and search_query not in haystack:
                continue
            pockettube_sections.append(item)

        if sort_key == "channels":
            pockettube_sections.sort(
                key=lambda item: (
                    -int(item.get("channel_count", 0) or 0),
                    -int(item.get("video_count", 0) or 0),
                    item.get("name", "").lower(),
                )
            )
        elif sort_key == "videos":
            pockettube_sections.sort(
                key=lambda item: (
                    -int(item.get("video_count", 0) or 0),
                    -int(item.get("channel_count", 0) or 0),
                    item.get("name", "").lower(),
                )
            )
        elif sort_key in {"name", "az"}:
            pockettube_sections.sort(key=lambda item: item.get("name", "").lower())
        elif sort_key in {"name_desc", "za"}:
            pockettube_sections.sort(key=lambda item: item.get("name", "").lower(), reverse=True)
        else:
            sort_key = "default"

        self._trace_render(
            "pockettube_groups",
            imported_section_count=len(imported_sections),
            section_card_count=len(pockettube_sections),
            latest_imported_at=latest.get("imported_at", ""),
            latest_fingerprint=latest.get("fingerprint", ""),
        )
        return render_template(
            "pockettube_groups.html",
            title="PocketTube Groups",
            pockettube_sections=pockettube_sections,
            pockettube_import=latest,
            pockettube_source_sections=imported_sections,
            pockettube_search_query=search_query,
            pockettube_sort_key=sort_key,
            ai_default_mode="study",
            ai_page_context="study",
        )
