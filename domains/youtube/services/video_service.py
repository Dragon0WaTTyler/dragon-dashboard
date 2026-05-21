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

    def render_video_detail(self, entry_id, query_args):
        force_refresh = str(query_args.get("refresh", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
            "force",
        }
        context = self.get_video_detail_context(entry_id, force_refresh=force_refresh)
        if not context:
            missing_entry = {
                "entry_id": entry_id,
                "title": "Missing Video",
                "name": "Missing Video",
                "playlist_name": "",
                "playlist_url": "",
                "video_id": "",
                "duration": "",
                "section": "",
                "url": "",
            }
            return (
                render_template(
                    "video_detail.html",
                    missing=True,
                    entry=missing_entry,
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
                ),
                404,
            )

        context = self.recommendation_service.enrich_video_detail_context(
            context,
            entry_id,
            query_args,
            get_section_route=self.get_section_route,
            ai_context_for_video=self.ai_context_for_video,
            url_for=url_for,
        )
        return render_template(
            "video_detail.html",
            missing=False,
            **context,
            score_display=self.score_display,
            score_color=self.score_color,
            yts_url=self.yts_url,
            build_query_url=self.build_query_url,
        )
