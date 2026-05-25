from html import unescape


class ReadingRssService:
    def __init__(
        self,
        *,
        normalize_reading_source,
        reading_source_feed_candidate_urls,
        reading_http_get,
        reading_feedparser_diagnostics,
        build_reading_import_item_from_feedparser_entry,
        build_reading_import_item_from_xml_node,
        reading_source_primary_url,
        normalize_reading_url,
        extract_reading_lead_image_from_html,
        extract_reading_image_from_html,
        extract_reading_author_info_from_html,
        reading_pick_best_image_candidate,
        normalize_timestamp_value,
        current_timestamp,
        strip_reading_html,
        reading_entry_content_score,
        normalize_reading_category,
        reading_visible_topic_label,
        normalize_reading_space,
        xml_etree,
        feedparser_module,
        app_logger,
        monotonic,
    ):
        self.normalize_reading_source = normalize_reading_source
        self.reading_source_feed_candidate_urls = reading_source_feed_candidate_urls
        self.reading_http_get = reading_http_get
        self.reading_feedparser_diagnostics = reading_feedparser_diagnostics
        self.build_reading_import_item_from_feedparser_entry = build_reading_import_item_from_feedparser_entry
        self.build_reading_import_item_from_xml_node = build_reading_import_item_from_xml_node
        self.reading_source_primary_url = reading_source_primary_url
        self.normalize_reading_url = normalize_reading_url
        self.extract_reading_lead_image_from_html = extract_reading_lead_image_from_html
        self.extract_reading_image_from_html = extract_reading_image_from_html
        self.extract_reading_author_info_from_html = extract_reading_author_info_from_html
        self.reading_pick_best_image_candidate = reading_pick_best_image_candidate
        self.normalize_timestamp_value = normalize_timestamp_value
        self.current_timestamp = current_timestamp
        self.strip_reading_html = strip_reading_html
        self.reading_entry_content_score = reading_entry_content_score
        self.normalize_reading_category = normalize_reading_category
        self.reading_visible_topic_label = reading_visible_topic_label
        self.normalize_reading_space = normalize_reading_space
        self.xml_etree = xml_etree
        self.feedparser = feedparser_module
        self.app_logger = app_logger
        self.monotonic = monotonic

    def reading_extract_text(self, node, names, default=""):
        for name in names:
            found = node.find(f".//{{*}}{name}")
            if found is None:
                continue
            text = unescape("".join(found.itertext()).strip())
            if text:
                return text
        return default

    def reading_extract_link(self, node):
        links = list(node.findall(".//{*}link"))
        preferred_links = []
        fallback_links = []
        for link in links:
            href = str(link.attrib.get("href", "") or "").strip()
            text = unescape("".join(link.itertext()).strip())
            rel = str(link.attrib.get("rel", "") or "").strip().lower()
            if href and rel in {"alternate", "related", ""}:
                preferred_links.append(href)
                continue
            if href:
                fallback_links.append(href)
                continue
            if text:
                fallback_links.append(text)
        if preferred_links:
            return preferred_links[0]
        if fallback_links:
            return fallback_links[0]
        guid = node.find(".//{*}guid")
        if guid is not None:
            guid_text = unescape("".join(guid.itertext()).strip())
            if guid_text:
                return guid_text
        return ""

    def reading_extract_categories(self, node):
        categories = []
        for category in node.findall(".//{*}category"):
            value = str(category.attrib.get("term", "") or category.attrib.get("label", "") or "").strip()
            if not value:
                value = unescape("".join(category.itertext()).strip())
            if value:
                categories.append(value)
        return categories

    def reading_extract_entry_identifier(self, node):
        for name in ("guid", "id"):
            found = node.find(f".//{{*}}{name}")
            if found is None:
                continue
            text = unescape("".join(found.itertext()).strip())
            if text:
                return text
        return ""

    def reading_node_local_name(self, node):
        return str(getattr(node, "tag", "") or "").split("}", 1)[-1].lower()

    def reading_extract_feed_content(self, node):
        for child in list(node):
            local_name = self.reading_node_local_name(child)
            if local_name in {"encoded", "content"}:
                text = unescape("".join(child.itertext()).strip())
                if text:
                    return text
        return self.reading_extract_text(node, ["description", "summary", "subtitle"], default="")

    def reading_extract_feed_image_details(self, node, content_html="", article_url="", source_name="", source_url=""):
        candidates = []
        for child in node.iter():
            local_name = self.reading_node_local_name(child)
            attrs = getattr(child, "attrib", {}) or {}
            if local_name in {"thumbnail", "content", "image"}:
                candidate = attrs.get("url") or attrs.get("href") or attrs.get("src")
                media_type = str(attrs.get("type", "") or "").lower()
                medium = str(attrs.get("medium", "") or "").lower()
                if candidate and (local_name != "content" or "image" in media_type or medium == "image"):
                    candidates.append({
                        "url": candidate,
                        "kind": "feed_cover",
                        "attrs": {
                            "type": media_type,
                            "medium": medium,
                            "width": attrs.get("width", ""),
                            "height": attrs.get("height", ""),
                            "title": attrs.get("title", ""),
                            "label": attrs.get("label", ""),
                        },
                    })
            if local_name == "enclosure":
                candidate = attrs.get("url", "")
                media_type = str(attrs.get("type", "") or "").lower()
                if candidate and media_type.startswith("image/"):
                    candidates.append({
                        "url": candidate,
                        "kind": "feed_cover",
                        "attrs": {
                            "type": media_type,
                            "width": attrs.get("width", ""),
                            "height": attrs.get("height", ""),
                        },
                    })
        lead_image_url = self.extract_reading_lead_image_from_html(content_html, article_url)
        if lead_image_url:
            candidates.append({"url": lead_image_url, "kind": "explicit", "attrs": {}})
        body_image_url = self.extract_reading_image_from_html(
            content_html,
            article_url,
            source_url=source_url,
            source_name=source_name,
        )
        if body_image_url:
            candidates.append({"url": body_image_url, "kind": "body", "attrs": {}})
        best = self.reading_pick_best_image_candidate(
            candidates,
            article_url=article_url,
            source_url=source_url,
            source_name=source_name,
        )
        if not best.get("url"):
            return "", "", ""
        if best.get("kind") == "body":
            return best.get("url", ""), "", "body"
        return best.get("url", ""), best.get("url", ""), best.get("kind", "feed_cover") or "feed_cover"

    def build_reading_import_item(self, source, feed_url, node, source_topic=""):
        title = self.reading_extract_text(node, ["title"], default="Untitled article")
        url = self.normalize_reading_url(self.reading_extract_link(node))
        external_id = self.reading_extract_entry_identifier(node)
        published_at = self.normalize_timestamp_value(
            self.reading_extract_text(node, ["pubDate", "published", "updated", "date"], default="")
        )
        imported_at = self.current_timestamp()
        topic = self.reading_extract_categories(node)
        content_html = self.reading_extract_feed_content(node)
        content_text = self.strip_reading_html(content_html)
        excerpt = content_text[:420].strip()
        image_url, lead_image_url, lead_image_kind = self.reading_extract_feed_image_details(
            node,
            content_html=content_html,
            article_url=url,
            source_name=source.get("name", ""),
            source_url=feed_url,
        )
        author_info = self.extract_reading_author_info_from_html(content_html, url)
        author_name = self.reading_extract_text(node, ["author", "creator", "dc:creator"], default="") or author_info.get("author", "")
        content_score = self.reading_entry_content_score({
            "content_html": content_html,
            "content_text": content_text,
            "excerpt": excerpt,
            "image_url": image_url,
            "lead_image_url": lead_image_url,
            "author_image_url": author_info.get("author_image_url", ""),
            "author": author_name,
        })
        return {
            "source": source.get("name", "Unknown Source"),
            "source_id": source.get("id", ""),
            "title": title,
            "url": url,
            "original_url": url,
            "external_id": external_id,
            "published_at": published_at,
            "added_at": imported_at,
            "imported_at": imported_at,
            "status": "unread",
            "starred": False,
            "author": author_name,
            "author_image_url": author_info.get("author_image_url", ""),
            "topic": topic[0] if topic else source_topic,
            "category": self.normalize_reading_category(source.get("category", "")),
            "topic_display": self.reading_visible_topic_label(topic[0] if topic else source_topic, source.get("category", "")),
            "image_url": image_url,
            "lead_image_url": lead_image_url,
            "lead_image_kind": lead_image_kind,
            "excerpt": excerpt,
            "content_text": content_text,
            "content_html": content_html,
            "content_score": content_score,
            "extraction_status": "feed" if content_text else "",
            "extraction_error": "",
            "content_cached_at": imported_at if content_text or image_url else "",
            "origin": "rss",
            "feed_url": feed_url,
        }

    def build_reading_import_item_from_feedparser(self, source, feed_url, entry, source_topic=""):
        source = source if isinstance(source, dict) else {}
        entry = entry if isinstance(entry, dict) else {}
        title = self.normalize_reading_space(entry.get("title", "")) or "Untitled article"
        url = self.normalize_reading_url(entry.get("link", "") or entry.get("id", ""))
        external_id = self.normalize_reading_space(entry.get("id", "") or entry.get("guid", ""))
        published_at = self.normalize_timestamp_value(
            entry.get("published", "")
            or entry.get("updated", "")
            or entry.get("created", "")
            or entry.get("pubDate", "")
        )
        imported_at = self.current_timestamp()
        topic = []
        for tag in entry.get("tags", []) or []:
            term = ""
            if isinstance(tag, dict):
                term = str(tag.get("term", "") or tag.get("label", "") or "").strip()
            else:
                term = str(tag or "").strip()
            if term:
                topic.append(term)
        content_html = ""
        for content_item in entry.get("content", []) or []:
            if isinstance(content_item, dict):
                candidate = str(content_item.get("value", "") or "").strip()
                if candidate:
                    content_html = candidate
                    break
        if not content_html:
            content_html = str(entry.get("summary", "") or entry.get("description", "") or entry.get("subtitle", "") or "").strip()
        content_text = self.strip_reading_html(content_html)
        excerpt = content_text[:420].strip()
        lead_image_url = self.extract_reading_lead_image_from_html(content_html, url)
        image_url = self.extract_reading_image_from_html(
            content_html,
            url,
            source_url=feed_url,
            source_name=source.get("name", ""),
        )
        lead_image_kind = "explicit" if lead_image_url else ""
        author_name = self.normalize_reading_space(entry.get("author", "") or "")
        author_image_url = ""
        content_score = self.reading_entry_content_score({
            "content_html": content_html,
            "content_text": content_text,
            "excerpt": excerpt,
            "image_url": image_url,
            "lead_image_url": lead_image_url,
            "author_image_url": author_image_url,
            "author": author_name,
        })
        return {
            "source": source.get("name", "Unknown Source"),
            "source_id": source.get("id", ""),
            "title": title,
            "url": url,
            "original_url": url,
            "external_id": external_id or url,
            "published_at": published_at,
            "added_at": imported_at,
            "imported_at": imported_at,
            "status": "unread",
            "starred": False,
            "author": author_name,
            "author_image_url": author_image_url,
            "topic": topic[0] if topic else source_topic,
            "category": self.normalize_reading_category(source.get("category", "")),
            "topic_display": self.reading_visible_topic_label(topic[0] if topic else source_topic, source.get("category", "")),
            "image_url": image_url,
            "lead_image_url": lead_image_url,
            "lead_image_kind": lead_image_kind,
            "excerpt": excerpt,
            "content_text": content_text,
            "content_html": content_html,
            "content_score": content_score,
            "extraction_status": "feed" if content_text else "",
            "extraction_error": "",
            "content_cached_at": imported_at if content_text or image_url else "",
            "origin": "rss",
            "feed_url": feed_url,
        }

    def fetch_reading_feed(self, source):
        started_at = self.monotonic()
        source = self.normalize_reading_source(source)
        attempts = []

        def _log_result(result):
            self.app_logger.info(
                "reading_rss fetch elapsed_ms=%.1f source=%s ok=%s feed_kind=%s attempts=%s raw_count=%s normalized_count=%s status_code=%s source_fallback_used=%s",
                (self.monotonic() - started_at) * 1000,
                str(source.get("name", "Unknown Source") or "Unknown Source").strip(),
                bool(result.get("ok")),
                str(result.get("feed_kind", "") or "").strip(),
                len(attempts),
                int(result.get("raw_count", 0) or 0),
                int(result.get("normalized_count", 0) or 0),
                int(result.get("status_code", 0) or 0),
                bool(result.get("source_fallback_used", False)),
            )
            return result

        feed_url = str(source.get("url", "") or "").strip()
        if not feed_url:
            return _log_result({
                "ok": False,
                "feed_kind": "empty",
                "source_url": feed_url,
                "primary_url": str(source.get("primary_url", "") or "").strip(),
                "feed_url": "",
                "resolved_url": "",
                "final_url": "",
                "successful_url": "",
                "status_code": 0,
                "content_type": "",
                "raw_count": 0,
                "normalized_count": 0,
                "items": [],
                "retry_count": 0,
                "timeout_reason": "",
                "feedparser_bozo": "",
                "feedparser_bozo_exception": "",
                "feedparser_entry_count": 0,
                "source_fallback_used": False,
                "tried_urls": [],
                "attempts": [],
                "error": "Missing feed URL.",
            })
        candidate_urls = self.reading_source_feed_candidate_urls(source)
        source_topic = str(source.get("topic", "") or "").strip()
        for candidate_index, candidate_url in enumerate(candidate_urls):
            response, request_diag = self.reading_http_get(
                candidate_url,
                timeout_seconds=20,
                purpose="feed",
                retries=1,
                source=source,
            )
            request_diag = dict(request_diag or {})
            request_diag["feed_url"] = candidate_url
            request_diag["source_url"] = feed_url
            request_diag["fallback_index"] = candidate_index
            attempts.append(request_diag)
            if response is None:
                continue
            status_code = int(getattr(response, "status_code", 0) or 0)
            content_type = str(getattr(response, "headers", {}).get("Content-Type", "") or "").strip()
            if status_code >= 400:
                request_diag["error"] = f"HTTP {status_code}"
                continue
            feedparser_diag = self.reading_feedparser_diagnostics(getattr(response, "content", b""))
            try:
                root = self.xml_etree.fromstring(response.content)
            except Exception as exc:
                request_diag["error"] = str(exc) or exc.__class__.__name__
                request_diag["parse_error"] = str(exc) or exc.__class__.__name__
                if feedparser_diag.get("entry_count", 0) and self.feedparser is not None:
                    parsed = self.feedparser.parse(response.content or b"")
                    items = []
                    for entry in getattr(parsed, "entries", []) or []:
                        items.append(
                            self.build_reading_import_item_from_feedparser_entry(
                                source,
                                candidate_url,
                                entry,
                                source_topic=source_topic,
                            )
                        )
                    return _log_result({
                        "ok": True,
                        "feed_kind": "feedparser",
                        "source_url": feed_url,
                        "primary_url": self.reading_source_primary_url(source),
                        "feed_url": candidate_url,
                        "resolved_url": request_diag.get("final_url", candidate_url) or candidate_url,
                        "final_url": request_diag.get("final_url", candidate_url) or candidate_url,
                        "successful_url": candidate_url,
                        "status_code": status_code,
                        "content_type": content_type,
                        "raw_count": len(getattr(parsed, "entries", []) or []),
                        "normalized_count": len(items),
                        "items": items,
                        "retry_count": int(request_diag.get("retry_count", 0) or 0),
                        "timeout_reason": str(request_diag.get("timeout_reason", "") or "").strip(),
                        "feedparser_bozo": feedparser_diag.get("bozo", ""),
                        "feedparser_bozo_exception": feedparser_diag.get("bozo_exception", ""),
                        "feedparser_entry_count": int(feedparser_diag.get("entry_count", 0) or 0),
                        "source_fallback_used": candidate_url != feed_url,
                        "tried_urls": [str(attempt.get("feed_url", "") or "") for attempt in attempts if str(attempt.get("feed_url", "") or "").strip()],
                        "attempts": attempts,
                        "error": "",
                    })
                continue

            items = []
            rss_items = []
            atom_items = []
            feed_kind = "unknown"
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
                items.append(
                    self.build_reading_import_item_from_xml_node(
                        source,
                        candidate_url,
                        node,
                        source_topic=source_topic,
                    )
                )

            for node in atom_items:
                items.append(
                    self.build_reading_import_item_from_xml_node(
                        source,
                        candidate_url,
                        node,
                        source_topic=source_topic,
                    )
                )

            return _log_result({
                "ok": True,
                "feed_kind": feed_kind,
                "source_url": feed_url,
                "primary_url": self.reading_source_primary_url(source),
                "feed_url": candidate_url,
                "resolved_url": request_diag.get("final_url", candidate_url) or candidate_url,
                "final_url": request_diag.get("final_url", candidate_url) or candidate_url,
                "successful_url": candidate_url,
                "status_code": status_code,
                "content_type": content_type,
                "raw_count": len(rss_items) + len(atom_items),
                "normalized_count": len(items),
                "items": items,
                "retry_count": int(request_diag.get("retry_count", 0) or 0),
                "timeout_reason": str(request_diag.get("timeout_reason", "") or "").strip(),
                "feedparser_bozo": feedparser_diag.get("bozo", ""),
                "feedparser_bozo_exception": feedparser_diag.get("bozo_exception", ""),
                "feedparser_entry_count": int(feedparser_diag.get("entry_count", 0) or 0),
                "source_fallback_used": candidate_url != feed_url,
                "tried_urls": [str(attempt.get("feed_url", "") or "") for attempt in attempts if str(attempt.get("feed_url", "") or "").strip()],
                "attempts": attempts,
                "error": "",
            })

        last_attempt = attempts[-1] if attempts else {}
        return _log_result({
            "ok": False,
            "feed_kind": "error",
            "source_url": feed_url,
            "primary_url": self.reading_source_primary_url(source),
            "feed_url": str(last_attempt.get("feed_url", feed_url) or feed_url),
            "resolved_url": str(last_attempt.get("final_url", "") or ""),
            "final_url": str(last_attempt.get("final_url", "") or ""),
            "successful_url": "",
            "status_code": int(last_attempt.get("status_code", 0) or 0),
            "content_type": str(last_attempt.get("content_type", "") or ""),
            "raw_count": 0,
            "normalized_count": 0,
            "items": [],
            "retry_count": int(last_attempt.get("retry_count", 0) or 0),
            "timeout_reason": str(last_attempt.get("timeout_reason", "") or "").strip(),
            "feedparser_bozo": "",
            "feedparser_bozo_exception": "",
            "feedparser_entry_count": 0,
            "source_fallback_used": bool(last_attempt and str(last_attempt.get("feed_url", "") or "") != feed_url),
            "tried_urls": [str(attempt.get("feed_url", "") or "") for attempt in attempts if str(attempt.get("feed_url", "") or "").strip()],
            "attempts": attempts,
            "error": str(last_attempt.get("error", "") or "") or "Unable to fetch feed.",
        })
