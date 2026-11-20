from __future__ import annotations

import os

import pandas as pd
import requests

from src.exceptions import ExtractionError
from src.extractors.base import BaseExtractor
from src.extractors.json_extractor import navigate, records_to_frame
from src.utils.env import expand_env
from src.utils.retry import retry_call


class TransientHTTPError(Exception):
    """HTTP 5xx / 429: worth retrying."""


def _is_transient(exc: Exception) -> bool:
    return isinstance(exc, (requests.Timeout, requests.ConnectionError, TransientHTTPError))


class APIExtractor(BaseExtractor):
    """Paginated REST extraction with timeouts, retries and response validation.

    Pagination types: none | page (page number) | offset | link (follow a 'next' URL in the body).
    Refuses to continue if ``max_pages`` is hit, because a silently truncated extract
    could cause a full load to delete good rows.
    """

    source_type = "api"

    def _session(self):  # separated so tests can inject a fake session
        return requests.Session()

    def _fetch(self, session, url, headers, params):
        cfg = self.config
        resp = session.request(cfg.get("method", "GET"), url, headers=headers, params=params,
                               timeout=cfg.get("timeout", 10))
        if resp.status_code >= 500 or resp.status_code == 429:
            raise TransientHTTPError(f"HTTP {resp.status_code} from {url}")
        if resp.status_code >= 400:
            raise ExtractionError(f"HTTP {resp.status_code} from {url} (not retryable)")
        try:
            return resp.json()
        except ValueError as exc:
            raise ExtractionError(f"response from {url} is not valid JSON") from exc

    def _fetch_with_retry(self, session, url, headers, params):
        try:
            return retry_call(
                lambda: self._fetch(session, url, headers, params),
                max_attempts=self.retry["max_attempts"], backoff_seconds=self.retry["backoff_seconds"],
                backoff_factor=self.retry["backoff_factor"], should_retry=_is_transient,
                description=f"GET {url}", logger=self.log, **({"sleep": self._sleep} if self._sleep else {}),
            )
        except (requests.Timeout, requests.ConnectionError, TransientHTTPError) as exc:
            raise ExtractionError(f"API request failed after {self.retry['max_attempts']} attempt(s): {exc}") from exc

    def _page_records(self, payload, rejected):
        records = navigate(payload, self.config.get("data_key"), "API data_key")
        if not isinstance(records, list):
            raise ExtractionError("unexpected API response shape: records are not a list")
        good = []
        for i, r in enumerate(records):
            if isinstance(r, dict):
                good.append(r)
            else:
                rejected.append({"reason": "API record is not a JSON object", "record": str(r)[:500]})
        return good

    def _extract(self):
        cfg = self.config
        try:
            headers = expand_env(cfg.get("headers", {}))
            params = expand_env(cfg.get("params", {}))
            url = expand_env(cfg["url"])
            auth = cfg.get("auth")
            if auth:
                token = os.environ.get(auth.get("token_env", ""))
                if not token:
                    raise KeyError(f"environment variable '{auth.get('token_env')}' is not set")
                headers = {**headers, "Authorization": f"Bearer {token}"}
        except KeyError as exc:
            raise ExtractionError(f"API configuration error: {exc}") from exc

        pg = cfg.get("pagination", {"type": "none"})
        ptype = pg.get("type", "none")
        session = self._session()
        records: list[dict] = []
        rejected: list[dict] = []
        pages = 0
        page_no = pg.get("start_page", 1)
        offset = 0
        next_url = url
        while True:
            if pages >= cfg.get("max_pages", 1000):
                raise ExtractionError(f"max_pages ({cfg.get('max_pages')}) reached; refusing to continue "
                                      "with possibly incomplete data")
            req_params = dict(params) if (ptype != "link" or pages == 0) else {}
            if ptype == "page":
                req_params[pg.get("page_param", "page")] = page_no
                req_params[pg.get("page_size_param", "page_size")] = pg["page_size"]
            elif ptype == "offset":
                req_params[pg.get("offset_param", "offset")] = offset
                req_params[pg.get("page_size_param", "limit")] = pg["page_size"]
            payload = self._fetch_with_retry(session, next_url, headers, req_params)
            page = self._page_records(payload, rejected)
            records.extend(page)
            pages += 1
            if ptype == "none":
                break
            if ptype in {"page", "offset"}:
                if len(page) < pg["page_size"]:
                    break
                page_no += 1
                offset += pg["page_size"]
            else:  # link
                nxt = payload.get(pg["next_key"]) if isinstance(payload, dict) else None
                if not nxt:
                    break
                next_url = nxt
        df = records_to_frame(records, True, rejected) if records else pd.DataFrame()
        return df, rejected, {"url": url, "pages_fetched": pages}
