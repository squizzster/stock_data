"""HTTP client and transport primitives for Massive read-only providers."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class MassiveProviderConfig:
    api_key: str
    base_url: str = "https://api.massive.com"
    timeout_seconds: float = 90.0

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("api_key is required")
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))


@dataclass(frozen=True)
class HttpJsonResponse:
    status_code: int | None
    payload: dict[str, Any]


class HttpJsonTransport(Protocol):
    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        """Return a decoded JSON object for one GET request."""


@dataclass(frozen=True)
class UrllibJsonTransport:
    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read().decode("utf-8", errors="replace")
                return HttpJsonResponse(response.status, _decode_json_payload(payload))
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="replace")
            return HttpJsonResponse(exc.code, _decode_json_payload(payload))
        except urllib.error.URLError as exc:
            return HttpJsonResponse(None, {"status": "URL_ERROR", "error": str(exc)})


def _decode_json_payload(payload: str) -> dict[str, Any]:
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return {"status": "NON_JSON_RESPONSE", "raw_preview": payload[:1000]}
    if isinstance(parsed, dict):
        return parsed
    return {"status": "NON_OBJECT_JSON", "raw": parsed}


@dataclass(frozen=True)
class MassiveRequestRecord:
    endpoint: str
    params_without_api_key: Any
    http_code: int | None
    api_status: str
    elapsed_seconds: float


@dataclass
class MassiveReadOnlyClient:
    config: MassiveProviderConfig
    transport: HttpJsonTransport = field(default_factory=UrllibJsonTransport)
    request_log: list[MassiveRequestRecord] = field(default_factory=list)
    raw_capture_dir: Path | None = None
    response_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = (
        field(default_factory=dict, repr=False)
    )

    def get(
        self, endpoint: str, params: dict[str, str] | None = None
    ) -> dict[str, Any]:
        request_params = dict(params or {})
        cache_key = (endpoint, tuple(sorted(request_params.items())))
        cached = self.response_cache.get(cache_key)
        if cached is not None:
            return cached
        query_params = dict(request_params)
        query_params["apiKey"] = self.config.api_key
        url = f"{self.config.base_url}{endpoint}?{urllib.parse.urlencode(query_params)}"
        started = time.monotonic()
        response = self.transport.get_json(
            url, timeout_seconds=self.config.timeout_seconds
        )
        elapsed = round(time.monotonic() - started, 3)
        self.request_log.append(
            MassiveRequestRecord(
                endpoint=endpoint,
                params_without_api_key=tuple(sorted(request_params.items())),
                http_code=response.status_code,
                api_status=str(response.payload.get("status") or ""),
                elapsed_seconds=elapsed,
            )
        )
        self._capture_response(endpoint, request_params, response, elapsed)
        self.response_cache[cache_key] = response.payload
        return response.payload

    def _capture_response(
        self,
        endpoint: str,
        params_without_api_key: dict[str, str],
        response: HttpJsonResponse,
        elapsed_seconds: float,
    ) -> None:
        if self.raw_capture_dir is None:
            return
        self.raw_capture_dir.mkdir(parents=True, exist_ok=True)
        index = len(self.request_log)
        safe_endpoint = endpoint.strip("/").replace("/", "_") or "root"
        path = self.raw_capture_dir / f"{index:04d}_{safe_endpoint}.json"
        capture = {
            "endpoint": endpoint,
            "params_without_api_key": tuple(sorted(params_without_api_key.items())),
            "http_code": response.status_code,
            "api_status": str(response.payload.get("status") or ""),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "payload": response.payload,
        }
        path.write_text(json.dumps(capture, indent=2, sort_keys=True), encoding="utf-8")
