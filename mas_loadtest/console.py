"""Console API client used to provision and clean up load-test resources."""
from __future__ import annotations

import base64
from http.cookiejar import CookieJar
import json
import ssl
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, HTTPSHandler, Request, build_opener
import uuid


class ConsoleError(RuntimeError):
    """Console API request failed; the message never contains credentials."""

    def __init__(self, message: str, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class ConsoleClient:
    def __init__(self, base_url: str, *, ca_file: Path | None = None, timeout_seconds: float = 60) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        context = ssl.create_default_context()
        if ca_file is not None:
            context.load_verify_locations(cafile=str(ca_file))
        self._cookies = CookieJar()
        self._opener = build_opener(HTTPCookieProcessor(self._cookies), HTTPSHandler(context=context))
        self._access_token: str | None = None

    def __repr__(self) -> str:
        return f"ConsoleClient(base_url={self._base_url!r}, authenticated={self._access_token is not None})"

    @property
    def access_token(self) -> str:
        if self._access_token is None:
            raise ConsoleError("console client is not authenticated")
        return self._access_token

    def _csrf_token(self) -> str | None:
        for cookie in self._cookies:
            if cookie.name.endswith("csrf_token"):
                return cookie.value
        return None

    def _request(self, method: str, path: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        headers = {"Accept": "application/json"}
        if self._access_token is not None:
            headers["Authorization"] = f"Bearer {self._access_token}"
        csrf = self._csrf_token()
        if csrf:
            headers["X-CSRF-Token"] = csrf
        body: bytes | None = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload).encode("utf-8")
        request = Request(f"{self._base_url}{path}", data=body, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self._timeout_seconds) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ConsoleError(f"{method} {path} failed with HTTP {exc.code}", http_status=exc.code) from exc
        except ssl.SSLCertVerificationError as exc:
            raise ConsoleError("TLS certificate verification failed; pass --ca-file") from exc
        except URLError as exc:
            if isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise ConsoleError("TLS certificate verification failed; pass --ca-file") from exc
            raise ConsoleError(f"{method} {path} failed: {type(exc.reason).__name__}") from exc
        except OSError as exc:
            raise ConsoleError(f"{method} {path} failed: {type(exc).__name__}") from exc
        if not raw:
            return {}
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConsoleError(f"{method} {path} returned invalid JSON") from exc
        return value if isinstance(value, dict) else {"data": value}

    def login(self, email: str, password: str) -> None:
        encoded = base64.b64encode(password.encode("utf-8")).decode("ascii")
        response = self._request("POST", "/console/api/login", {"email": email, "password": encoded, "remember_me": True})
        data = response.get("data")
        token = data.get("access_token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token:
            token = next((cookie.value for cookie in self._cookies if cookie.name.endswith("access_token")), None)
        if response.get("result") != "success" or not token:
            raise ConsoleError("login did not return an access token")
        self._access_token = token

    def import_app(self, yaml_content: str, name: str) -> str:
        response = self._request("POST", "/console/api/apps/imports", {"mode": "yaml-content", "yaml_content": yaml_content, "name": name})
        if response.get("status") == "pending":
            response = self._request("POST", f"/console/api/apps/imports/{response['id']}/confirm", {})
        app_id = response.get("app_id")
        if not str(response.get("status")).startswith("completed") or not isinstance(app_id, str):
            raise ConsoleError(f"app import failed with status {response.get('status')}")
        return app_id

    def publish_workflow(self, app_id: str) -> None:
        self._request("POST", f"/console/api/apps/{app_id}/workflows/publish", {"marked_name": "", "marked_comment": ""})

    def enable_api(self, app_id: str) -> None:
        self._request("POST", f"/console/api/apps/{app_id}/api-enable", {"enable_api": True})

    def create_app_api_key(self, app_id: str) -> tuple[str, str]:
        return self._token_and_id(self._request("POST", f"/console/api/apps/{app_id}/api-keys", {}))

    def delete_app_api_key(self, app_id: str, key_id: str) -> None:
        self._request("DELETE", f"/console/api/apps/{app_id}/api-keys/{key_id}")

    def delete_app(self, app_id: str) -> None:
        self._request("DELETE", f"/console/api/apps/{app_id}")

    def create_dataset(self, name: str) -> str:
        response = self._request("POST", "/console/api/datasets", {"name": name, "indexing_technique": "economy", "permission": "only_me"})
        dataset_id = response.get("id")
        if not isinstance(dataset_id, str):
            raise ConsoleError("dataset creation returned no id")
        return dataset_id

    def create_dataset_api_key(self) -> tuple[str, str]:
        return self._token_and_id(self._request("POST", "/console/api/datasets/api-keys", {}))

    def delete_dataset_api_key(self, key_id: str) -> None:
        self._request("DELETE", f"/console/api/datasets/api-keys/{key_id}")

    def delete_dataset(self, dataset_id: str) -> None:
        self._request("DELETE", f"/console/api/datasets/{dataset_id}")

    @staticmethod
    def _token_and_id(response: dict[str, object]) -> tuple[str, str]:
        token, key_id = response.get("token"), response.get("id")
        if not isinstance(token, str) or not isinstance(key_id, str):
            raise ConsoleError("API key creation returned no token")
        return token, key_id


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]
