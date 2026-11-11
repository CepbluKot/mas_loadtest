"""HTTP-транспорт MAS Service API и общие проверки конфигурации."""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
import mimetypes
import os
from pathlib import Path
import re
import ssl
import stat
from typing import Literal, Protocol
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class ConfigError(ValueError):
    """Ошибка формата или ограничений конфигурации бенчмарка."""


class PreflightError(RuntimeError):
    """Одна из проверок namespace, Workflow или Dataset не пройдена."""


class MasRequestError(RuntimeError):
    """Сетевой сбой MAS с безопасной категорией и оценкой неопределённости."""

    def __init__(self, error_type: str, *, http_status: int | None = None, uncertain: bool = False) -> None:
        super().__init__(error_type)
        self.error_type = error_type
        self.http_status = http_status
        self.uncertain = uncertain


class MasTransport(Protocol):
    def get_json(self, target: TargetConfig, token_file: Path, path: str) -> dict[str, object]: ...

    def post_json(
        self, target: TargetConfig, token_file: Path, path: str, payload: dict[str, object]
    ) -> dict[str, object]: ...

    def post_multipart(
        self,
        target: TargetConfig,
        token_file: Path,
        path: str,
        file_content: bytes,
        filename: str,
        form_data: dict[str, str],
    ) -> dict[str, object]: ...

    def delete_document(self, target: TargetConfig, token_file: Path, path: str) -> int: ...




@dataclass(frozen=True)
class TargetConfig:
    namespace: str
    base_url: str
    ca_file: Path | None
    console_token_file: Path | None


class HttpMasTransport:
    """HTTP GET JSON transport с проверкой TLS по умолчанию."""

    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def _request_json(
        self,
        method: Literal["GET", "POST"],
        target: TargetConfig,
        token_file: Path,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        try:
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise MasRequestError("TokenFileReadError") from exc
        if not token:
            raise MasRequestError("EmptyTokenFile")
        url = f"{target.base_url}{path}"
        headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        body: bytes | None = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = Request(url, data=body, headers=headers, method=method)
        ssl_context = ssl.create_default_context()
        if target.ca_file:
            ssl_context.load_verify_locations(cafile=str(target.ca_file))
        try:
            with urlopen(request, timeout=self._timeout_seconds, context=ssl_context) as response:
                payload = response.read(20 * 1024 * 1024 + 1)
        except HTTPError as exc:
            raise MasRequestError("HttpError", http_status=exc.code, uncertain=method == "POST" and exc.code >= 500) from exc
        except ssl.SSLCertVerificationError as exc:
            raise MasRequestError("TLSVerificationError") from exc
        except URLError as exc:
            if isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise MasRequestError("TLSVerificationError") from exc
            if isinstance(exc.reason, TimeoutError):
                raise MasRequestError("RequestTimeout", uncertain=method == "POST") from exc
            raise MasRequestError("ConnectionError", uncertain=method == "POST") from exc
        except TimeoutError as exc:
            raise MasRequestError("RequestTimeout", uncertain=method == "POST") from exc
        except OSError as exc:
            raise MasRequestError("ConnectionError", uncertain=method == "POST") from exc
        if len(payload) > 20 * 1024 * 1024:
            raise MasRequestError("ResponseTooLarge", uncertain=method == "POST")
        try:
            return _object(json.loads(payload), "MAS response")
        except (json.JSONDecodeError, ConfigError) as exc:
            raise MasRequestError("InvalidJSONResponse", uncertain=method == "POST") from exc

    def get_json(self, target: TargetConfig, token_file: Path, path: str) -> dict[str, object]:
        try:
            return self._request_json("GET", target, token_file, path)
        except MasRequestError as exc:
            if exc.http_status is not None:
                raise PreflightError(f"MAS GET failed with HTTP {exc.http_status}") from exc
            descriptions = {
                "TLSVerificationError": "TLS certificate verification failed",
                "RequestTimeout": "MAS GET request timed out",
                "ConnectionError": "MAS GET request failed",
                "TokenFileReadError": "cannot read API token file",
                "EmptyTokenFile": "API token file is empty",
                "ResponseTooLarge": "MAS JSON response exceeds 20 MiB",
                "InvalidJSONResponse": "MAS returned invalid JSON",
            }
            raise PreflightError(descriptions.get(exc.error_type, "MAS GET request failed")) from exc

    def post_json(
        self, target: TargetConfig, token_file: Path, path: str, payload: dict[str, object]
    ) -> dict[str, object]:
        return self._request_json("POST", target, token_file, path, payload)

    def post_multipart(
        self,
        target: TargetConfig,
        token_file: Path,
        path: str,
        file_content: bytes,
        filename: str,
        form_data: dict[str, str],
    ) -> dict[str, object]:
        try:
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise MasRequestError("UploadInputReadError") from exc
        if not token:
            raise MasRequestError("EmptyTokenFile")
        if not filename or re.search(r'[\r\n"\\/]', filename):
            raise MasRequestError("InvalidUploadFilename")
        boundary = f"masbench-{uuid.uuid4().hex}"
        parts: list[bytes] = []
        for name, value in form_data.items():
            if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
                raise MasRequestError("InvalidMultipartField")
            parts.extend(
                [
                    f"--{boundary}\r\n".encode("ascii"),
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("ascii"),
                    value.encode("utf-8"),
                    b"\r\n",
                ]
            )
        mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        parts.extend(
            [
                f"--{boundary}\r\n".encode("ascii"),
                f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode("utf-8"),
                f"Content-Type: {mime_type}\r\n\r\n".encode("ascii"),
                file_content,
                b"\r\n",
                f"--{boundary}--\r\n".encode("ascii"),
            ]
        )
        request = Request(
            f"{target.base_url}{path}",
            data=b"".join(parts),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
            method="POST",
        )
        ssl_context = ssl.create_default_context()
        if target.ca_file:
            ssl_context.load_verify_locations(cafile=str(target.ca_file))
        try:
            with urlopen(request, timeout=self._timeout_seconds, context=ssl_context) as response:
                response_payload = response.read(20 * 1024 * 1024 + 1)
        except HTTPError as exc:
            raise MasRequestError("HttpError", http_status=exc.code, uncertain=exc.code >= 500) from exc
        except ssl.SSLCertVerificationError as exc:
            raise MasRequestError("TLSVerificationError") from exc
        except URLError as exc:
            if isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise MasRequestError("TLSVerificationError") from exc
            if isinstance(exc.reason, TimeoutError):
                raise MasRequestError("RequestTimeout", uncertain=True) from exc
            raise MasRequestError("ConnectionError", uncertain=True) from exc
        except TimeoutError as exc:
            raise MasRequestError("RequestTimeout", uncertain=True) from exc
        except OSError as exc:
            raise MasRequestError("ConnectionError", uncertain=True) from exc
        if len(response_payload) > 20 * 1024 * 1024:
            raise MasRequestError("ResponseTooLarge", uncertain=True)
        try:
            return _object(json.loads(response_payload), "MAS upload response")
        except (json.JSONDecodeError, ConfigError) as exc:
            raise MasRequestError("InvalidJSONResponse", uncertain=True) from exc

    def delete_document(self, target: TargetConfig, token_file: Path, path: str) -> int:
        try:
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise MasRequestError("TokenFileReadError") from exc
        if not token:
            raise MasRequestError("EmptyTokenFile")
        request = Request(
            f"{target.base_url}{path}",
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            method="DELETE",
        )
        ssl_context = ssl.create_default_context()
        if target.ca_file:
            ssl_context.load_verify_locations(cafile=str(target.ca_file))
        try:
            with urlopen(request, timeout=self._timeout_seconds, context=ssl_context) as response:
                return int(response.status)
        except HTTPError as exc:
            raise MasRequestError("HttpError", http_status=exc.code) from exc
        except ssl.SSLCertVerificationError as exc:
            raise MasRequestError("TLSVerificationError") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise MasRequestError("DeleteRequestError", uncertain=True) from exc




def _safe_run_dir(run_dir: Path, results_root: Path) -> Path:
    root = results_root.expanduser().resolve()
    candidate = run_dir.expanduser().resolve()
    if candidate == root:
        raise ConfigError("run-dir must be inside results_root, not results_root itself")
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ConfigError("run-dir must be inside results_root") from exc
    return candidate



def _secure_directory(path: Path) -> None:
    if path.exists():
        info = path.stat()
        if not path.is_dir() or info.st_uid != os.getuid():
            raise ConfigError("result directory must be owned by the current user")
        if stat.S_IMODE(info.st_mode) != 0o700:
            path.chmod(0o700)
    else:
        path.mkdir(parents=True, mode=0o700)
    if stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise ConfigError("result directory must have mode 0700")


def _atomic_json(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)



def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be a JSON object")
    return value


def _required_string(data: dict[str, object], key: str, label: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label}.{key} is required")
    return value.strip()


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value < 1:
        raise ConfigError(f"{label} must be a positive integer")
    return value


def _positive_seconds(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{label} must be positive seconds")
    return float(value)


def _private_file(value: object, label: str, *, secret: bool = False) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label} is required")
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise ConfigError(f"{label} file does not exist")
    info = path.stat()
    if secret and (
        info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) not in {0o400, 0o600}
    ):
        raise ConfigError(f"{label} must be owned by the current user and have mode 0600")
    if not secret and not os.access(path, os.R_OK):
        raise ConfigError(f"{label} must be readable by the current user")
    return path



def _origin(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{label} must be an HTTP(S) origin")
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ConfigError(f"{label} must be an HTTP(S) origin without credentials, path, query, or fragment")
    try:
        parsed.port
    except ValueError as exc:
        raise ConfigError(f"{label} has an invalid port") from exc
    return f"{parsed.scheme}://{parsed.netloc}"



def _validate_api_token_binding(response: dict[str, object], token_file: Path, label: str) -> None:
    values = response.get("data")
    if not isinstance(values, list):
        raise PreflightError(f"{label} API key list is invalid")
    try:
        configured_token = token_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise PreflightError(f"{label} API token file cannot be read") from exc
    if not configured_token or not any(
        isinstance(value, dict) and value.get("token") == configured_token for value in values
    ):
        raise PreflightError(f"{label} API token does not belong to the configured resource")



def _indexing_document(response: dict[str, object], document_id: str) -> dict[str, object]:
    values = response.get("data")
    if not isinstance(values, list):
        raise PreflightError("indexing status response has no data list")
    for value in values:
        if isinstance(value, dict) and value.get("id") == document_id:
            if not isinstance(value.get("indexing_status"), str):
                raise PreflightError("indexing status response has no status for document ID")
            return value
    raise PreflightError("indexing status response does not contain the uploaded document ID")


def indexing_state(response: dict[str, object], document_id: str) -> str:
    return str(_indexing_document(response, document_id)["indexing_status"])


