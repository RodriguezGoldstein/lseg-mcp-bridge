from __future__ import annotations

import logging
import os
import platform as python_platform
import threading
from dataclasses import dataclass
from typing import Any, Literal

try:
    import lseg.data as ld
except ImportError as exc:  # pragma: no cover - depends on local environment
    ld = None
    _LSEG_IMPORT_ERROR: ImportError | None = exc
else:
    _LSEG_IMPORT_ERROR = None

AuthMode = Literal[
    "auto",
    "platform_client_credentials",
    "platform_password",
    "desktop",
]
ResolvedAuthMode = Literal[
    "platform_client_credentials",
    "platform_password",
    "desktop",
]

SENSITIVE_ENV_NAMES = (
    "LSEG_APP_KEY",
    "LSEG_CLIENT_ID",
    "LSEG_CLIENT_SECRET",
    "LSEG_USERNAME",
    "LSEG_PASSWORD",
)
AUTH_PRIORITY: tuple[ResolvedAuthMode, ...] = (
    "platform_client_credentials",
    "platform_password",
    "desktop",
)
HTTP_TIMEOUT_CONFIG_KEY = "http.request-timeout"
DEFAULT_TIMEOUT_SECONDS = 120

LOGGER = logging.getLogger("lseg_python_mcp_bridge.auth")


class BridgeError(RuntimeError):
    """Structured error raised by bridge helpers."""

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = redact_text(message)
        self.details = redact_value(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

    @classmethod
    def from_exception(
        cls,
        code: str,
        exc: BaseException,
        *,
        details: dict[str, Any] | None = None,
    ) -> "BridgeError":
        detail_map = dict(details or {})
        detail_map.setdefault("exception_type", type(exc).__name__)
        return cls(code=code, message=safe_exception_message(exc), details=detail_map)


def normalize_error(
    exc: BaseException,
    fallback_code: str,
    *,
    details: dict[str, Any] | None = None,
) -> BridgeError:
    if isinstance(exc, BridgeError):
        if details:
            merged = dict(exc.details)
            merged.update(redact_value(details))
            return BridgeError(code=exc.code, message=exc.message, details=merged)
        return exc
    return BridgeError.from_exception(fallback_code, exc, details=details)


@dataclass(slots=True)
class SessionSnapshot:
    session: Any
    auth_mode_used: ResolvedAuthMode
    library_version: str


def get_lseg_module() -> Any:
    """Return the imported lseg.data module or raise a structured error."""
    if ld is None:
        assert _LSEG_IMPORT_ERROR is not None
        raise BridgeError(
            code="missing_dependency",
            message="The 'lseg.data' package is not installed or could not be imported.",
            details={
                "install_hint": "Install dependencies from requirements.txt before starting the server.",
                "exception": safe_exception_message(_LSEG_IMPORT_ERROR),
            },
        )
    return ld


def library_version() -> str:
    module = get_lseg_module()
    return str(getattr(module, "__version__", "unknown"))


def python_version() -> str:
    return python_platform.python_version()


def available_credential_types() -> list[ResolvedAuthMode]:
    detected: list[ResolvedAuthMode] = []
    if _has_env_vars("LSEG_APP_KEY", "LSEG_CLIENT_ID", "LSEG_CLIENT_SECRET"):
        detected.append("platform_client_credentials")
    if _has_env_vars("LSEG_APP_KEY", "LSEG_USERNAME", "LSEG_PASSWORD"):
        detected.append("platform_password")
    if _has_env_vars("LSEG_APP_KEY"):
        detected.append("desktop")
    return detected


def detect_auth_mode(requested_mode: AuthMode = "auto") -> ResolvedAuthMode:
    detected = available_credential_types()
    if requested_mode != "auto":
        if requested_mode not in detected:
            raise BridgeError(
                code="missing_credentials",
                message=f"Requested auth mode '{requested_mode}' is not fully configured in the environment.",
                details={
                    "requested_mode": requested_mode,
                    "detected_credential_types": detected,
                    "required_env_vars": required_env_vars_for_mode(requested_mode),
                },
            )
        return requested_mode

    for mode in AUTH_PRIORITY:
        if mode in detected:
            return mode

    raise BridgeError(
        code="missing_credentials",
        message="No supported LSEG credential set was detected in the environment.",
        details={
            "detected_credential_types": detected,
            "supported_auth_modes": list(AUTH_PRIORITY),
        },
    )


def required_env_vars_for_mode(mode: ResolvedAuthMode | str) -> list[str]:
    if mode == "platform_client_credentials":
        return ["LSEG_APP_KEY", "LSEG_CLIENT_ID", "LSEG_CLIENT_SECRET"]
    if mode == "platform_password":
        return ["LSEG_APP_KEY", "LSEG_USERNAME", "LSEG_PASSWORD"]
    if mode == "desktop":
        return ["LSEG_APP_KEY"]
    return []


def safe_exception_message(exc: BaseException) -> str:
    return redact_text(f"{type(exc).__name__}: {exc}")


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): redact_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def redact_text(text: str) -> str:
    sanitized = text
    for env_name in SENSITIVE_ENV_NAMES:
        secret = os.getenv(env_name)
        if secret:
            sanitized = sanitized.replace(secret, f"<redacted:{env_name}>")
    return sanitized


def _has_env_vars(*names: str) -> bool:
    return all(os.getenv(name) for name in names)


class SessionManager:
    """Singleton session owner with lazy authentication and reuse."""

    _instance: "SessionManager | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "SessionManager":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._lock = threading.RLock()
        self._session: Any | None = None
        self._auth_mode_used: ResolvedAuthMode | None = None
        self._initialized = True

    @classmethod
    def instance(cls) -> "SessionManager":
        return cls()

    def ping_session(self, auth_mode: AuthMode = "auto") -> dict[str, Any]:
        response: dict[str, Any] = {
            "session_open": False,
            "auth_mode_used": None,
            "library_version": library_version() if ld is not None else None,
            "python_version": python_version(),
            "connectivity_summary": "",
            "detected_credential_types": available_credential_types(),
        }

        try:
            snapshot = self.ensure_session(auth_mode=auth_mode)
        except Exception as exc:
            bridge_error = normalize_error(
                exc,
                "session_open_failed",
                details={"auth_mode_requested": auth_mode},
            )
            response["connectivity_summary"] = bridge_error.message
            response["error"] = bridge_error.to_dict()
            return response

        session = snapshot.session
        response["session_open"] = self._is_session_open(session)
        response["auth_mode_used"] = snapshot.auth_mode_used
        response["library_version"] = snapshot.library_version
        response["connectivity_summary"] = (
            f"Session opened via {snapshot.auth_mode_used}; "
            f"session_name={getattr(session, 'name', 'unknown')}; "
            f"open_state={self._session_state(session)}; "
            f"http_timeout={self._http_timeout_seconds()}s."
        )
        return response

    def ensure_session(self, auth_mode: AuthMode = "auto") -> SessionSnapshot:
        module = get_lseg_module()
        requested_mode = detect_auth_mode(auth_mode)

        with self._lock:
            if self._session is not None and self._auth_mode_used is not None:
                if self._is_session_open(self._session) and (
                    auth_mode == "auto" or self._auth_mode_used == requested_mode
                ):
                    return SessionSnapshot(
                        session=self._session,
                        auth_mode_used=self._auth_mode_used,
                        library_version=str(getattr(module, "__version__", "unknown")),
                    )
                self.close_session()

            self._configure_timeout(module)
            try:
                session = self._open_session(module, requested_mode)
            except Exception as exc:
                raise normalize_error(
                    exc,
                    "session_open_failed",
                    details={
                        "auth_mode_requested": auth_mode,
                        "auth_mode_used": requested_mode,
                        "detected_credential_types": available_credential_types(),
                    },
                ) from exc

            self._session = session
            self._auth_mode_used = requested_mode
            return SessionSnapshot(
                session=session,
                auth_mode_used=requested_mode,
                library_version=str(getattr(module, "__version__", "unknown")),
            )

    def close_session(self) -> None:
        if ld is None:
            self._session = None
            self._auth_mode_used = None
            return

        module = get_lseg_module()
        with self._lock:
            if self._session is not None:
                try:
                    self._session.close()
                except Exception as exc:
                    LOGGER.debug("Session close raised: %s", safe_exception_message(exc))
            try:
                module.close_session()
            except Exception as exc:
                LOGGER.debug("Library close_session raised: %s", safe_exception_message(exc))
            finally:
                self._session = None
                self._auth_mode_used = None

    def current_auth_mode(self) -> ResolvedAuthMode | None:
        return self._auth_mode_used

    def _open_session(self, module: Any, auth_mode: ResolvedAuthMode) -> Any:
        if auth_mode == "platform_client_credentials":
            grant = module.session.platform.ClientCredentials(
                client_id=os.environ["LSEG_CLIENT_ID"],
                client_secret=os.environ["LSEG_CLIENT_SECRET"],
            )
            definition = module.session.platform.Definition(
                app_key=os.environ["LSEG_APP_KEY"],
                grant=grant,
            )
            return self._open_definition_session(module, definition, auth_mode)

        if auth_mode == "platform_password":
            grant = module.session.platform.GrantPassword(
                username=os.environ["LSEG_USERNAME"],
                password=os.environ["LSEG_PASSWORD"],
            )
            definition = module.session.platform.Definition(
                app_key=os.environ["LSEG_APP_KEY"],
                grant=grant,
            )
            return self._open_definition_session(module, definition, auth_mode)

        if auth_mode == "desktop":
            return self._open_desktop_session(module)

        raise BridgeError(
            code="session_open_failed",
            message=f"Unsupported auth mode '{auth_mode}'.",
            details={"auth_mode_used": auth_mode},
        )

    def _open_desktop_session(self, module: Any) -> Any:
        app_key = os.environ["LSEG_APP_KEY"]
        try:
            session = module.open_session(app_key=app_key)
            if self._is_session_open(session):
                return session
            LOGGER.debug("ld.open_session() returned a session that was not fully opened.")
        except Exception as exc:
            LOGGER.debug(
                "ld.open_session() desktop attempt failed, falling back to desktop.Definition: %s",
                safe_exception_message(exc),
            )

        definition = module.session.desktop.Definition(app_key=app_key)
        return self._open_definition_session(module, definition, "desktop")

    def _open_definition_session(self, module: Any, definition: Any, auth_mode: ResolvedAuthMode) -> Any:
        try:
            session = definition.get_session()
            module.session.set_default(session)
            open_state = session.open()
        except Exception as exc:
            raise BridgeError.from_exception(
                "session_open_failed",
                exc,
                details={"auth_mode_used": auth_mode},
            ) from exc

        state = str(getattr(open_state, "value", open_state))
        if state != "Opened":
            raise BridgeError(
                code="session_open_failed",
                message="The LSEG session did not reach the Opened state.",
                details={
                    "auth_mode_used": auth_mode,
                    "open_state": state,
                },
            )
        return session

    def _configure_timeout(self, module: Any) -> None:
        timeout_seconds = self._http_timeout_seconds()
        try:
            config = module.get_config()
            config.set_param(HTTP_TIMEOUT_CONFIG_KEY, timeout_seconds, auto_create=True)
        except Exception as exc:
            LOGGER.debug("Failed to set global timeout config: %s", safe_exception_message(exc))

    def _http_timeout_seconds(self) -> int:
        raw_value = os.getenv("LSEG_HTTP_TIMEOUT", str(DEFAULT_TIMEOUT_SECONDS)).strip()
        try:
            timeout = int(raw_value)
        except ValueError:
            return DEFAULT_TIMEOUT_SECONDS
        return timeout if timeout > 0 else DEFAULT_TIMEOUT_SECONDS

    def _is_session_open(self, session: Any) -> bool:
        return self._session_state(session) == "Opened"

    def _session_state(self, session: Any) -> str:
        state = getattr(session, "open_state", None)
        if hasattr(state, "value"):
            return str(state.value)
        return str(state) if state is not None else "unknown"
