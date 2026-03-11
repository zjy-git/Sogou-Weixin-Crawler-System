from __future__ import annotations

import logging
import os
import random
import re
import threading
from pathlib import Path
from typing import Dict, List, Optional


class ProxyPool:
    """
    Optional proxy pool with YAML-based dynamic config.

    Behavior priority:
    1) If constructor gets non-empty proxies, use them directly (static mode).
    2) Otherwise, try loading proxies from YAML config file (dynamic mode).

    YAML config lookup order:
    - env PROXY_POOL_YAML
    - ./proxy_pool.yaml
    - ./proxy_pool.yml
    - ./crawler_project/proxy_pool/proxy_pool.yaml
    - ./crawler_project/proxy_pool/proxy_pool.yml

    Supported YAML schema:

    proxy_pool:
      auto_reload: true
      reload_interval: 15
      proxies:
        - http://127.0.0.1:8080
        - http://127.0.0.1:8081

    or:

    proxies:
      - http://127.0.0.1:8080
      - http://127.0.0.1:8081
    """

    _DEFAULT_RELOAD_INTERVAL = 15.0

    def __init__(self, proxies: Optional[List[str]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._session_proxy: Dict[str, str] = {}
        self._in_use: set[str] = set()
        self._lock = threading.Lock()

        self._config_path = self._resolve_config_path()
        self._config_mtime: Optional[float] = None
        self._auto_reload = True
        self._reload_interval = self._DEFAULT_RELOAD_INTERVAL

        self._stop_event = threading.Event()
        self._reload_thread: Optional[threading.Thread] = None

        static_proxies = self._normalize_proxies(proxies or [])
        if static_proxies:
            self._proxies = static_proxies
            self._auto_reload = False
        else:
            self._proxies = []
            self._reload_from_yaml(force=True)
            if self._config_path and self._auto_reload:
                self._start_reload_thread()

    def acquire_proxy(self, session_id: str) -> Optional[str]:
        with self._lock:
            if session_id in self._session_proxy:
                return self._session_proxy[session_id]
            if not self._proxies:
                return None

            candidates = [proxy for proxy in self._proxies if proxy not in self._in_use]
            if not candidates:
                return None

            proxy = random.choice(candidates)
            self._session_proxy[session_id] = proxy
            self._in_use.add(proxy)
            return proxy

    def release_proxy(self, session_id: str) -> None:
        with self._lock:
            proxy = self._session_proxy.pop(session_id, None)
            if proxy is not None:
                self._in_use.discard(proxy)

    def close(self) -> None:
        self._stop_event.set()
        if self._reload_thread and self._reload_thread.is_alive():
            self._reload_thread.join(timeout=1.0)

    def _start_reload_thread(self) -> None:
        self._reload_thread = threading.Thread(
            target=self._reload_loop,
            name="proxy-pool-config-reload",
            daemon=True,
        )
        self._reload_thread.start()

    def _reload_loop(self) -> None:
        while not self._stop_event.wait(self._reload_interval):
            if not self._auto_reload:
                return
            self._reload_from_yaml(force=False)

    def _reload_from_yaml(self, force: bool) -> None:
        path = self._config_path
        if path is None or not path.exists():
            return

        try:
            mtime = path.stat().st_mtime
        except OSError:
            return

        if not force and self._config_mtime is not None and mtime <= self._config_mtime:
            return

        config = self._load_yaml_data(path)
        if config is None:
            return

        proxies, auto_reload, reload_interval = self._extract_settings(config)
        with self._lock:
            self._proxies = proxies
            self._auto_reload = auto_reload
            self._reload_interval = reload_interval
            self._config_mtime = mtime

    def _resolve_config_path(self) -> Optional[Path]:
        env_path = os.getenv("PROXY_POOL_YAML", "").strip()
        candidates: List[Path] = []
        if env_path:
            candidates.append(Path(env_path))

        candidates.extend(
            [
                Path("proxy_pool.yaml"),
                Path("proxy_pool.yml"),
                Path("crawler_project/proxy_pool/proxy_pool.yaml"),
                Path("crawler_project/proxy_pool/proxy_pool.yml"),
            ]
        )

        for candidate in candidates:
            try:
                resolved = candidate.expanduser().resolve()
            except OSError:
                continue
            if resolved.exists() and resolved.is_file():
                return resolved

        if env_path:
            try:
                return Path(env_path).expanduser().resolve()
            except OSError:
                return None
        return None

    def _load_yaml_data(self, path: Path) -> Optional[dict]:
        try:
            raw_text = path.read_text(encoding="utf-8")
        except OSError as exc:
            self._logger.warning("Failed to read proxy yaml: %s", exc)
            return None

        data: Optional[dict] = None
        try:
            import yaml  # type: ignore

            loaded = yaml.safe_load(raw_text)  # noqa: S506 - local config file
            if isinstance(loaded, dict):
                data = loaded
            elif loaded is None:
                data = {}
        except Exception:
            data = None

        if data is None:
            data = self._parse_simple_yaml(raw_text)

        if not isinstance(data, dict):
            self._logger.warning("Proxy yaml format is invalid: %s", path)
            return None
        return data

    def _extract_settings(self, data: dict) -> tuple[List[str], bool, float]:
        section = data.get("proxy_pool") if isinstance(data.get("proxy_pool"), dict) else data

        raw_proxies = section.get("proxies", data.get("proxies", []))
        proxies = self._normalize_proxies(raw_proxies)

        auto_reload = bool(section.get("auto_reload", True))

        raw_interval = section.get("reload_interval", section.get("reload_interval_seconds", self._DEFAULT_RELOAD_INTERVAL))
        try:
            reload_interval = max(1.0, float(raw_interval))
        except (TypeError, ValueError):
            reload_interval = self._DEFAULT_RELOAD_INTERVAL

        return proxies, auto_reload, reload_interval

    def _normalize_proxies(self, raw: object) -> List[str]:
        values: List[str] = []

        if isinstance(raw, str):
            values = [item.strip() for item in re.split(r"[,\n]", raw)]
        elif isinstance(raw, list):
            values = [str(item).strip() for item in raw]
        else:
            values = []

        deduplicated: List[str] = []
        seen: set[str] = set()
        for value in values:
            if not value or value in seen:
                continue
            deduplicated.append(value)
            seen.add(value)
        return deduplicated

    def _parse_simple_yaml(self, raw_text: str) -> dict:
        """
        Lightweight fallback parser for simple YAML layouts used by proxy config.
        It intentionally handles only a small subset:
        - top-level keys
        - proxy_pool nested keys
        - list items under proxies
        """
        result: dict = {}
        in_proxy_pool = False
        in_proxies = False

        for line in raw_text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            if stripped == "proxy_pool:":
                result.setdefault("proxy_pool", {})
                in_proxy_pool = True
                in_proxies = False
                continue

            if stripped.startswith("proxies:"):
                target = result["proxy_pool"] if in_proxy_pool else result
                inline = stripped.split(":", 1)[1].strip()
                if inline.startswith("[") and inline.endswith("]"):
                    items = [item.strip().strip("'\"") for item in inline[1:-1].split(",") if item.strip()]
                    target["proxies"] = items
                    in_proxies = False
                else:
                    target.setdefault("proxies", [])
                    in_proxies = True
                continue

            if in_proxies and stripped.startswith("-"):
                value = stripped[1:].strip().strip("'\"")
                target = result["proxy_pool"] if in_proxy_pool else result
                target.setdefault("proxies", []).append(value)
                continue

            key_value = stripped.split(":", 1)
            if len(key_value) != 2:
                continue

            key = key_value[0].strip()
            value = key_value[1].strip().strip("'\"")
            target = result["proxy_pool"] if in_proxy_pool else result

            if key in {"auto_reload"}:
                target[key] = value.lower() in {"1", "true", "yes", "on"}
            elif key in {"reload_interval", "reload_interval_seconds"}:
                try:
                    target[key] = float(value)
                except ValueError:
                    pass

            if stripped and not line.startswith(" ") and key != "proxy_pool":
                in_proxy_pool = False
                in_proxies = False

        return result

