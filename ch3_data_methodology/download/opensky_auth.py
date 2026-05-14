#!/usr/bin/env python3
# Shared OAuth2 authentication helper for OpenSky Trino scripts.
#
# Two modes:
#
#   1. Headless (automatic) — credentials configured
#      Set OPENSKY_USER + OPENSKY_PASS env vars, or create
#      ~/.opensky_credentials (two lines: username / password).
#      When Trino triggers re-auth, a background thread automatically
#      submits the Keycloak login form — no browser interaction needed.
#
#   2. Manual browser (fallback) — no credentials configured
#      The auth URL is printed to stdout; open it in a browser.
#      Token is cached in ~/.trino_oauth_cache.json for reuse.
#
# Setup (headless mode)
#   export OPENSKY_USER=your@email.com
#   export OPENSKY_PASS=your_password
#   # or:
#   printf 'your@email.com\nyour_password\n' > ~/.opensky_credentials
#   chmod 600 ~/.opensky_credentials

import json
import os
import re
import threading
from pathlib import Path

import requests
import trino.auth


# Constants

## Token cache file.
_CACHE_PATH = Path.home() / ".trino_oauth_cache.json"

## Optional credentials file (fallback for env vars).
_CREDS_PATH = Path.home() / ".opensky_credentials"

## Lock for cache file writes.
_LOCK = threading.Lock()


# Credential discovery

def _load_credentials() -> tuple[str, str] | None:
    """
    Return (username, password) from env vars or credentials file.
    Returns: (username, password) or None if not configured.
    """
    user   = os.environ.get("OPENSKY_USER") or os.environ.get("OPENSKY_USERNAME")
    passwd = os.environ.get("OPENSKY_PASS") or os.environ.get("OPENSKY_PASSWORD")
    if user and passwd:
        return user, passwd

    if _CREDS_PATH.exists():
        lines = _CREDS_PATH.read_text().splitlines()
        if len(lines) >= 2:
            return lines[0].strip(), lines[1].strip()

    return None


# File-backed token cache

class _FileTokenCache:
    """
    Persistent file-backed token cache for trino's OAuth2 flow.

    Implements the trino internal _OAuth2TokenCache interface:
      get_token_from_cache(key) → str | None
      store_token_to_cache(key, token) → None
    """

    def _read(self) -> dict:
        try:
            return json.loads(_CACHE_PATH.read_text())
        except Exception:
            return {}

    def get_token_from_cache(self, key) -> str | None:
        try:
            return self._read().get(str(key))
        except Exception:
            return None

    def store_token_to_cache(self, key, token: str) -> None:
        with _LOCK:
            try:
                data = self._read()
                data[str(key)] = token
                _CACHE_PATH.write_text(json.dumps(data, indent=2))
            except Exception:
                pass


# Headless redirect handler

class _HeadlessRedirectHandler:
    """
    Automatic Keycloak form-login handler for Trino's OAuth2 flow.

    When the Trino library needs re-authentication, it calls this handler
    with the Keycloak initiate URL.  A background thread visits that URL,
    parses the Keycloak login form, submits the credentials, and follows
    the redirect back to Trino — all without a browser.

    The Trino library's existing polling loop then picks up the stored
    token automatically.
    """

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def __call__(self, url: str) -> None:
        print("  ↺ Token expired — re-authenticating headlessly …", flush=True)
        threading.Thread(target=self._login, args=(url,), daemon=True).start()

    def _login(self, initiate_url: str) -> None:
        try:
            session = requests.Session()
            session.headers["User-Agent"] = "Mozilla/5.0"

            # Follow Trino → Keycloak redirect chain
            resp = session.get(initiate_url, allow_redirects=True, timeout=30)

            # Parse form action URL
            m = re.search(r'<form[^>]+action="([^"]+)"', resp.text, re.IGNORECASE)
            if not m:
                print("  ✗ Keycloak login form not found — manual re-auth needed.",
                      flush=True)
                return
            action_url = m.group(1).replace("&amp;", "&")

            # Collect all hidden inputs (CSRF token etc.)
            hidden: dict[str, str] = {}
            for pat in [
                r'<input[^>]+type=["\']hidden["\'][^>]+name=["\']([^"\']+)["\'][^>]+value=["\']([^"\']*)["\']',
                r'<input[^>]+name=["\']([^"\']+)["\'][^>]+type=["\']hidden["\'][^>]+value=["\']([^"\']*)["\']',
            ]:
                hidden.update(re.findall(pat, resp.text, re.IGNORECASE))

            # Submit credentials + hidden fields
            resp = session.post(
                action_url,
                data={**hidden, "username": self._username, "password": self._password},
                allow_redirects=True,
                timeout=30,
            )

            if resp.ok:
                print("  ✓ Headless re-authentication complete.", flush=True)
            else:
                print(f"  ✗ Headless auth failed (HTTP {resp.status_code}) "
                      "— manual re-auth needed.", flush=True)

        except Exception as exc:
            print(f"  ✗ Headless auth error: {exc} — manual re-auth needed.",
                  flush=True)


# Public API

def make_auth() -> trino.auth.OAuth2Authentication:
    """
    Build a Trino OAuth2Authentication with automatic re-auth support.

    If credentials are available (env vars or ~/.opensky_credentials), uses
    _HeadlessRedirectHandler to re-authenticate without browser interaction.
    Otherwise falls back to printing the auth URL to stdout.

    Token is always cached in ~/.trino_oauth_cache.json so it survives
    restarts and is shared across concurrent connections.

    Returns: Configured OAuth2Authentication ready for trino.dbapi.connect().
    """
    trino.auth.OAuth2Authentication.MAX_OAUTH_ATTEMPTS = 60  # 60s window

    creds = _load_credentials()
    if creds:
        username, password = creds
        handler = _HeadlessRedirectHandler(username, password)
    else:
        print("  ℹ  No OpenSky credentials found. "
              "Set OPENSKY_USER + OPENSKY_PASS for automatic re-auth.")
        handler = trino.auth.ConsoleRedirectHandler()

    auth = trino.auth.OAuth2Authentication(redirect_auth_url_handler=handler)
    auth._bearer._token_cache = _FileTokenCache()
    return auth


# Backwards-compat alias

def make_oauth2_auth() -> trino.auth.OAuth2Authentication:
    """@deprecated  Use make_auth() instead."""
    return make_auth()
