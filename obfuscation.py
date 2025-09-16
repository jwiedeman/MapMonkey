"""Utilities for generating varied browser identities for Playwright workers."""
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


# A collection of realistic desktop operating system tokens and the matching
# JavaScript platform value. These are combined with a recent Chromium version
# to build believable user agent strings when generating identities on the fly.
_OS_PLATFORMS: Sequence[tuple[str, str]] = (
    ("Windows NT 10.0; Win64; x64", "Win32"),
    ("Windows NT 11.0; Win64; x64", "Win32"),
    ("Macintosh; Intel Mac OS X 10_15_7", "MacIntel"),
    ("Macintosh; Intel Mac OS X 12_6", "MacIntel"),
    ("X11; Linux x86_64", "Linux x86_64"),
)

_CHROME_VERSIONS: Sequence[str] = (
    "123.0.6312.124",
    "123.0.6312.86",
    "122.0.6261.128",
    "122.0.6261.94",
    "121.0.6167.184",
    "121.0.6167.140",
)

_VIEWPORTS: Sequence[tuple[int, int]] = (
    (1920, 1080),
    (1680, 1050),
    (1600, 900),
    (1536, 864),
    (1440, 900),
    (1366, 768),
    (1280, 720),
)

_LOCALES: Sequence[str] = (
    "en-US",
    "en-GB",
    "en-CA",
    "en-AU",
    "fr-FR",
    "de-DE",
    "es-ES",
)

_TIMEZONES: Sequence[str] = (
    "America/New_York",
    "America/Chicago",
    "America/Los_Angeles",
    "America/Toronto",
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Europe/Madrid",
    "Australia/Sydney",
)

_COLOR_SCHEMES: Sequence[str] = ("light", "dark")
_DEVICE_SCALE_FACTORS: Sequence[float] = (1.0, 1.25, 1.5, 1.75, 2.0)
_HARDWARE_CONCURRENCY: Sequence[int] = (4, 6, 8, 12, 16)

_DEFAULT_VIEWPORT = (1920, 1080)
_DEFAULT_LOCALE = "en-US"
_DEFAULT_TIMEZONE = "America/New_York"
_DEFAULT_COLOR_SCHEME = "light"
_DEFAULT_PLATFORM = "Win32"
_DEFAULT_DEVICE_SCALE_FACTOR = 1.0
_DEFAULT_HARDWARE_CONCURRENCY = 8


@dataclass(frozen=True)
class BrowserIdentity:
    """Represents the fingerprint Playwright should emulate for a worker."""

    user_agent: str
    viewport: tuple[int, int]
    locale: str
    timezone: str
    color_scheme: str
    device_scale_factor: float = 1.0
    is_mobile: bool = False
    platform: str = "Win32"
    hardware_concurrency: int = 8

    def to_context_kwargs(self) -> dict[str, Any]:
        """Return keyword arguments for :meth:`browser.new_context`."""
        viewport = {"width": self.viewport[0], "height": self.viewport[1]}
        return {
            "user_agent": self.user_agent,
            "viewport": viewport,
            "locale": self.locale,
            "timezone_id": self.timezone,
            "color_scheme": self.color_scheme,
            "device_scale_factor": self.device_scale_factor,
            "is_mobile": self.is_mobile,
        }

    def window_size(self) -> tuple[int, int]:
        """Return the window size that matches the viewport dimensions."""
        return self.viewport

    def init_script(self) -> str:
        """Return JavaScript to reduce automation fingerprints."""
        languages = [self.locale]
        base_lang = self.locale.split("-")[0]
        if base_lang not in languages:
            languages.append(base_lang)
        languages_json = json.dumps(languages)
        locale_json = json.dumps(self.locale)
        platform_json = json.dumps(self.platform)
        return "\n".join(
            [
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});",
                "if (!window.chrome) { window.chrome = { runtime: {} }; }",
                f"Object.defineProperty(navigator, 'languages', {{get: () => {languages_json}}});",
                f"Object.defineProperty(navigator, 'language', {{get: () => {locale_json}}});",
                f"Object.defineProperty(navigator, 'platform', {{get: () => {platform_json}}});",
                f"Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => {self.hardware_concurrency}}});",
                f"Object.defineProperty(navigator, 'maxTouchPoints', {{get: () => {4 if self.is_mobile else 0}}});",
            ]
        )

    @classmethod
    def random(cls, *, rng: random.Random | None = None) -> "BrowserIdentity":
        """Create a believable random desktop identity."""
        rng = rng or random.SystemRandom()
        os_token, platform = rng.choice(_OS_PLATFORMS)
        chrome_version = rng.choice(_CHROME_VERSIONS)
        user_agent = (
            f"Mozilla/5.0 ({os_token}) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"
        )
        viewport = rng.choice(_VIEWPORTS)
        locale = rng.choice(_LOCALES)
        timezone = rng.choice(_TIMEZONES)
        color_scheme = rng.choice(_COLOR_SCHEMES)
        device_scale_factor = rng.choice(_DEVICE_SCALE_FACTORS)
        hardware_concurrency = rng.choice(_HARDWARE_CONCURRENCY)
        return cls(
            user_agent=user_agent,
            viewport=viewport,
            locale=locale,
            timezone=timezone,
            color_scheme=color_scheme,
            device_scale_factor=device_scale_factor,
            is_mobile=False,
            platform=platform,
            hardware_concurrency=hardware_concurrency,
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "BrowserIdentity":
        """Build an identity from a JSON configuration mapping."""
        user_agent = config.get("user_agent")
        if not user_agent:
            raise ValueError("browser profile entries must define 'user_agent'")

        viewport_cfg = config.get("viewport")
        viewport = _parse_viewport(viewport_cfg) if viewport_cfg is not None else _DEFAULT_VIEWPORT
        locale = config.get("locale", _DEFAULT_LOCALE)
        timezone = config.get("timezone", _DEFAULT_TIMEZONE)
        color_scheme = config.get("color_scheme", _DEFAULT_COLOR_SCHEME)
        device_scale_factor = float(config.get("device_scale_factor", _DEFAULT_DEVICE_SCALE_FACTOR))
        is_mobile = bool(config.get("is_mobile", False))
        platform = config.get("platform", _DEFAULT_PLATFORM)
        hardware_concurrency = int(config.get("hardware_concurrency", _DEFAULT_HARDWARE_CONCURRENCY))

        return cls(
            user_agent=user_agent,
            viewport=viewport,
            locale=locale,
            timezone=timezone,
            color_scheme=color_scheme,
            device_scale_factor=device_scale_factor,
            is_mobile=is_mobile,
            platform=platform,
            hardware_concurrency=hardware_concurrency,
        )

def _parse_viewport(viewport: Any) -> tuple[int, int]:
    """Normalise viewport configuration values."""
    if isinstance(viewport, dict):
        width = viewport.get("width")
        height = viewport.get("height")
    elif isinstance(viewport, (list, tuple)) and len(viewport) == 2:
        width, height = viewport
    else:
        raise ValueError("viewport must be a dict with width/height or a length-2 sequence")

    if width is None or height is None:
        raise ValueError("viewport is missing width or height values")
    return int(width), int(height)


class BrowserIdentityPool:
    """Factory for browser identities used by workers."""

    def __init__(self, identities: Sequence[BrowserIdentity] | None = None) -> None:
        self._identities = list(identities or [])

    @classmethod
    def from_file(cls, path: str | Path) -> "BrowserIdentityPool":
        """Load identities from a JSON or plain text file."""
        data = Path(path).read_text(encoding="utf8")
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            # Treat the file as a simple list of user agent strings.
            identities = [
                BrowserIdentity(
                    user_agent=line.strip(),
                    viewport=_DEFAULT_VIEWPORT,
                    locale=_DEFAULT_LOCALE,
                    timezone=_DEFAULT_TIMEZONE,
                    color_scheme=_DEFAULT_COLOR_SCHEME,
                )
                for line in data.splitlines()
                if line.strip()
            ]
            if not identities:
                raise ValueError("profile file does not contain any user agents")
            return cls(identities)

        if not isinstance(parsed, list):
            raise ValueError("profile file must contain a list of entries")

        identities: list[BrowserIdentity] = []
        for entry in parsed:
            if isinstance(entry, str):
                identities.append(
                    BrowserIdentity(
                        user_agent=entry.strip(),
                        viewport=_DEFAULT_VIEWPORT,
                        locale=_DEFAULT_LOCALE,
                        timezone=_DEFAULT_TIMEZONE,
                        color_scheme=_DEFAULT_COLOR_SCHEME,
                    )
                )
            elif isinstance(entry, dict):
                identities.append(BrowserIdentity.from_config(entry))
            else:
                raise ValueError("profile entries must be strings or objects")

        if not identities:
            raise ValueError("profile file does not define any identities")
        return cls(identities)

    def sample(self, rng: random.Random | None = None) -> BrowserIdentity:
        """Return an identity from the pool or generate a new one."""
        rng = rng or random
        if self._identities:
            return rng.choice(self._identities)
        return BrowserIdentity.random(rng=rng)

    def sample_many(self, count: int, *, rng: random.Random | None = None) -> list[BrowserIdentity]:
        """Return ``count`` identities, allowing reuse when necessary."""
        return [self.sample(rng=rng) for _ in range(count)]


def create_identity_pool(profile_file: str | None) -> BrowserIdentityPool:
    """Helper to build a pool from a user-provided file or defaults."""
    if profile_file:
        return BrowserIdentityPool.from_file(profile_file)
    return BrowserIdentityPool()
