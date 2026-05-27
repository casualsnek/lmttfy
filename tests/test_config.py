"""Tests for deferred configuration and the DeferStrategy."""

import pytest

from lmttfy.deferred import (
    configure_deferred,
    DeferStrategy,
    _CONFIG,
    _inject_password,
    _pick_url,
    _try_connect,
)
from lmttfy.deferred import deferred_call


class TestConfigureDeferred:
    """configure_deferred() updates the global config."""

    def test_configure_urls(self):
        """Setting URLs via configure_deferred updates the config."""
        urls = ["redis://host1:6379/0", "redis://host2:6379/0"]
        configure_deferred(urls=urls)
        assert _CONFIG.urls == urls

    def test_configure_password(self):
        """Setting a password via configure_deferred."""
        configure_deferred(password="secret")
        assert _CONFIG.password == "secret"

    def test_configure_strategy(self):
        """Setting strategy via configure_deferred."""
        configure_deferred(strategy=DeferStrategy.RANDOM)
        assert _CONFIG.strategy == DeferStrategy.RANDOM

    def test_configure_partial(self):
        """Partial configure_deferred only changes the specified fields."""
        configure_deferred(urls=["redis://a:1"], password="pw1", strategy=DeferStrategy.ROUND_ROBIN)
        configure_deferred(urls=["redis://b:2"])  # only urls
        assert _CONFIG.urls == ["redis://b:2"]
        assert _CONFIG.password == "pw1"  # unchanged
        assert _CONFIG.strategy == DeferStrategy.ROUND_ROBIN  # unchanged


class TestInjectPassword:
    """_inject_password helper."""

    def test_no_password(self):
        assert _inject_password("redis://h:1/0", None) == "redis://h:1/0"

    def test_injects_password(self):
        result = _inject_password("redis://h:1/0", "pwd")
        assert result == "redis://:pwd@h:1/0"

    def test_existing_credentials_unchanged(self):
        result = _inject_password("redis://user:old@h:1/0", "new")
        assert result == "redis://user:old@h:1/0"


class TestDeferStrategy:
    """DeferStrategy enum and pick URL."""

    def test_first_available_uses_first(self):
        """FIRST_AVAILABLE always returns the first URL."""
        configure_deferred(
            urls=["redis://a:1", "redis://b:2"],
            strategy=DeferStrategy.FIRST_AVAILABLE,
        )
        for _ in range(5):
            assert _pick_url() == "redis://a:1"

    def test_round_robin_cycles(self):
        """ROUND_ROBIN cycles through URLs in order."""
        configure_deferred(
            urls=["redis://a:1", "redis://b:2", "redis://c:3"],
            strategy=DeferStrategy.ROUND_ROBIN,
        )
        expected = ["redis://a:1", "redis://b:2", "redis://c:3"]
        for i in range(6):
            assert _pick_url() == expected[i % 3], f"iteration {i}"

    def test_random_distribution(self):
        """RANDOM picks URLs (hard to test deterministically, just check it works)."""
        configure_deferred(
            urls=["redis://a:1", "redis://b:2"],
            strategy=DeferStrategy.RANDOM,
        )
        picked = {_pick_url() for _ in range(20)}
        assert len(picked) <= 2  # only the two URLs exist
        assert picked.issubset({"redis://a:1", "redis://b:2"})

    def test_empty_urls_returns_none(self):
        """_pick_url returns None when no URLs configured."""
        configure_deferred(urls=[])
        assert _pick_url() is None


class TestDeferredCallWithConfig:
    """deferred_call uses the global config when no redis_url is given."""

    def test_still_falls_back_locally(self):
        """With no Redis reachable, allow_local=True still uses local fallback."""
        configure_deferred(urls=["redis://nonexistent.example:6379/0"])
        f = deferred_call(allow_local=True)(lambda a, b: a + b)
        result = f(10, 20)
        assert result.wait() == 30
        # restore
        configure_deferred(urls=["redis://127.0.0.1:6379/0"])

    def test_still_raises_when_not_allowed(self):
        """With no Redis reachable and allow_local=False, RuntimeError."""
        configure_deferred(urls=["redis://nonexistent.example:6379/0"])
        f = deferred_call(allow_local=False)(lambda a, b: a + b)
        with pytest.raises(RuntimeError, match="no backend"):
            f(10, 20)
        configure_deferred(urls=["redis://127.0.0.1:6379/0"])
