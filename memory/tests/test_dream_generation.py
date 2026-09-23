import json
from types import SimpleNamespace

import pytest

import server


def _dream_text(length):
    seed = "我推开会呼吸的门，桃香沿着楼梯往上走，影子踩住一片发热的月光。"
    return (seed * ((length // len(seed)) + 1))[:length]


class _FakeCompletions:
    responses = []
    calls = []

    async def create(self, **kwargs):
        type(self).calls.append(kwargs)
        index = min(len(type(self).calls) - 1, len(type(self).responses) - 1)
        message = SimpleNamespace(content=type(self).responses[index])
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


@pytest.fixture
def dream_setup(monkeypatch, tmp_path):
    bucket = {
        "id": "memory-1",
        "content": "Human把一只脆桃递过来，果肉边缘是浅粉到乳白的渐变。",
        "metadata": {"created": "2026-07-27T09:00:00", "name": "脆桃", "type": "dynamic"},
    }

    async def list_all(**kwargs): return [bucket]

    _FakeCompletions.responses = []
    _FakeCompletions.calls = []
    monkeypatch.setattr(server.bucket_mgr, "list_all", list_all)
    monkeypatch.setattr(server, "BUCKETS_DIR", str(tmp_path))
    monkeypatch.setattr(server, "_post_dream_to_room", lambda entry: None)  # 测试梦不往外推
    monkeypatch.setattr(server.dehydrator, "api_available", True)
    monkeypatch.setattr(server.dehydrator, "client", SimpleNamespace(chat=SimpleNamespace(completions=_FakeCompletions())))
    return tmp_path


@pytest.mark.asyncio
async def test_dream_first_full_paragraph_is_cached_without_retry(dream_setup):
    compliant = _dream_text(240)
    _FakeCompletions.responses = [compliant]
    dream, _, recent, _ = await server._refresh_dream_cache()
    assert len(_FakeCompletions.calls) == 1
    assert "sourced memory fragments" in _FakeCompletions.calls[0]["messages"][0]["content"]
    assert dream == compliant
    assert [item["id"] for item in recent] == ["memory-1"]
    assert json.loads((dream_setup / "latest_dream.json").read_text())["dream"] == compliant


@pytest.mark.asyncio
async def test_dream_200_character_first_answer_does_not_retry(dream_setup):
    boundary = _dream_text(200)
    _FakeCompletions.responses = [boundary]
    dream, _, _, _ = await server._refresh_dream_cache()
    assert len(_FakeCompletions.calls) == 1
    assert dream == boundary


@pytest.mark.asyncio
async def test_dream_199_character_first_answer_retries(dream_setup):
    _FakeCompletions.responses = [_dream_text(199), _dream_text(230)]
    dream, _, _, _ = await server._refresh_dream_cache()
    assert len(_FakeCompletions.calls) == 2
    assert dream == _dream_text(230)


@pytest.mark.asyncio
async def test_dream_short_first_answer_is_rewritten_once_and_cached(dream_setup):
    short, rewritten = _dream_text(64), _dream_text(250)
    _FakeCompletions.responses = [short, rewritten]
    dream, _, _, _ = await server._refresh_dream_cache()
    assert len(_FakeCompletions.calls) == 2
    retry = _FakeCompletions.calls[1]["messages"]
    assert retry[1] == {"role": "assistant", "content": short}
    assert dream == rewritten
    assert json.loads((dream_setup / "latest_dream.json").read_text())["dream"] == rewritten


@pytest.mark.asyncio
async def test_dream_never_requests_more_than_two_answers(dream_setup):
    _FakeCompletions.responses = [_dream_text(64), _dream_text(80), _dream_text(240)]
    dream, _, _, _ = await server._refresh_dream_cache()
    assert len(_FakeCompletions.calls) == 2
    assert dream == _dream_text(80)


@pytest.mark.asyncio
async def test_no_material_does_not_erase_previous_dream(dream_setup, monkeypatch):
    cached = {"dream": _dream_text(140), "ts": 123, "fragments": ["old"]}
    (dream_setup / "latest_dream.json").write_text(json.dumps(cached), encoding="utf-8")

    async def list_all(**kwargs): return []
    monkeypatch.setattr(server.bucket_mgr, "list_all", list_all)
    dream, _, recent, _ = await server._refresh_dream_cache()
    assert dream == ""
    assert recent == []
    assert json.loads((dream_setup / "latest_dream.json").read_text()) == cached


def test_service_bearer_auth_is_independent_from_browser_session(monkeypatch):
    token = "s" * 48
    monkeypatch.setenv("OMBRE_SERVICE_TOKEN", token)
    request = SimpleNamespace(cookies={}, headers={"authorization": f"Bearer {token}"})
    assert server._is_service_authenticated(request) is True
    assert server._require_auth(request, allow_service=True) is None
    assert server._require_auth(request, allow_service=False) is not None
