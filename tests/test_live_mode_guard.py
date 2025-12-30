from sublimine.run import _allow_live_mode


def test_live_mode_guard_blocks_pytest_env():
    assert _allow_live_mode({"PYTEST_CURRENT_TEST": "test"}) is False
    assert _allow_live_mode({}) is True
