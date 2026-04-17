from apps.normalizer.app.main import app


def test_app_exists() -> None:
    assert app.title == "normalizer"
