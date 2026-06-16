from __future__ import annotations

from apps.normalizer.app.search_docs.service import UniversitySearchDocProjectionService
from scripts.seed_university_examples.data import DEMO_UNIVERSITY_CARDS


def test_seed_university_examples_are_complete_cards() -> None:
    assert len(DEMO_UNIVERSITY_CARDS) == 4

    for card in DEMO_UNIVERSITY_CARDS:
        assert card.canonical_name.value
        assert card.description
        assert card.history.get("summary")
        assert card.location.city
        assert card.contacts.website
        assert card.dormitory.get("available") is not None
        assert card.military_department.get("available") is not None
        assert card.programs

        for program in card.programs:
            assert program["code"]
            assert program["name"]
            assert program["description"]
            assert program["tuition_per_year"]
            assert program["passing_score"] is not None
            assert program["exams"]
            assert all("subject" in exam and "min_score" in exam for exam in program["exams"])


def test_seed_university_examples_project_search_filters() -> None:
    card = next(
        item for item in DEMO_UNIVERSITY_CARDS if "Донской" in str(item.canonical_name.value)
    )
    service = UniversitySearchDocProjectionService(repository=None)  # type: ignore[arg-type]
    record = service._build_search_doc(card)

    assert record.search_document["dormitory"]["available"] is True
    assert record.search_document["military_department"]["available"] is True
    assert "38.03.04" in record.search_document["program_codes"]
    assert "Математика" in record.search_document["program_ege_subjects"]
