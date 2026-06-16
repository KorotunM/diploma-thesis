from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from libs.domain.university.models import (
    CardVersionInfo,
    ConfidenceValue,
    ContactsInfo,
    FieldAttribution,
    InstitutionalInfo,
    LocationInfo,
    RatingItem,
    ReviewSummary,
    StatsInfo,
    UniversityCard,
)

GENERATED_AT = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)
YEAR = 2026

KUBSU_PASSING_URL = "https://www.kubsu.ru/ru/node/44875"
KUBSU_EXAMS_URL = "https://www.kubsu.ru/sites/default/files/insert/page/2026_exams_b_0.pdf"
KUBSU_TUITION_URL = (
    "https://www.kubsu.ru/sites/default/files/insert/page/"
    "stoimost_obucheniya_na_1_kurse_v_2026-2027_na_sayt_v_razdel_abiturientam.pdf"
)
KUBSU_HISTORY_URL = "https://www.kubsu.ru/ru/university/history-tradition"
KUBSU_DORM_URL = "https://www.kubsu.ru/ru/sveden/grants"

DGTU_BASE_URL = "https://donstu.ru/university/"
DGTU_DORM_URL = (
    "https://donstu.ru/university/struktura/priyemnaya-prorektora-po-adm-khoz-rabote/stud-gorodok/"
)
DGTU_VUC_URL = "https://donstu.ru/university/struktura/voennyy-uchebnyy-tsentr/"
DGTU_GMU_URL = "https://donstu.ru/abiturient/katalog-obrazovatelnykh-programm/1764/"
DGTU_TRANSLATION_URL = "https://donstu.ru/abiturient/katalog-obrazovatelnykh-programm/1749/"
DGTU_HISTORY_PROGRAM_URL = "https://donstu.ru/abiturient/katalog-obrazovatelnykh-programm/1755/"
DGTU_ZOO_URL = "https://donstu.ru/abiturient/katalog-obrazovatelnykh-programm/1771/"

KUBGAU_DOCS_URL = "https://kubsau.ru/entrant/docs/bakalavriat-spetsialitet-magistratura/"
KUBGAU_DIRECTIONS_URL = "https://kubsau.ru/upload/iblock/cf9/yhohmfa7bw8mttdyd5ul25fi87tjvd80.pdf"
KUBGAU_PLACES_URL = "https://kubsau.ru/upload/iblock/492/riph20dgar1sfx73ezbegj90452038ka.pdf"
KUBGAU_EXAMS_URL = "https://kubsau.ru/upload/iblock/fdc/7kwv5zxvgf905i0ruwqwhws5n3um3f0x.pdf"
KUBGAU_TUITION_URL = "https://kubsau.ru/upload/iblock/d34/e71y2vea46thhyeliuojk6ik984z29a7.pdf"
KUBGAU_DORM_URL = "https://kubsau.ru/upload/iblock/170/u9hrj3q550rs3s2aawhul22xycei4hz9.pdf"
KUBGAU_DORM_PLACES_URL = "https://kubsau.ru/upload/iblock/7b9/l26irdegloe04nx0ed86otboed5cxbpc.pdf"
KUBGAU_HISTORY_URL = "https://kubsau.ru/university/"
KUBGAU_VUC_URL = "https://kubsau.ru/education/military/"

KUBGTU_HISTORY_URL = "https://kubstu.ru/s-15"
KUBGTU_ADMISSION_DOCS_URL = "https://kubstu.ru/s-3"
KUBGTU_EXAMS_URL = "https://kubstu.ru/s-750"
KUBGTU_DORM_URL = "https://kubstu.ru/s-67"
KUBGTU_ABIT_URL = "https://abit.kubstu.ru/"


def _uid(slug: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"https://demo.local/universities/{slug}"))


def _source(source_key: str, url: str) -> FieldAttribution:
    return FieldAttribution(source_key=source_key, source_url=url)


def _exam(subject: str, min_score: int, *, required: bool, priority: int) -> dict[str, Any]:
    return {
        "subject": subject,
        "min_score": min_score,
        "required": required,
        "priority": priority,
        "max_score": 100,
    }


def _program(
    index: int,
    *,
    source_key: str,
    source_url: str,
    code: str,
    name: str,
    faculty: str,
    description: str,
    budget_places: int | None,
    paid_places: int | None,
    passing_score: int | None,
    tuition_per_year: int | None,
    exams: list[dict[str, Any]],
    study_form: str = "full_time",
    level: str = "Бакалавриат",
) -> dict[str, Any]:
    return {
        "field_name": f"programs.{index:03d}.{code.replace('.', '')}",
        "faculty": faculty,
        "code": code,
        "name": name,
        "description": description,
        "budget_places": budget_places,
        "paid_places": paid_places,
        "passing_score": passing_score,
        "study_form": study_form,
        "level": level,
        "year": YEAR,
        "tuition_per_year": tuition_per_year,
        "ege_subjects": sorted({str(item["subject"]) for item in exams}),
        "exams": exams,
        "confidence": 0.98,
        "sources": [
            {
                "source_key": source_key,
                "source_url": source_url,
                "evidence_ids": [],
            }
        ],
    }


def _card(
    *,
    slug: str,
    source_key: str,
    source_urls: list[str],
    name: str,
    aliases: list[str],
    description: str,
    history: dict[str, Any],
    city: str,
    region: str,
    address: str,
    website: str,
    emails: list[str],
    phones: list[str],
    founded_year: int,
    category: str,
    programs: list[dict[str, Any]],
    dormitory: dict[str, Any],
    military_department: dict[str, Any],
    rating_value: str,
) -> UniversityCard:
    budget_values = [item["budget_places"] or 0 for item in programs]
    passing_values = [
        item["passing_score"] for item in programs if isinstance(item.get("passing_score"), int)
    ]
    sources = [_source(source_key, url) for url in source_urls]
    return UniversityCard(
        university_id=_uid(slug),
        canonical_name=ConfidenceValue(
            value=name,
            confidence=0.99,
            sources=[sources[0]],
        ),
        aliases=aliases,
        description=description,
        history=history,
        location=LocationInfo(
            country="RU",
            region=region,
            city=city,
            address=address,
        ),
        contacts=ContactsInfo(website=website, emails=emails, phones=phones),
        institutional=InstitutionalInfo.model_validate(
            {
                "type": "Государственный вуз",
                "founded_year": founded_year,
                "category": category,
                "is_flagship": False,
            }
        ),
        programs=programs,
        tuition=[
            {
                "year": YEAR,
                "currency": "RUB",
                "scope": "selected_programs",
                "min": min(
                    item["tuition_per_year"] for item in programs if item["tuition_per_year"]
                ),
                "max": max(
                    item["tuition_per_year"] for item in programs if item["tuition_per_year"]
                ),
            }
        ],
        stats=StatsInfo(
            avg_passing_score=round(mean(passing_values), 1) if passing_values else None,
            budget_places=sum(budget_values),
            programs_count=len(programs),
        ),
        ratings=[
            RatingItem(
                provider="Demo profile completeness",
                year=YEAR,
                metric="official_data_score",
                value=rating_value,
            )
        ],
        dormitory=dormitory,
        military_department=military_department,
        reviews=ReviewSummary(
            summary="Отзывы в демо-наполнении не подставляются без источника.",
            rating=None,
            rating_count=None,
            items=[],
        ),
        sources=sources,
        version=CardVersionInfo(card_version=1, generated_at=GENERATED_AT),
    )


def _kubsu_programs() -> list[dict[str, Any]]:
    geo_exams = [
        _exam("Математика", 40, required=True, priority=1),
        _exam("Русский язык", 45, required=True, priority=3),
        _exam("Информатика", 46, required=False, priority=2),
        _exam("Физика", 41, required=False, priority=2),
    ]
    geography_exams = [
        _exam("Математика", 40, required=True, priority=1),
        _exam("Русский язык", 45, required=True, priority=3),
        _exam("География", 40, required=False, priority=2),
        _exam("Информатика", 46, required=False, priority=2),
    ]
    ecology_exams = geography_exams + [_exam("Биология", 40, required=False, priority=2)]
    cs_exams = [
        _exam("Математика", 40, required=True, priority=1),
        _exam("Информатика", 46, required=True, priority=2),
        _exam("Русский язык", 50, required=True, priority=3),
    ]
    tourism_exams = [
        _exam("Обществознание", 45, required=True, priority=1),
        _exam("Русский язык", 45, required=True, priority=3),
        _exam("История", 40, required=False, priority=2),
        _exam("География", 40, required=False, priority=2),
        _exam("Иностранный язык", 45, required=False, priority=2),
    ]
    biology_exams = [
        _exam("Биология", 42, required=True, priority=1),
        _exam("Русский язык", 45, required=True, priority=3),
        _exam("Химия", 40, required=False, priority=2),
        _exam("Математика", 40, required=False, priority=2),
        _exam("Информатика", 46, required=False, priority=2),
    ]
    rows = [
        (
            "05.03.01",
            "Геология",
            "Институт географии, геологии, туризма и сервиса",
            25,
            182,
            214500,
            geo_exams,
        ),
        (
            "05.03.02",
            "География",
            "Институт географии, геологии, туризма и сервиса",
            25,
            176,
            214500,
            geography_exams,
        ),
        (
            "05.03.03",
            "Картография и геоинформатика",
            "Институт географии, геологии, туризма и сервиса",
            25,
            186,
            214500,
            geography_exams,
        ),
        (
            "05.03.06",
            "Экология и природопользование",
            "Институт географии, геологии, туризма и сервиса",
            25,
            175,
            214500,
            ecology_exams,
        ),
        (
            "43.03.02",
            "Туризм",
            "Институт географии, геологии, туризма и сервиса",
            17,
            222,
            188500,
            tourism_exams,
        ),
        (
            "01.03.02",
            "Прикладная математика и информатика",
            "Факультет математики и компьютерных наук",
            60,
            233,
            245700,
            cs_exams,
        ),
        (
            "02.03.03",
            "Математическое обеспечение и администрирование информационных систем",
            "Факультет математики и компьютерных наук",
            29,
            242,
            245700,
            cs_exams,
        ),
        ("06.03.01", "Биология", "Биологический факультет", 75, 191, 250250, biology_exams),
    ]
    return [
        _program(
            index=index,
            source_key="kubsu-official",
            source_url=KUBSU_PASSING_URL,
            code=code,
            name=name,
            faculty=faculty,
            description=(
                f"Демо-направление КубГУ {code}: {name}. Бюджетные места и проходной балл "
                "взяты со страницы проходных баллов, вступительные испытания - из PDF 2026/27, "
                "стоимость - из приказа о стоимости 1 курса 2026/2027."
            ),
            budget_places=budget,
            paid_places=None,
            passing_score=passing,
            tuition_per_year=tuition,
            exams=exams,
        )
        for index, (code, name, faculty, budget, passing, tuition, exams) in enumerate(
            rows, start=1
        )
    ]


def _dgtu_programs() -> list[dict[str, Any]]:
    return [
        _program(
            1,
            source_key="dgtu-official",
            source_url=DGTU_GMU_URL,
            code="38.03.04",
            name="Государственное управление и территориальное развитие",
            faculty="Инновационный бизнес и менеджмент",
            description=(
                "Подготовка специалистов в области государственного и муниципального управления, "
                "территориального развития и взаимодействия власти, бизнеса и общества."
            ),
            budget_places=0,
            paid_places=20,
            passing_score=130,
            tuition_per_year=135000,
            exams=[
                _exam("Математика", 40, required=True, priority=1),
                _exam("Русский язык", 40, required=True, priority=2),
                _exam("Обществознание", 45, required=False, priority=3),
                _exam("История", 40, required=False, priority=3),
                _exam("Иностранный язык", 40, required=False, priority=3),
            ],
        ),
        _program(
            2,
            source_key="dgtu-official",
            source_url=DGTU_TRANSLATION_URL,
            code="45.03.02",
            name="Перевод и локализация",
            faculty="Прикладная лингвистика",
            description=(
                "Программа готовит переводчиков-локализаторов для ПО, игр, сайтов, "
                "маркетинговых материалов и профессиональной документации."
            ),
            budget_places=7,
            paid_places=20,
            passing_score=228,
            tuition_per_year=135000,
            exams=[
                _exam("Иностранный язык", 40, required=True, priority=1),
                _exam("Русский язык", 40, required=True, priority=2),
                _exam("История", 40, required=False, priority=3),
                _exam("Литература", 40, required=False, priority=3),
                _exam("Обществознание", 45, required=False, priority=3),
            ],
        ),
        _program(
            3,
            source_key="dgtu-official",
            source_url=DGTU_HISTORY_PROGRAM_URL,
            code="44.03.01",
            name="История. Археология",
            faculty="Социально-гуманитарный",
            description=(
                "Подготовка педагогов, сотрудников музеев, архивов и исследователей, "
                "работающих с историческими источниками и археологическими материалами."
            ),
            budget_places=0,
            paid_places=15,
            passing_score=135,
            tuition_per_year=135000,
            exams=[
                _exam("Обществознание", 45, required=True, priority=1),
                _exam("Русский язык", 40, required=True, priority=2),
                _exam("История", 40, required=False, priority=3),
                _exam("Литература", 40, required=False, priority=3),
                _exam("Иностранный язык", 40, required=False, priority=3),
            ],
        ),
        _program(
            4,
            source_key="dgtu-official",
            source_url=DGTU_ZOO_URL,
            code="36.03.02",
            name="Охотоведение, кинология и зоопарковое дело",
            faculty="Биоинженерия и ветеринарная медицина",
            description=(
                "Прикладная программа по зоотехнии, сохранению биоразнообразия, "
                "кинологии и управлению популяциями животных."
            ),
            budget_places=25,
            paid_places=5,
            passing_score=171,
            tuition_per_year=153300,
            exams=[
                _exam("Биология", 40, required=True, priority=1),
                _exam("Русский язык", 40, required=True, priority=2),
                _exam("Математика", 40, required=False, priority=3),
                _exam("Химия", 40, required=False, priority=3),
            ],
        ),
    ]


def _kubgau_programs() -> list[dict[str, Any]]:
    tech_exams = [
        _exam("Математика", 27, required=True, priority=1),
        _exam("Русский язык", 36, required=True, priority=2),
        _exam("Информатика", 40, required=False, priority=3),
        _exam("Физика", 36, required=False, priority=3),
    ]
    agronomy_exams = [
        _exam("Биология", 36, required=True, priority=1),
        _exam("Русский язык", 36, required=True, priority=2),
        _exam("Математика", 27, required=False, priority=3),
        _exam("Химия", 36, required=False, priority=3),
        _exam("География", 37, required=False, priority=3),
        _exam("Физика", 36, required=False, priority=3),
        _exam("Информатика", 40, required=False, priority=3),
    ]
    economy_exams = [
        _exam("Математика", 27, required=True, priority=1),
        _exam("Русский язык", 36, required=True, priority=2),
        _exam("Обществознание", 42, required=False, priority=3),
        _exam("История", 32, required=False, priority=3),
        _exam("География", 37, required=False, priority=3),
        _exam("Информатика", 40, required=False, priority=3),
        _exam("Иностранный язык", 22, required=False, priority=3),
    ]
    veterinary_exams = [
        _exam("Биология", 36, required=True, priority=1),
        _exam("Русский язык", 36, required=True, priority=2),
        _exam("Математика", 27, required=False, priority=3),
        _exam("Химия", 36, required=False, priority=3),
    ]
    rows = [
        (
            "09.03.02",
            "Информационные системы и технологии",
            "Прикладной информатики",
            "Разработка и модификация информационных систем и баз данных",
            65,
            50,
            218,
            230400,
            tech_exams,
        ),
        (
            "09.03.03",
            "Прикладная информатика",
            "Прикладной информатики",
            "Менеджмент ИТ-проектов и жизненного цикла информационных систем",
            65,
            50,
            231,
            230400,
            tech_exams,
        ),
        (
            "13.03.02",
            "Электроэнергетика и электротехника",
            "Энергетики",
            "Электроснабжение",
            40,
            10,
            205,
            213600,
            tech_exams,
        ),
        (
            "35.03.04",
            "Агрономия",
            "Агрономии и экологии",
            "Селекция, генетика и технологии растениеводства",
            79,
            21,
            194,
            213600,
            agronomy_exams,
        ),
        (
            "38.03.01",
            "Экономика",
            "Цифровой экономики и инноваций",
            "Бизнес-аналитика",
            0,
            50,
            165,
            225000,
            economy_exams,
        ),
        (
            "36.05.01",
            "Ветеринария",
            "Ветеринарной медицины, зоотехнии и биотехнологии",
            "Ветеринария",
            95,
            23,
            216,
            223200,
            veterinary_exams,
        ),
    ]
    return [
        _program(
            index=index,
            source_key="kubgau-official",
            source_url=KUBGAU_DOCS_URL,
            code=code,
            name=name,
            faculty=faculty,
            description=(
                f"{profile}. Данные по местам, стоимости и экзаменам взяты "
                "из PDF приемной кампании КубГАУ 2026/27."
            ),
            budget_places=budget,
            paid_places=paid,
            passing_score=passing,
            tuition_per_year=tuition,
            exams=exams,
            level="Специалитет" if code.split(".")[1] == "05" else "Бакалавриат",
        )
        for index, (
            code,
            name,
            faculty,
            profile,
            budget,
            paid,
            passing,
            tuition,
            exams,
        ) in enumerate(rows, start=1)
    ]


def _kubgtu_programs() -> list[dict[str, Any]]:
    it_exams = [
        _exam("Математика", 40, required=True, priority=1),
        _exam("Русский язык", 40, required=True, priority=3),
        _exam("Информатика", 46, required=False, priority=2),
        _exam("Физика", 41, required=False, priority=2),
    ]
    energy_exams = [
        _exam("Физика", 41, required=True, priority=1),
        _exam("Русский язык", 40, required=True, priority=3),
        _exam("Математика", 40, required=False, priority=2),
        _exam("Информатика", 46, required=False, priority=2),
        _exam("Химия", 40, required=False, priority=2),
    ]
    economy_exams = [
        _exam("Русский язык", 40, required=True, priority=3),
        _exam("Математика", 40, required=True, priority=1),
        _exam("История", 40, required=False, priority=2),
        _exam("Обществознание", 45, required=False, priority=2),
        _exam("Иностранный язык", 40, required=False, priority=2),
        _exam("География", 40, required=False, priority=2),
        _exam("Информатика", 46, required=False, priority=2),
    ]
    hotel_exams = [
        _exam("Русский язык", 40, required=True, priority=3),
        _exam("История", 40, required=False, priority=2),
        _exam("География", 40, required=False, priority=2),
        _exam("Иностранный язык", 40, required=False, priority=2),
    ]
    rows = [
        (
            "09.03.01",
            "Искусственный интеллект и машинное обучение",
            "Информационных технологий и кибербезопасности",
            60,
            40,
            212,
            224446,
            it_exams,
        ),
        (
            "09.03.03",
            "Прикладная информатика",
            "Информационных технологий и кибербезопасности",
            90,
            40,
            225,
            224446,
            it_exams,
        ),
        (
            "10.03.01",
            "Информационная безопасность",
            "Информационных технологий и кибербезопасности",
            120,
            0,
            208,
            224446,
            it_exams,
        ),
        (
            "13.03.02",
            "Электроэнергетика и электротехника",
            "Нефти, газа и энергетики",
            85,
            90,
            183,
            230458,
            energy_exams,
        ),
        (
            "15.03.04",
            "Автоматизация технологических процессов и производств",
            "Машиностроения и автосервиса",
            39,
            61,
            223,
            230458,
            energy_exams,
        ),
        (
            "19.03.02",
            "Продукты питания из растительного сырья",
            "Пищевых и перерабатывающих производств",
            85,
            40,
            143,
            187762,
            energy_exams,
        ),
        (
            "38.03.01",
            "Экономика",
            "Экономики, управления и бизнеса",
            73,
            30,
            261,
            169000,
            economy_exams,
        ),
        ("43.03.03", "Гостиничное дело", "Сервиса и туризма", 0, 70, 137, 159500, hotel_exams),
    ]
    return [
        _program(
            index=index,
            source_key="kubgtu-official",
            source_url=KUBGTU_ADMISSION_DOCS_URL,
            code=code,
            name=name,
            faculty=faculty,
            description=(
                f"Направление КубГТУ {code}: {name}. План приема и стоимость взяты "
                "со страницы документов приема 2026, минимальные баллы - "
                "со страницы вступительных испытаний."
            ),
            budget_places=budget,
            paid_places=paid,
            passing_score=passing,
            tuition_per_year=tuition,
            exams=exams,
        )
        for index, (code, name, faculty, budget, paid, passing, tuition, exams) in enumerate(
            rows, start=1
        )
    ]


def build_university_cards() -> list[UniversityCard]:
    return [
        _card(
            slug="kubsu",
            source_key="kubsu-official",
            source_urls=[
                KUBSU_HISTORY_URL,
                KUBSU_PASSING_URL,
                KUBSU_EXAMS_URL,
                KUBSU_TUITION_URL,
                KUBSU_DORM_URL,
            ],
            name="Кубанский государственный университет",
            aliases=["КубГУ", "Kuban State University"],
            description=(
                "Классический университет Краснодара с программами в области естественных, "
                "математических, гуманитарных, экономических и педагогических наук. "
                "Демо-карточка заполнена официальными данными приемной кампании 2026/27."
            ),
            history={
                "founded_year": 1920,
                "summary": (
                    "КубГУ ведет историю с открытия университета в Краснодаре "
                    "в сентябре 1920 года; "
                    "первым ректором был историк Никандр Маркс."
                ),
                "source_url": KUBSU_HISTORY_URL,
            },
            city="Краснодар",
            region="Краснодарский край",
            address="350040, г. Краснодар, ул. Ставропольская, 149",
            website="https://www.kubsu.ru",
            emails=["priem@kubsu.ru"],
            phones=["+7 (861) 219-95-07"],
            founded_year=1920,
            category="A",
            programs=_kubsu_programs(),
            dormitory={
                "available": True,
                "count": 5,
                "places_count": None,
                "note": (
                    "В официальных сведениях указано наличие общежитий; "
                    "количество общежитий - 5."
                ),
                "source_url": KUBSU_DORM_URL,
            },
            military_department={
                "available": False,
                "note": (
                    "Актуальная страница ВУЦ в официальных материалах "
                    "приемной кампании не подтверждена."
                ),
                "source_url": "https://www.kubsu.ru/ru/abiturient",
            },
            rating_value="88",
        ),
        _card(
            slug="dgtu",
            source_key="dgtu-official",
            source_urls=[DGTU_BASE_URL, DGTU_DORM_URL, DGTU_VUC_URL, DGTU_GMU_URL],
            name="Донской государственный технический университет",
            aliases=["ДГТУ", "DSTU", "Don State Technical University"],
            description=(
                "Опорный технический университет Ростовской области и крупный инновационный "
                "центр Юга России. В демо включены программы из официального каталога ДГТУ."
            ),
            history={
                "founded_year": 1930,
                "summary": (
                    "ДГТУ ведет историю с 1930 года и развивался как "
                    "технический вуз Юга России."
                ),
                "source_url": DGTU_BASE_URL,
            },
            city="Ростов-на-Дону",
            region="Ростовская область",
            address="344003, г. Ростов-на-Дону, пл. Гагарина, 1",
            website="https://donstu.ru",
            emails=["spu-33@donstu.ru"],
            phones=["8 800 100 19 30"],
            founded_year=1930,
            category="A",
            programs=_dgtu_programs(),
            dormitory={
                "available": True,
                "count": 8,
                "places_count": 4334,
                "note": "Студенческий городок ДГТУ: 8 общежитий и 4334 места.",
                "source_url": DGTU_DORM_URL,
            },
            military_department={
                "available": True,
                "note": "В структуре ДГТУ указан военный учебный центр.",
                "source_url": DGTU_VUC_URL,
            },
            rating_value="90",
        ),
        _card(
            slug="kubgau",
            source_key="kubgau-official",
            source_urls=[
                KUBGAU_HISTORY_URL,
                KUBGAU_DOCS_URL,
                KUBGAU_PLACES_URL,
                KUBGAU_EXAMS_URL,
                KUBGAU_TUITION_URL,
                KUBGAU_VUC_URL,
            ],
            name="Кубанский государственный аграрный университет имени И. Т. Трубилина",
            aliases=["КубГАУ", "Kuban State Agrarian University"],
            description=(
                "Один из крупнейших аграрных вузов России, центр образования, науки и "
                "инноваций агропромышленного комплекса. Данные направлений взяты из PDF "
                "приемной кампании 2026/27."
            ),
            history={
                "founded_year": 1922,
                "summary": (
                    "КубГАУ создан в 1922 году как Кубанский сельскохозяйственный институт; "
                    "сейчас университет объединяет десятки факультетов и образовательных программ."
                ),
                "source_url": KUBGAU_HISTORY_URL,
            },
            city="Краснодар",
            region="Краснодарский край",
            address="350044, г. Краснодар, ул. Калинина, 13",
            website="https://kubsau.ru",
            emails=["pk@kubsau.ru", "mail@kubsau.ru"],
            phones=["+7 (861) 221-58-18", "+7 (861) 221-54-85"],
            founded_year=1922,
            category="A",
            programs=_kubgau_programs(),
            dormitory={
                "available": True,
                "count": 20,
                "places_count": 1737,
                "note": "Документы приема 2026/27 указывают 20 общежитий и 1737 мест.",
                "source_url": KUBGAU_DORM_PLACES_URL,
            },
            military_department={
                "available": True,
                "note": "В университете действует военный учебный центр.",
                "source_url": KUBGAU_VUC_URL,
            },
            rating_value="89",
        ),
        _card(
            slug="kubgtu",
            source_key="kubgtu-official",
            source_urls=[
                KUBGTU_HISTORY_URL,
                KUBGTU_ADMISSION_DOCS_URL,
                KUBGTU_EXAMS_URL,
                KUBGTU_DORM_URL,
                KUBGTU_ABIT_URL,
            ],
            name="Кубанский государственный технологический университет",
            aliases=["КубГТУ", "Кубанский Политех", "Kuban State Technological University"],
            description=(
                "Старейший технический вуз Кубани и Северного Кавказа. Карточка содержит "
                "направления из документов приема 2026, вступительные испытания "
                "и стоимость обучения."
            ),
            history={
                "founded_year": 1918,
                "summary": (
                    "КубГТУ основан 16 июня 1918 года как Северо-Кавказский политехнический "
                    "институт, первый вуз Кубани."
                ),
                "source_url": KUBGTU_HISTORY_URL,
            },
            city="Краснодар",
            region="Краснодарский край",
            address="350072, г. Краснодар, ул. Московская, 2",
            website="https://kubstu.ru",
            emails=["priem@kubstu.ru"],
            phones=["+7 (861) 255-25-32"],
            founded_year=1918,
            category="A",
            programs=_kubgtu_programs(),
            dormitory={
                "available": True,
                "count": None,
                "places_count": None,
                "note": (
                    "Официальная страница студенческой информации содержит "
                    "документы по общежитиям и стоимости проживания."
                ),
                "source_url": KUBGTU_DORM_URL,
            },
            military_department={
                "available": False,
                "note": (
                    "Актуальная страница военного учебного центра на официальном "
                    "сайте не найдена; есть только документы по воинскому учету."
                ),
                "source_url": KUBGTU_DORM_URL,
            },
            rating_value="87",
        ),
    ]


DEMO_UNIVERSITY_CARDS = build_university_cards()
