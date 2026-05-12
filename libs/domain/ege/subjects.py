from __future__ import annotations

# Canonical list of EGE subjects — IDs must match what the frontend uses.
EGE_SUBJECTS_CATALOG: list[dict[str, str]] = [
    {"id": "russian",     "label": "Русский язык"},
    {"id": "math",        "label": "Математика"},
    {"id": "physics",     "label": "Физика"},
    {"id": "chemistry",   "label": "Химия"},
    {"id": "biology",     "label": "Биология"},
    {"id": "informatics", "label": "Информатика"},
    {"id": "social",      "label": "Обществознание"},
    {"id": "history",     "label": "История"},
    {"id": "literature",  "label": "Литература"},
    {"id": "geography",   "label": "География"},
    {"id": "foreign",     "label": "Иностранные языки"},
]

# OKSO group (first 2 digits of code like "09.03.01") → required EGE subject IDs.
# Source: типовые требования к вступительным испытаниям Минобрнауки России.
_OKSO_GROUP_SUBJECTS: dict[str, list[str]] = {
    "01": ["russian", "math", "physics"],        # Математика и механика
    "02": ["russian", "math", "informatics"],    # Компьютерные и информационные науки
    "03": ["russian", "physics", "chemistry"],   # Физика и астрономия
    "04": ["russian", "chemistry", "math"],      # Химические науки
    "05": ["russian", "math", "chemistry"],      # Науки о Земле
    "06": ["russian", "biology", "chemistry"],   # Биологические науки
    "07": ["russian", "math", "social"],         # Архитектура
    "08": ["russian", "math", "physics"],        # Техника и технологии строительства
    "09": ["russian", "math", "informatics"],    # Информатика и вычислительная техника
    "10": ["russian", "math", "informatics"],    # Информационная безопасность
    "11": ["russian", "math", "physics"],        # Электроника, радиотехника и системы связи
    "12": ["russian", "math", "physics"],        # Фотоника, приборостроение
    "13": ["russian", "math", "physics"],        # Электро- и теплоэнергетика
    "14": ["russian", "math", "physics"],        # Ядерная энергетика и технологии
    "15": ["russian", "math", "physics"],        # Машиностроение
    "16": ["russian", "math", "physics"],        # Физтех, приборостроение
    "17": ["russian", "math", "physics"],        # Оружие и системы вооружения
    "18": ["russian", "math", "chemistry"],      # Химические технологии
    "19": ["russian", "math", "chemistry"],      # Промышленная экология и биотехнологии
    "20": ["russian", "math", "chemistry"],      # Техносферная безопасность
    "21": ["russian", "math", "physics"],        # Прикладная геология
    "22": ["russian", "math", "physics"],        # Технологии материалов
    "23": ["russian", "math", "physics"],        # Техника и технологии наземного транспорта
    "24": ["russian", "math", "physics"],        # Авиационная и ракетно-космическая техника
    "25": ["russian", "math", "physics"],        # Аэронавигация
    "26": ["russian", "math", "physics"],        # Техника и технологии кораблестроения
    "27": ["russian", "math", "physics"],        # Управление в технических системах
    "28": ["russian", "math", "physics"],        # Нанотехнологии и наноматериалы
    "29": ["russian", "math", "chemistry"],      # Технологии лёгкой промышленности
    "30": ["russian", "biology", "chemistry"],   # Фундаментальная медицина
    "31": ["russian", "biology", "chemistry"],   # Клиническая медицина
    "32": ["russian", "biology", "chemistry"],   # Науки о здоровье и профилактическая медицина
    "33": ["russian", "biology", "chemistry"],   # Фармация
    "34": ["russian", "biology", "social"],      # Сестринское дело
    "35": ["russian", "biology", "chemistry"],   # Сельское, лесное и рыбное хозяйство
    "36": ["russian", "biology", "chemistry"],   # Ветеринария и зоотехния
    "37": ["russian", "biology", "social"],      # Психологические науки
    "38": ["russian", "math", "social"],         # Экономика и управление
    "39": ["russian", "social", "history"],      # Социология и социальная работа
    "40": ["russian", "social", "history"],      # Юриспруденция
    "41": ["russian", "social", "history"],      # Политические науки и регионоведение
    "42": ["russian", "social", "literature"],   # СМИ и информационно-библиотечное дело
    "43": ["russian", "social", "math"],         # Сервис и туризм
    "44": ["russian", "social", "biology"],      # Образование и педагогические науки
    "45": ["russian", "foreign", "history"],     # Языкознание и литературоведение
    "46": ["russian", "history", "social"],      # История и археология
    "47": ["russian", "social", "history"],      # Философия, этика и религиоведение
    "48": ["russian", "social", "history"],      # Теология
    "49": ["russian", "biology", "social"],      # Физическая культура и спорт
    "51": ["russian", "literature", "history"],  # Культуроведение и социокультурные проекты
    "52": ["russian", "literature", "history"],  # Изобразительное и прикладные виды искусств
    "53": ["russian", "literature", "history"],  # Музыкальное искусство
    "54": ["russian", "social", "literature"],   # Дизайн
    "55": ["russian", "social", "history"],      # Экранные искусства
    "56": ["russian", "social", "history"],      # Военное управление
    "58": ["russian", "social", "history"],      # Востоковедение и африканистика
}

_DEFAULT_SUBJECTS: list[str] = ["russian", "math"]


def ege_subjects_for_okso(code: str) -> list[str]:
    """Return required EGE subject IDs for a given OKSO program code (e.g. '09.03.01')."""
    if not code or len(code) < 2:
        return list(_DEFAULT_SUBJECTS)
    group = code[:2]
    return list(_OKSO_GROUP_SUBJECTS.get(group, _DEFAULT_SUBJECTS))
