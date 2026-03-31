"""
business_hours.py — Calcolo minuti lavorativi effettivi.
Orario: Lunedì-Venerdì, 08:30-17:30. Festività nazionali italiane.
"""

from datetime import date, datetime, timedelta
from typing import Optional

WORK_START_MINUTES = 8 * 60 + 30    # 510
WORK_END_MINUTES   = 17 * 60 + 30   # 1050
WORK_MINUTES_PER_DAY = WORK_END_MINUTES - WORK_START_MINUTES  # 540


def _easter(year: int) -> date:
    a = year % 19; b = year // 100; c = year % 100
    d = b // 4; e = b % 4; f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4; k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)

_holidays_cache: dict = {}

def get_italian_holidays(year: int) -> set:
    easter = _easter(year)
    return {
        date(year, 1, 1), date(year, 1, 6),
        easter, easter + timedelta(days=1),
        date(year, 4, 25), date(year, 5, 1), date(year, 6, 2),
        date(year, 8, 15), date(year, 11, 1), date(year, 12, 8),
        date(year, 12, 25), date(year, 12, 26),
    }

def is_working_day(d: date) -> bool:
    """True se il giorno è lun-ven e non è festivo."""
    if d.weekday() >= 5:
        return False
    year = d.year
    if year not in _holidays_cache:
        _holidays_cache[year] = get_italian_holidays(year)
    return d not in _holidays_cache[year]


def business_minutes(start: datetime, end: datetime) -> Optional[float]:
    """
    Calcola i minuti lavorativi tra start ed end.
    Per ogni giorno nel range, calcola l'intersezione tra [start,end]
    e la finestra lavorativa 08:30-17:30 del giorno (se lavorativo).
    """
    if start is None or end is None:
        return None
    if getattr(start, 'tzinfo', None) is not None:
        start = start.replace(tzinfo=None)
    if getattr(end, 'tzinfo', None) is not None:
        end = end.replace(tzinfo=None)
    if end <= start:
        return 0.0

    total = 0.0
    current = start.date()
    last    = end.date()

    while current <= last:
        if is_working_day(current):
            day_start = datetime(current.year, current.month, current.day,
                                 WORK_START_MINUTES // 60, WORK_START_MINUTES % 60)
            day_end   = datetime(current.year, current.month, current.day,
                                 WORK_END_MINUTES // 60, WORK_END_MINUTES % 60)
            w_start = max(start, day_start)
            w_end   = min(end,   day_end)
            if w_end > w_start:
                total += (w_end - w_start).total_seconds() / 60
        current += timedelta(days=1)

    return round(total, 1)


def business_hours(start: datetime, end: datetime) -> Optional[float]:
    mins = business_minutes(start, end)
    return round(mins / 60, 2) if mins is not None else None


if __name__ == "__main__":
    # Nota: 6 gennaio = Epifania (festivo), quindi usiamo 10-13 gennaio
    tests = [
        # ven 10/1 17:00 → lun 13/1 10:00 = 30min(ven) + 90min(lun) = 120
        (datetime(2025, 1, 10, 17, 0),  datetime(2025, 1, 13, 10, 0),  120,  "Venerdì 17:00 → Lunedì 10:00"),
        # stesso giorno 09:00-11:00 = 120 min
        (datetime(2025, 1, 7,   9, 0),  datetime(2025, 1, 7,  11, 0),  120,  "Stesso giorno 09:00 → 11:00"),
        # 24/12 16:00 → 27/12 10:00: 24(90min) + 25(Natale) + 26(S.Stefano) + 27(90min) = 180
        (datetime(2025, 12, 24, 16, 0), datetime(2025, 12, 27, 10, 0), 180,  "24/12 16:00 → 27/12 10:00"),
        # sabato 11/1 → lunedì 13/1 10:00 = solo 90min lunedì
        (datetime(2025, 1, 11, 15, 0),  datetime(2025, 1, 13, 10, 0),   90,  "Sabato 15:00 → Lunedì 10:00"),
        # giornata intera
        (datetime(2025, 1, 7,   8, 30), datetime(2025, 1, 7,  17, 30), 540,  "Giornata intera 08:30-17:30"),
        # gio 17/4 → mer 23/4: 17(mer) + 18(ven) + Pasqua(20 dom) + LunAngelo(21) + 22(mar) + 23(mer) 
        # giorni lav: 17(gio)=540, 22(mar)=540, 23(mer fino 10:00)=90 = 1170? 
        # aspetta: 17 apr 2025 è giovedì, quindi 17(gio)=1g, 18(ven)=1g, 19(sab)=no, 20(dom Pasqua)=no, 
        # 21(lun Angelo)=no, 22(mar)=1g → tot 3gg lavorativi = 3*540=1620 ... no
        # Semplifichiamo: thu 17/4 10:00 → mon 22/4 10:00
        # 17 gio: 10:00-17:30 = 450min, 18 ven: 540min, 22 mar: 08:30-10:00 = 90min = 1080
        (datetime(2025, 4, 17, 10, 0),  datetime(2025, 4, 22, 10, 0),  1080, "Giovedì → martedì con Pasqua+LunedìAngelo"),
    ]
    ok = 0
    for s, e, expected, label in tests:
        result = business_minutes(s, e)
        status = "✅" if result == expected else "❌"
        print(f"{status} {label}: {result} min (atteso: {expected})")
        if result == expected:
            ok += 1
    print(f"\n{ok}/{len(tests)} test passati")
    print(f"\nFestività 2025: {sorted(get_italian_holidays(2025))}")