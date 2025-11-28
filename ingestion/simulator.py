# ingestion/simulator.py
import os
import random
from datetime import datetime
from typing import Optional

DEFAULT_FRAUD_RATE = float(os.getenv("DEFAULT_FRAUD_RATE", "0.002"))  # 0.2%

CONTRACT_TYPES = ["Cash loans", "Revolving loans"]
GENDERS = ["M", "F"]
YES_NO = ["Y", "N"]
EDUCATION_TYPES = [
    "Secondary / secondary special",
    "Higher education",
    "Incomplete higher",
    "Lower secondary",
]
FAMILY_STATUS = [
    "Single / not married",
    "Married",
    "Separated",
    "Widow",
]
HOUSING_TYPES = [
    "House / apartment",
    "Rented apartment",
    "Municipal apartment",
    "Office apartment",
]
ORG_TYPES = [
    "Business Entity Type 1",
    "Self-employed",
    "Government",
    "Other",
]


def generate_application(
    sk_id: Optional[int] = None,
    fraud_rate: Optional[float] = None,
) -> dict:
    """
    Sinh 1 hồ sơ vay giả lập.
    TARGET = 1 ~ 'xấu' (fraud / default) theo tỷ lệ fraud_rate.
    """
    if sk_id is None:
        sk_id = random.randint(100000, 999999)

    if fraud_rate is None:
        fraud_rate = DEFAULT_FRAUD_RATE

    age_years = random.randint(21, 70)
    days_birth = -age_years * 365
    days_employed = -random.randint(0, 365 * 40)

    income = float(random.randint(30_000, 500_000))
    credit = float(random.randint(10_000, 1_000_000))
    annuity = credit / random.uniform(12, 72)
    goods_price = credit * random.uniform(0.8, 1.2)

    ext1 = random.uniform(0.0, 1.0)
    ext2 = random.uniform(0.0, 1.0)
    ext3 = random.uniform(0.0, 1.0)

    target = 1 if random.random() < fraud_rate else 0

    event = {
        "SK_ID_CURR": sk_id,
        "NAME_CONTRACT_TYPE": random.choice(CONTRACT_TYPES),
        "CODE_GENDER": random.choice(GENDERS),
        "FLAG_OWN_CAR": random.choice(YES_NO),
        "FLAG_OWN_REALTY": random.choice(YES_NO),
        "AMT_INCOME_TOTAL": income,
        "AMT_CREDIT": credit,
        "AMT_ANNUITY": annuity,
        "AMT_GOODS_PRICE": goods_price,
        "DAYS_BIRTH": float(days_birth),
        "DAYS_EMPLOYED": float(days_employed),
        "NAME_EDUCATION_TYPE": random.choice(EDUCATION_TYPES),
        "NAME_FAMILY_STATUS": random.choice(FAMILY_STATUS),
        "NAME_HOUSING_TYPE": random.choice(HOUSING_TYPES),
        "ORGANIZATION_TYPE": random.choice(ORG_TYPES),
        "EXT_SOURCE_1": ext1,
        "EXT_SOURCE_2": ext2,
        "EXT_SOURCE_3": ext3,
        # label + thời điểm event
        "TARGET": target,
        "event_time": datetime.utcnow().isoformat(),
    }
    return event
