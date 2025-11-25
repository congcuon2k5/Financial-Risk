# ingestion/app.py
import os
from typing import Optional, List

from fastapi import FastAPI, Body, Query
from pydantic import BaseModel, Field

from kafka_producer import send_event
from simulator import generate_application

app = FastAPI(
    title="Credit Application Simulator (CCTS-mini)",
    version="1.0.0",
)

DEFAULT_FRAUD_RATE = float(os.getenv("DEFAULT_FRAUD_RATE", "0.002"))


class LoanApplicationIn(BaseModel):
    # Cho phép client gửi data tùy chọn; nếu thiếu thì simulator tự đổ.
    SK_ID_CURR: Optional[int] = None
    NAME_CONTRACT_TYPE: Optional[str] = None
    CODE_GENDER: Optional[str] = None
    FLAG_OWN_CAR: Optional[str] = None
    FLAG_OWN_REALTY: Optional[str] = None
    AMT_INCOME_TOTAL: Optional[float] = None
    AMT_CREDIT: Optional[float] = None
    AMT_ANNUITY: Optional[float] = None
    AMT_GOODS_PRICE: Optional[float] = None
    DAYS_BIRTH: Optional[float] = None
    DAYS_EMPLOYED: Optional[float] = None
    NAME_EDUCATION_TYPE: Optional[str] = None
    NAME_FAMILY_STATUS: Optional[str] = None
    NAME_HOUSING_TYPE: Optional[str] = None
    ORGANIZATION_TYPE: Optional[str] = None
    EXT_SOURCE_1: Optional[float] = None
    EXT_SOURCE_2: Optional[float] = None
    EXT_SOURCE_3: Optional[float] = None
    TARGET: Optional[int] = Field(
        default=None,
        description="0=good,1=bad; nếu None sẽ generate theo fraud_rate",
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/loan-application")
def create_loan_application(
    app_in: LoanApplicationIn = Body(...),
    fraud_rate: float = Query(
        DEFAULT_FRAUD_RATE,
        description="Tỷ lệ TARGET=1 khi TARGET không được gửi",
    ),
):
    """
    Nhận 1 hồ sơ vay (hoặc 1 phần), bổ sung field còn thiếu,
    gửi vào Kafka và trả lại event.
    """
    base = generate_application(
        sk_id=app_in.SK_ID_CURR,
        fraud_rate=fraud_rate,
    )

    # override bằng những field client gửi vào
    data = base.copy()
    for field, value in app_in.dict(exclude_unset=True).items():
        if value is not None:
            data[field] = value

    send_event(data, key=data["SK_ID_CURR"])
    return {"status": "sent", "event": data}


class BatchSimRequest(BaseModel):
    n: int = Field(..., gt=0, le=10000, description="Số bản ghi cần sinh")
    fraud_rate: float = Field(
        DEFAULT_FRAUD_RATE,
        ge=0.0,
        le=1.0,
        description="Tỷ lệ TARGET=1",
    )


@app.post("/api/simulate-batch")
def simulate_batch(req: BatchSimRequest):
    """
    Sinh n bản ghi giả và đẩy hết vào Kafka. Dùng để test load.
    """
    for i in range(req.n):
        ev = generate_application(fraud_rate=req.fraud_rate)
        send_event(ev, key=ev["SK_ID_CURR"])

    return {"status": "sent", "count": req.n, "fraud_rate": req.fraud_rate}
