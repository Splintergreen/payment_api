from pydantic import BaseModel


class PaymentRequest(BaseModel):
    user_id: str
    amount: float
    currency: str
    card_number: str
    status: str
