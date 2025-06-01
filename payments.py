from yookassa import Configuration, Payment

Configuration.account_id = "497610"
Configuration.secret_key = "live_lw2lVKYBA6ExP2VrXg220ZfPlusNJqNs8BsByoNYZbo"

def create_payment(amount, description, return_url, metadata=None):
    payment = Payment.create({
        "amount": {
            "value": f"{amount:.2f}",
            "currency": "RUB"
        },
        "confirmation": {
            "type": "redirect",
            "return_url": return_url
        },
        "capture": True,
        "description": description,
        "metadata": metadata or {},
    })
    return payment

def check_payment(payment_id):
    payment = Payment.find_one(payment_id)
    return payment.status  # 'pending', 'succeeded', 'canceled', и др.
