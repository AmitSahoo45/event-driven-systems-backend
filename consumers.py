import json
from fastapi import HTTPException


def create_delivery(state, event):
    data = json.loads(event.data)
    return {
        "id": event.delivery_id,
        "budget": int(data["budget"]),
        "notes": data["notes"],
        "status": "ready",
        # This is an extra value that frontend wont provide but we need this value in order to track the delivery and this will change as the delivery progresses and different events are triggered
    }


def start_delivery(state, event):
    if state['status'] != 'ready':
        raise HTTPException(
            status_code=400,
            detail="Delivery has started"
        )
    return state | {"status": "active"}


def pickup_delivery(state, event):
    data = json.loads(event.data)
    new_budget = state['budget'] - \
        int(data['purchased_price']) * int(data['quantity'])

    if new_budget < 0:
        raise HTTPException(
            status_code=400,
            detail="Not enough budget"
        )

    return state | {
        "budget": new_budget,
        "purchased_price": int(data["purchased_price"]),
        "quantity": int(data["quantity"]),
        "status": "collected",
    }


def deliver_products(state, event):
    data = json.loads(event.data)
    new_budget = state['budget'] - \
        int(data['selling_price']) * int(data['quantity'])
    new_quantity = state['quantity'] - int(data['quantity'])

    if new_quantity < 0:
        raise HTTPException(
            status_code=400,
            detail="Not enough products"
        )

    return state | {
        "budget": new_budget,
        "quantity": new_quantity,
        "selling_price": int(data["selling_price"]),
        "status": "collected",
    }


def increase_budget(state, event):
    data = json.loads(event.data)
    state['budget'] += int(data['budget'])
    return state


CONSUMER = {
    "CREATE_DELIVERY": create_delivery,
    "START_DELIVERY": start_delivery,
    "PICKUP_DELIVERY": pickup_delivery,
    "DELIVER_PRODUCTS": deliver_products,
    "INCREASE_BUDGET": increase_budget,
}
