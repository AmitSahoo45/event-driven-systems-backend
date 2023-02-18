import json
import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel

from dotenv import load_dotenv

from consumers import CONSUMER

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis = get_redis_connection(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    password=os.getenv("DB_PASSWORD"),
    decode_responses=True
)


class Delivery(HashModel):
    budget: int = 0
    notes: str = ""

    class Meta:
        database = redis


class Event(HashModel):
    delivery_id: str = None
    type: str  # event type
    data: str  # THE body or json taht we will send through the request

    class Meta:
        database = redis


def build_state(pk: str):
    primary_key = Event.all_pks()
    all_events = [Event.get(pk) for pk in primary_key]
    events = [event for event in all_events if event.delivery_id == pk]
    state = {}

    for event in events:
        state = CONSUMER[event.type](state, event)

    return events


@app.get('/deliveries/{pk}/status')
async def get_state(pk: str):
    state = redis.get(f'delivery:{pk}')

    if state is not None:
        return json.loads(state)

    state = build_state(pk)
    redis.set(f'delivery:{pk}', json.dumps(state))
    return {}


@app.post("/deliveries/create")
async def create(request: Request):
    body = await request.json()
    delivery = Delivery(budget=body['data']
                        ['budget'], notes=body['data']['notes']).save()
    event = Event(delivery_id=delivery.pk,
                  type=body['type'], data=json.dumps(body['data'])).save()
    state = CONSUMER[event.type]({}, event)
    # this will return the state that has to be handled in the frontend
    redis.set(f'delivery:{delivery.pk}', json.dumps(state))
    return state


@app.post("/deliveries/events")
async def dispatch(request: Request):
    body = await request.json()
    delivery_id = body['delivery_id']
    event = Event(
        delivery_id=delivery_id,
        type=body['type'],
        data=json.dumps(body['data'])
    ).save()
    state = await get_state(delivery_id)
    new_state = CONSUMER[event.type](state, event)
    # Rather than this we can also use CONSUMNER[event.type](state, event)
    redis.set(f'delivery:{delivery_id}', json.dumps(new_state))
    return new_state
