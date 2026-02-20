import json
import random
import uuid
from datetime import datetime, UTC
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# --- Kafka Configuration ---
conf = {
    'bootstrap.servers': 'localhost:9092',  # Default for local Kafka
    'client.id': 'logistics-generator'
}

producer = Producer(conf)

# Callback to confirm delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# --- Data Model ---
class LogisticsEvent(BaseModel):
    event_id: str
    order_id: str
    timestamp: str
    status: str
    origin: str
    destination: str
    carrier_name: str
    latitude: float
    longitude: float
    weight_kg: float
    estimated_delivery: str

def generate_random_event(order_id: Optional[str] = None):
    statuses = ["Order Created", "Picked Up", "In Transit", "Out for Delivery", "Delivered", "Delayed"]
    return LogisticsEvent(
        event_id=str(uuid.uuid4()),
        order_id=order_id or f"ORD-{fake.bothify(text='??-####')}",
        timestamp=datetime.now(UTC).isoformat(),
        status=random.choice(statuses),
        origin=f"{fake.city()}, {fake.country_code()}",
        destination=f"{fake.city()}, {fake.country_code()}",
        carrier_name=random.choice(["BlueDart", "FedEx", "DHL", "SwiftLogistics"]),
        latitude=float(fake.latitude()),
        longitude=float(fake.longitude()),
        weight_kg=round(random.uniform(1.0, 500.0), 2),
        estimated_delivery=fake.date_between(start_date='today', end_date='+10d').isoformat()
    )

# --- Lifespan context manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting up - Kafka producer initialized")
    yield
    # Shutdown
    print("Shutting down - Flushing Kafka producer")
    producer.flush()

# --- FastAPI App ---
app = FastAPI(
    title="Logistics Synthetic Data API",
    lifespan=lifespan
)

# --- Root endpoint ---
@app.get("/")
def root():
    """API documentation and status."""
    return {
        "message": "Logistics Synthetic Data API",
        "endpoints": {
            "health": "GET /health",
            "generate": "POST /generate",
            "generate_batch": "POST /generate-batch?count=10",
            "docs": "GET /docs"
        }
    }

# --- Health Check ---
@app.get("/health")
def health_check():
    return {"status": "alive", "kafka_broker": "localhost:9092"}

# --- Generate Single Event ---
@app.post("/generate", status_code=201)
async def trigger_event():
    event = generate_random_event()
    
    try:
        # Use model_dump_json()
        payload = event.model_dump_json().encode('utf-8')
        
        # Produce to the 'logistics-events' topic
        producer.produce(
            'logistics-events', 
            key=event.order_id, 
            value=payload, 
            callback=delivery_report
        )
        
        # Poll ensures the delivery report callback is triggered
        producer.poll(0) 
        
        return {"status": "sent_to_kafka", "data": event}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Generate Batch Events ---
@app.post("/generate-batch", status_code=201)
async def trigger_batch_events(count: int = 10):
    """Generate and send multiple events to Kafka."""
    if count < 1 or count > 1000:
        raise HTTPException(status_code=400, detail="count must be between 1 and 1000")
    
    events = []
    try:
        for _ in range(count):
            event = generate_random_event()
            payload = event.model_dump_json().encode('utf-8')
            
            producer.produce(
                'logistics-events',
                key=event.order_id,
                value=payload,
                callback=delivery_report
            )
            events.append(event)
        
        producer.poll(0)
        
        return {
            "status": "sent_to_kafka",
            "count": count,
            "data": events
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)