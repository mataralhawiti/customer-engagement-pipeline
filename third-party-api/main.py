"""
Minimal FastAPI app to simulate third-party service integration.
Receives enriched events from the streaming pipeline and logs them.
"""

import logging
from typing import List, Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s"
)
logger = logging.getLogger("third-party-api")

app = FastAPI(title="Third-Party API Simulator", version="1.0.0")


class EventBatch(BaseModel):
    """Model for batch of enriched events"""
    events: List[Dict[str, Any]]


@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "third-party-api"}


@app.post("/events")
async def receive_events(batch: EventBatch):
    """
    Receive batch of enriched events from the streaming pipeline.
    Logs the events and returns success response.
    """
    events = batch.events
    event_count = len(events)
    
    logger.info(f"Received batch of {event_count} enriched events")
    
    # Log sample of events for debugging (first 3 events)
    for i, event in enumerate(events[:3]):
        logger.info(f"Event {i+1}: id={event.get('id')}, "
                   f"content_id={event.get('content_id')}, "
                   f"event_type={event.get('event_type')}, "
                   f"engagement_pct={event.get('engagement_pct')}")
    
    if event_count > 3:
        logger.info(f"... and {event_count - 3} more events")
    
    # Simulate successful processing
    response = {
        "status": "success",
        "processed_count": event_count,
        "message": f"Successfully processed {event_count} events"
    }
    
    logger.info(f"Responding with: {response}")
    return response


@app.get("/stats")
async def get_stats():
    """Get basic stats (could be extended to track metrics)"""
    return {
        "total_batches_received": "tracked_in_logs",
        "service_uptime": "check_logs",
        "last_batch_time": "check_logs"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
