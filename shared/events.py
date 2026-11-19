import redis.asyncio as redis
import json
from typing import Callable, Dict, Any
import asyncio
from datetime import datetime


class EventBus:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.pubsub = None
        self.redis = None

    async def connect(self):
        self.redis = redis.from_url(self.redis_url)
        self.pubsub = self.redis.pubsub()

    async def publish(self, channel: str, event: Dict[str, Any]):
        """Publish event to channel"""
        if not self.redis:
            await self.connect()
        event["timestamp"] = datetime.utcnow().isoformat()
        await self.redis.publish(channel, json.dumps(event))

    async def subscribe(self, channel: str, handler: Callable[[Dict], None]):
        """Subscribe to channel with handler"""
        if not self.pubsub:
            await self.connect()
        await self.pubsub.subscribe(channel)

        async for message in self.pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await handler(data)
                except Exception as e:
                    print(f"Error handling event: {e}")

    async def close(self):
        if self.pubsub:
            await self.pubsub.close()
        if self.redis:
            await self.redis.close()


# Event types
class Events:
    SERVICE_REGISTERED = "service.registered"
    SERVICE_UPDATED = "service.updated"
    SERVICE_DELETED = "service.deleted"
    DEPLOY_REQUESTED = "deploy.requested"
    DEPLOY_COMPLETED = "deploy.completed"
    DEPLOY_FAILED = "deploy.failed"
    SCALE_REQUESTED = "scale.requested"
    SCALE_COMPLETED = "scale.completed"
    CONTAINER_CRASHED = "container.crashed"
    CONTAINER_RESTARTED = "container.restarted"
    CIRCUIT_BROKEN = "circuit.broken"
    CIRCUIT_RESET = "circuit.reset"
    BLUE_GREEN_DEPLOY = "bluegreen.deploy"
    BLUE_GREEN_SWITCH = "bluegreen.switch"
    ROLLBACK = "rollback"
    HEALTH_CHECK_FAILED = "health.failed"
    HEALTH_CHECK_PASSED = "health.passed"