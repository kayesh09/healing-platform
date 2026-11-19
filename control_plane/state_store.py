import redis.asyncio as redis
import json
from typing import Optional, Dict, List
from .models import ServiceState, ServiceSpec, ServiceStatus
from datetime import datetime, timezone


def utcnow():
    return datetime.now(timezone.utc)


class StateStore:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.key_prefix = "platform:service:"
        self.all_services_key = "platform:services"

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        print(f"✓ Redis connected at {self.redis_url}")

    async def close(self):
        if self.redis:
            await self.redis.close()

    def _key(self, name: str) -> str:
        return f"{self.key_prefix}{name}"

    async def register_service(self, spec: ServiceSpec) -> ServiceState:
        state = ServiceState(spec=spec)
        await self.redis.set(self._key(spec.name), state.model_dump_json())
        await self.redis.sadd(self.all_services_key, spec.name)
        return state

    async def get_service(self, name: str) -> Optional[ServiceState]:
        data = await self.redis.get(self._key(name))
        if not data:
            return None
        return ServiceState.model_validate_json(data)

    async def update_service(self, state: ServiceState) -> ServiceState:
        state.updated_at = utcnow()
        await self.redis.set(self._key(state.spec.name), state.model_dump_json())
        return state

    async def delete_service(self, name: str):
        await self.redis.delete(self._key(name))
        await self.redis.srem(self.all_services_key, name)

    async def list_services(self) -> List[ServiceState]:
        names = await self.redis.smembers(self.all_services_key)
        services = []
        for name in names:
            svc = await self.get_service(name)
            if svc:
                services.append(svc)
        return services

    async def get_actual_state(self, name: str) -> Optional[Dict]:
        data = await self.redis.get(f"platform:actual:{name}")
        return json.loads(data) if data else None

    async def set_actual_state(self, name: str, state: Dict):
        await self.redis.set(f"platform:actual:{name}", json.dumps(state))

    async def acquire_lock(self, name: str, timeout: int = 30) -> bool:
        return await self.redis.set(f"platform:lock:{name}", "1", nx=True, ex=timeout)

    async def release_lock(self, name: str):
        await self.redis.delete(f"platform:lock:{name}")