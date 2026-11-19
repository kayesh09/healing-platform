import asyncio
from typing import Dict
from datetime import datetime, timezone

from shared.docker_client import DockerClient
from shared.events import EventBus, Events
from shared.metrics import scaling_events_total
from control_plane.state_store import StateStore


def utcnow():
    return datetime.now(timezone.utc)


class AutoScaler:
    def __init__(
        self,
        docker: DockerClient,
        events: EventBus,
        store: StateStore,
    ):
        self.docker = docker
        self.events = events
        self.store = store
        self.check_interval = 15
        self.running = False
        self.last_scale_up: Dict[str, datetime] = {}
        self.last_scale_down: Dict[str, datetime] = {}

    async def start(self):
        self.running = True
        while self.running:
            try:
                await self.check_all_services()
            except Exception as e:
                print(f"Scaler error: {type(e).__name__}: {e}")
            await asyncio.sleep(self.check_interval)

    async def stop(self):
        self.running = False

    async def check_all_services(self):
        services = await self.store.list_services()
        for service in services:
            if not service.spec.autoscaling.enabled:
                continue
            await self.check_service_scaling(service)

    async def check_service_scaling(self, service):
        name = service.spec.name
        actual = await self.store.get_actual_state(name)
        if not actual:
            return

        containers = [
            c for c in actual.get("containers", [])
            if c.get("status") == "running"
        ]
        if not containers:
            return

        total_cpu = sum(
            c.get("metrics", {}).get("cpu_percent", 0) for c in containers
        )
        total_memory = sum(
            c.get("metrics", {}).get("memory_percent", 0) for c in containers
        )
        avg_cpu = total_cpu / len(containers)
        avg_memory = total_memory / len(containers)

        current_replicas = len(containers)
        min_replicas = service.spec.autoscaling.min_replicas
        max_replicas = service.spec.autoscaling.max_replicas

        now = utcnow()

        # Use total_seconds() instead of .seconds — .seconds drops days and
        # was a subtle bug for long uptimes.
        def _elapsed(t: datetime) -> float:
            return (now - t).total_seconds() if t else float("inf")

        if (avg_cpu > service.spec.autoscaling.target_cpu_percent or
                avg_memory > service.spec.autoscaling.target_memory_percent):

            if current_replicas < max_replicas:
                if _elapsed(self.last_scale_up.get(name)) < \
                        service.spec.autoscaling.scale_up_cooldown:
                    return

                new_replicas = min(current_replicas + 1, max_replicas)
                await self.scale_service(
                    service, new_replicas, "up", avg_cpu, avg_memory
                )

        elif (avg_cpu < service.spec.autoscaling.target_cpu_percent * 0.5 and
              avg_memory < service.spec.autoscaling.target_memory_percent * 0.5):

            if current_replicas > min_replicas:
                if _elapsed(self.last_scale_down.get(name)) < \
                        service.spec.autoscaling.scale_down_cooldown:
                    return

                new_replicas = max(current_replicas - 1, min_replicas)
                await self.scale_service(
                    service, new_replicas, "down", avg_cpu, avg_memory
                )

    async def scale_service(
        self,
        service,
        new_replicas: int,
        direction: str,
        cpu: float,
        memory: float,
    ):
        name = service.spec.name

        print(f"Scaling {name} {direction}: "
              f"{service.spec.replicas} -> {new_replicas} "
              f"(CPU: {cpu:.1f}%, Memory: {memory:.1f}%)")

        service.spec.replicas = new_replicas
        await self.store.update_service(service)

        if direction == "up":
            self.last_scale_up[name] = utcnow()
        else:
            self.last_scale_down[name] = utcnow()

        scaling_events_total.labels(service=name, direction=direction).inc()

        await self.events.publish(Events.SCALE_REQUESTED, {
            "service": name,
            "replicas": new_replicas,
            "direction": direction,
            "cpu": cpu,
            "memory": memory,
            "source": "autoscaler"
        })