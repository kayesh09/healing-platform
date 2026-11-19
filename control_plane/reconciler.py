import asyncio
from typing import Dict
from .state_store import StateStore
from .models import ServiceState, ServiceStatus
from shared.events import EventBus, Events
from shared.metrics import services_total, service_replicas, service_running_replicas
from datetime import datetime, timezone


def utcnow():
    return datetime.now(timezone.utc)


class Reconciler:
    def __init__(self, store: StateStore, events: EventBus):
        self.store = store
        self.events = events
        self.running = False
        self.reconcile_interval = 10

    async def start(self):
        self.running = True
        while self.running:
            try:
                await self.reconcile_all()
            except Exception as e:
                print(f"Reconciliation error: {e}")
            await asyncio.sleep(self.reconcile_interval)

    async def stop(self):
        self.running = False

    async def reconcile_all(self):
        services = await self.store.list_services()
        services_total.set(len(services))

        for service in services:
            await self.reconcile_service(service)

    async def reconcile_service(self, service: ServiceState):
        actual = await self.store.get_actual_state(service.spec.name)

        service_replicas.labels(service=service.spec.name).set(service.spec.replicas)
        if actual:
            running = len([c for c in actual.get("containers", []) if c["status"] == "running"])
            service_running_replicas.labels(service=service.spec.name).set(running)

        if service.status == ServiceStatus.CIRCUIT_BROKEN:
            if service.circuit_broken_until and utcnow() > service.circuit_broken_until:
                service.status = ServiceStatus.RUNNING
                service.circuit_broken_until = None
                await self.store.update_service(service)
                await self.events.publish(Events.CIRCUIT_RESET, {"service": service.spec.name})
            return

        if service.status == ServiceStatus.PENDING:
            await self.events.publish(Events.DEPLOY_REQUESTED, {
                "service": service.spec.name,
                "spec": service.spec.model_dump()
            })
            service.status = ServiceStatus.DEPLOYING
            await self.store.update_service(service)

        if actual:
            running_containers = [c for c in actual.get("containers", []) if c["status"] == "running"]
            desired = service.spec.replicas
            current = len(running_containers)

            if current < desired and service.status == ServiceStatus.RUNNING:
                await self.events.publish(Events.SCALE_REQUESTED, {
                    "service": service.spec.name,
                    "replicas": desired,
                    "reason": "reconciliation"
                })
            elif current > desired:
                excess = running_containers[desired:]
                for container in excess:
                    await self.events.publish("container.terminate", {
                        "service": service.spec.name,
                        "container_id": container["id"]
                    })