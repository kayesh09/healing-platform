import asyncio
import hashlib
import os
from typing import Dict
from datetime import datetime, timezone

from shared.docker_client import DockerClient
from shared.events import EventBus, Events
from shared.metrics import scaling_events_total
from control_plane.state_store import StateStore
from control_plane.models import ServiceState, ServiceStatus, ContainerState
from .monitor import HealthMonitor
from .scaler import AutoScaler


def utcnow():
    return datetime.now(timezone.utc)


def _deterministic_host_port(service_name: str, replica_index: int) -> int:
    """Stable across processes (unlike hash() which is randomized by
    PYTHONHASHSEED). Same service+index -> same port every time."""
    h = hashlib.sha256(service_name.encode("utf-8")).hexdigest()
    base = int(h[:8], 16) % 1000   # 0..999
    return 8000 + base + replica_index


class Agent:
    def __init__(self):
        self.docker = DockerClient()
        self.store = StateStore(os.getenv("REDIS_URL", "redis://localhost:6379"))
        self.events = EventBus(os.getenv("REDIS_URL", "redis://localhost:6379"))
        self.monitor = HealthMonitor(self.docker, self.events)
        self.scaler = AutoScaler(self.docker, self.events, self.store)
        self.running = False
        self.reconcile_interval = 5

    async def start(self):
        print("Starting agent...")

        await self.store.connect()
        await self.events.connect()

        asyncio.create_task(
            self.events.subscribe("platform.events", self.handle_event)
        )

        asyncio.create_task(self.monitor.start())
        asyncio.create_task(self.scaler.start())
        asyncio.create_task(self.reconciliation_loop())

        self.running = True
        print("Agent started")

        while self.running:
            await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        await self.monitor.stop()
        await self.scaler.stop()
        await self.store.close()
        await self.events.close()

    async def handle_event(self, event: Dict):
        event_type = event.get("type")

        if event_type == Events.DEPLOY_REQUESTED:
            await self.deploy_service(event["service"])
        elif event_type == Events.SCALE_REQUESTED:
            await self.scale_service(event["service"], event["replicas"])
        elif event_type == "container.terminate":
            await self.docker.stop_container(event["container_id"])
        elif event_type == Events.BLUE_GREEN_DEPLOY:
            await self.blue_green_deploy(
                event["service"], event["image"]
            )

    async def reconciliation_loop(self):
        while self.running:
            try:
                await self.reconcile()
            except Exception as e:
                print(f"Reconciliation error: {type(e).__name__}: {e}")
            await asyncio.sleep(self.reconcile_interval)

    async def reconcile(self):
        services = await self.store.list_services()

        for service in services:
            if service.status in [
                ServiceStatus.CIRCUIT_BROKEN,
                ServiceStatus.STOPPED,
            ]:
                continue

            containers = await self.docker.list_containers(
                filters={"label": f"service.name={service.spec.name}"}
            )
            running = [c for c in containers if c.status == "running"]

            # ALWAYS write actual_state on every pass, even when scaling up.
            # Previously this was skipped on the scale-up pass, so the API
            # returned containers=[] right after a deploy until the next cycle.
            actual_state = {
                "containers": [
                    {
                        "id": c.id,
                        "name": c.name,
                        "status": c.status,
                        "image": c.image,
                        "restart_count": 0,
                        "metrics": await self.docker.get_container_stats(c.id),
                    }
                    for c in running
                ],
                "last_updated": utcnow().isoformat(),
            }
            await self.store.set_actual_state(service.spec.name, actual_state)

            desired = service.spec.replicas
            current = len(running)

            if current < desired:
                needed = desired - current
                print(f"Service {service.spec.name}: scaling up by {needed}")
                await self.scale_service(service.spec.name, desired)
                # Don't flip status here; wait for the next reconcile pass
                # to confirm the new container is actually running.
                if service.status != ServiceStatus.DEPLOYING:
                    service.status = ServiceStatus.DEPLOYING
                    await self.store.update_service(service)
                continue

            if current == desired:
                if service.status != ServiceStatus.RUNNING:
                    service.status = ServiceStatus.RUNNING
                    await self.store.update_service(service)
                    await self.events.publish(Events.DEPLOY_COMPLETED, {
                        "service": service.spec.name
                    })
            elif current > 0:
                if service.status != ServiceStatus.DEGRADED:
                    service.status = ServiceStatus.DEGRADED
                    await self.store.update_service(service)

    async def deploy_service(self, service_name: str):
        service = await self.store.get_service(service_name)
        if not service:
            return

        print(f"Deploying service {service_name}")

        existing = await self.docker.list_containers(
            filters={"label": f"service.name={service_name}"}
        )

        for i in range(service.spec.replicas):
            container_name = f"{service_name}-{i}-{int(utcnow().timestamp())}"
            host_port = _deterministic_host_port(service_name, i)

            labels = {
                "managed-by": "healing-platform",
                "service.name": service_name,
                "service.version": service.version,
                "health.port": str(service.spec.port),
                "health.endpoint": service.spec.health_check.endpoint,
            }

            try:
                container_id = await self.docker.run_container(
                    image=service.spec.image,
                    name=container_name,
                    ports={str(service.spec.port): host_port},
                    env=service.spec.env,
                    labels=labels,
                    memory_limit=service.spec.resources.memory_limit,
                    cpu_limit=service.spec.resources.cpu_limit,
                )
                print(f"Started container {container_name}: {container_id[:12]}")
            except Exception as e:
                print(f"Failed to start container: {type(e).__name__}: {e}")
                await self.events.publish(Events.DEPLOY_FAILED, {
                    "service": service_name,
                    "error": str(e)
                })

        for old in existing:
            if old.status == "running":
                await self.docker.stop_container(old.id)

        service.status = ServiceStatus.RUNNING
        await self.store.update_service(service)

    async def scale_service(self, service_name: str, replicas: int):
        service = await self.store.get_service(service_name)
        if not service:
            return

        current = await self.docker.list_containers(
            filters={"label": f"service.name={service_name}"}
        )
        running = [c for c in current if c.status == "running"]

        if len(running) < replicas:
            for i in range(len(running), replicas):
                container_name = (
                    f"{service_name}-{i}-{int(utcnow().timestamp())}"
                )
                host_port = _deterministic_host_port(service_name, i)

                labels = {
                    "managed-by": "healing-platform",
                    "service.name": service_name,
                    "service.version": service.version,
                    "health.port": str(service.spec.port),
                    "health.endpoint": service.spec.health_check.endpoint,
                }

                try:
                    await self.docker.run_container(
                        image=service.spec.image,
                        name=container_name,
                        ports={str(service.spec.port): host_port},
                        env=service.spec.env,
                        labels=labels,
                        memory_limit=service.spec.resources.memory_limit,
                        cpu_limit=service.spec.resources.cpu_limit,
                    )
                except Exception as e:
                    print(f"Scale up failed: {type(e).__name__}: {e}")

        elif len(running) > replicas:
            to_remove = running[replicas:]
            for container in to_remove:
                await self.docker.stop_container(container.id)

        await self.events.publish(Events.SCALE_COMPLETED, {
            "service": service_name,
            "replicas": replicas
        })

    async def blue_green_deploy(self, service_name: str, new_image: str):
        """Blue-green deploy with ACTUAL health validation of green before
        promoting. Previously `healthy` was always True because stats were
        fetched but never checked."""
        service = await self.store.get_service(service_name)
        if not service:
            return

        print(f"Starting blue-green deploy for {service_name}")

        green_containers = []
        green_host_ports = []
        for i in range(service.spec.replicas):
            container_name = (
                f"{service_name}-green-{i}-{int(utcnow().timestamp())}"
            )
            host_port = 9000 + _deterministic_host_port(service_name, i) - 8000
            green_host_ports.append(host_port)

            labels = {
                "managed-by": "healing-platform",
                "service.name": service_name,
                "deployment.color": "green",
                "health.port": str(service.spec.port),
                "health.endpoint": service.spec.health_check.endpoint,
            }

            try:
                cid = await self.docker.run_container(
                    image=new_image,
                    name=container_name,
                    ports={str(service.spec.port): host_port},
                    env=service.spec.env,
                    labels=labels,
                    memory_limit=service.spec.resources.memory_limit,
                    cpu_limit=service.spec.resources.cpu_limit,
                )
                green_containers.append(cid)
            except Exception as e:
                print(f"Green deployment failed: {type(e).__name__}: {e}")

        if len(green_containers) != service.spec.replicas:
            print(f"Green deployment incomplete for {service_name}; rolling back")
            for cid in green_containers:
                await self.docker.stop_container(cid)
            await self.events.publish(Events.DEPLOY_FAILED, {
                "service": service_name,
                "error": "green deployment incomplete"
            })
            return

        # Wait for containers to start up.
        start_period = service.spec.health_check.start_period
        await asyncio.sleep(start_period)

        # ACTUALLY health-check each green container via its host port.
        health_endpoint = service.spec.health_check.endpoint
        timeout = service.spec.health_check.timeout
        retries = service.spec.health_check.retries

        healthy = True
        for host_port in green_host_ports:
            url = f"http://127.0.0.1:{host_port}{health_endpoint}"
            ok = False
            for _ in range(retries):
                try:
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        resp = await client.get(url)
                    if resp.status_code == 200:
                        ok = True
                        break
                except Exception as e:
                    print(f"Green health check {url} failed: "
                          f"{type(e).__name__}: {e}")
                await asyncio.sleep(2)
            if not ok:
                healthy = False
                print(f"Green container at {url} failed health checks")
                break

        if healthy:
            # Find and stop blue (current) containers.
            blue_containers = await self.docker.list_containers(
                filters={"label": f"service.name={service_name}"}
            )
            blue_containers = [
                c for c in blue_containers
                if c.labels.get("deployment.color") != "green"
            ]

            for container in blue_containers:
                await self.docker.stop_container(container.id)

            # Promote green -> blue by relabeling (or just track in state).
            service.spec.image = new_image
            service.blue_green = {
                "blue": {"image": new_image, "active": True},
                "green": {"image": new_image, "active": False},
                "last_switch": utcnow().isoformat(),
            }
            await self.store.update_service(service)

            await self.events.publish(Events.BLUE_GREEN_SWITCH, {
                "service": service_name,
                "image": new_image
            })
            print(f"Blue-green deploy completed for {service_name}")
        else:
            # Rollback: stop green, keep blue.
            for cid in green_containers:
                await self.docker.stop_container(cid)
            print(f"Blue-green deploy failed for {service_name}; "
                  f"rolled back to blue")
            await self.events.publish(Events.DEPLOY_FAILED, {
                "service": service_name,
                "error": "green failed health checks; rolled back"
            })


async def main():
    agent = Agent()
    try:
        await agent.start()
    except KeyboardInterrupt:
        await agent.stop()


if __name__ == "__main__":
    asyncio.run(main())