import asyncio
import httpx
from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta

from shared.docker_client import DockerClient
from shared.events import EventBus, Events
from shared.metrics import (
    health_checks_total,
    container_restarts_total,
    circuit_breaker_trips,
)


def utcnow():
    return datetime.now(timezone.utc)


class HealthMonitor:
    def __init__(self, docker: DockerClient, events: EventBus):
        self.docker = docker
        self.events = events
        self.check_interval = 5
        self.failure_threshold = 3
        self.running = False

        # container_id -> consecutive failure count.
        # A value of -1 means "already handled as crashed, don't re-fire".
        self.container_failures: Dict[str, int] = {}

        # container_id -> last status we saw (for running->stopped transition detection)
        self.last_status: Dict[str, str] = {}

        # service_name -> list of restart timestamps in the rolling window
        self.restart_history: Dict[str, List[datetime]] = {}
        self.circuit_breaker_threshold = 5
        self.circuit_breaker_window = timedelta(minutes=10)

    async def start(self):
        self.running = True
        while self.running:
            try:
                await self.check_all_containers()
            except Exception as e:
                print(f"Monitor error: {type(e).__name__}: {e}")
            await asyncio.sleep(self.check_interval)

    async def stop(self):
        self.running = False

    async def check_all_containers(self):
        """Only inspect RUNNING containers here.

        Exited containers are reconciled back to desired count by the agent's
        reconcile loop. Inspecting exited ones here previously caused a death
        spiral where every poll re-handled the same dead container forever.
        """
        containers = await self.docker.list_containers(
            filters={"label": "managed-by=healing-platform"}
        )
        seen_ids = {c.id for c in containers}

        # Detect running -> stopped transitions for containers we saw last cycle.
        for cid, prev in list(self.last_status.items()):
            if prev == "running" and cid not in seen_ids:
                # Container disappeared between polls -> crashed/exited.
                await self.events.publish(Events.CONTAINER_CRASHED, {
                    "service": "unknown",
                    "container_id": cid,
                    "reason": "container disappeared",
                })
                # Forget tracking so we don't keep firing for the same dead id.
                self.container_failures.pop(cid, None)
                self.last_status.pop(cid, None)

        for container in containers:
            self.last_status[container.id] = container.status

            if container.status != "running":
                # Exited containers are reconciled elsewhere; skip here.
                # Only act on a fresh transition (running -> not running) ONCE.
                if self.container_failures.get(container.id, 0) == 0:
                    await self.handle_crashed_container(container)
                    self.container_failures[container.id] = -1
                continue

            # Reset tracking for a fresh running container.
            self.container_failures.pop(container.id, None)
            await self.check_http_health(container)

    async def handle_crashed_container(self, container, reason: str = ""):
        service_name = container.labels.get("service.name")
        if not service_name:
            return

        print(f"Container {container.name} crashed ({reason}); scheduling restart...")

        if service_name not in self.restart_history:
            self.restart_history[service_name] = []

        now = utcnow()
        self.restart_history[service_name].append(now)

        # Trim to rolling window
        cutoff = now - self.circuit_breaker_window
        self.restart_history[service_name] = [
            t for t in self.restart_history[service_name] if t > cutoff
        ]

        restart_count = len(self.restart_history[service_name])
        container_restarts_total.labels(service=service_name).inc()

        await self.events.publish(Events.CONTAINER_CRASHED, {
            "service": service_name,
            "container_id": container.id,
            "restart_count": restart_count,
            "reason": reason,
        })

        # Circuit breaker
        if restart_count >= self.circuit_breaker_threshold:
            print(f"Service {service_name}: circuit breaker OPEN "
                  f"({restart_count} restarts in {self.circuit_breaker_window}); "
                  f"not restarting")
            await self.events.publish(Events.CIRCUIT_BROKEN, {
                "service": service_name,
                "restart_count": restart_count
            })
            circuit_breaker_trips.labels(service=service_name).inc()
            return

        # Schedule the actual kill+restart as a background task so we don't
        # block the monitor loop. Backoff applied per service.
        backoff = min(2 ** (restart_count - 1), 60)
        asyncio.create_task(
            self._do_restart(container, service_name, reason, backoff)
        )

    async def _do_restart(self, container, service_name: str, reason: str, backoff: int):
        try:
            await asyncio.sleep(backoff)
            try:
                await self.docker.kill_container(container.id)
            except Exception as e:
                # Container may already be stopped; that's fine.
                print(f"Container {container.name}: kill returned "
                      f"{type(e).__name__}: {e}")

            await self.events.publish(Events.CONTAINER_RESTARTED, {
                "service": service_name,
                "container_id": container.id,
                "backoff": backoff,
                "reason": reason
            })

            # Forget tracking for this container id; the reconciler will start
            # a fresh container with a new id.
            self.container_failures.pop(container.id, None)
            self.last_status.pop(container.id, None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Restart of {container.name} failed: "
                  f"{type(e).__name__}: {e}")

    async def check_http_health(self, container):
        service_name = container.labels.get("service.name")
        health_port = container.labels.get("health.port")
        health_endpoint = container.labels.get("health.endpoint", "/health")

        if not health_port:
            return

        inspect = await self.docker.inspect_container(container.id)
        if not inspect:
            return

        network_settings = inspect.get("NetworkSettings", {})
        if not network_settings:
            return

        # Docker Desktop on Windows/Mac runs containers inside a VM; the
        # container's bridge IP is unreachable from the host. Prefer the
        # published host port binding (e.g. 127.0.0.1:8784). Fall back to
        # container IP only when there's no binding (Linux).
        url = self._pick_health_url(
            network_settings, health_port, health_endpoint
        )
        if not url:
            return

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url)

            if response.status_code == 200:
                health_checks_total.labels(
                    service=service_name, status="healthy"
                ).inc()
                self.container_failures[container.id] = 0
                await self.events.publish(Events.HEALTH_CHECK_PASSED, {
                    "service": service_name,
                    "container_id": container.id
                })
            else:
                await self.handle_unhealthy(
                    container, f"HTTP {response.status_code} from {url}"
                )
        except Exception as e:
            await self.handle_unhealthy(
                container, f"{type(e).__name__}: {e} (url={url})"
            )

    @staticmethod
    def _pick_health_url(network_settings: dict,
                         health_port: str,
                         health_endpoint: str) -> Optional[str]:
        ports = network_settings.get("Ports") or {}
        for _cport, bindings in ports.items():
            # bindings looks like [{"HostIp": "0.0.0.0", "HostPort": "8784"}]
            if bindings:
                host_binding = bindings[0]
                host_ip = host_binding.get("HostIp") or "127.0.0.1"
                if host_ip in ("0.0.0.0", "::"):
                    host_ip = "127.0.0.1"
                return (
                    f"http://{host_ip}:"
                    f"{host_binding['HostPort']}"
                    f"{health_endpoint}"
                )
        # No host binding -> fall back to container IP (Linux only).
        networks = network_settings.get("Networks") or {}
        if networks:
            ip = list(networks.values())[0].get("IPAddress")
            if ip:
                return f"http://{ip}:{health_port}{health_endpoint}"
        return None

    async def handle_unhealthy(self, container, reason: str):
        service_name = container.labels.get("service.name")
        if not service_name:
            return

        failures = self.container_failures.get(container.id, 0) + 1
        self.container_failures[container.id] = failures

        health_checks_total.labels(
            service=service_name, status="unhealthy"
        ).inc()

        print(f"Container {container.name} unhealthy "
              f"({failures}/{self.failure_threshold}): {reason}")

        if failures >= self.failure_threshold:
            await self.handle_crashed_container(container, reason=reason)
            self.container_failures[container.id] = -1
        else:
            await self.events.publish(Events.HEALTH_CHECK_FAILED, {
                "service": service_name,
                "container_id": container.id,
                "failures": failures,
                "reason": reason
            })