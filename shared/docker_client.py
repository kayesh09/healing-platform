import docker
import asyncio
from typing import Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime, timezone


def utcnow():
    return datetime.now(timezone.utc)


@dataclass
class ContainerInfo:
    id: str
    name: str
    status: str
    image: str
    labels: Dict[str, str]
    created: float
    ports: Dict


class DockerClient:
    def __init__(self):
        self.client = docker.from_env()
        self._lock = asyncio.Lock()

    async def run_container(
        self,
        image: str,
        name: str,
        ports: Dict[str, int],
        env: Dict[str, str],
        labels: Dict[str, str],
        memory_limit: str = "512m",
        cpu_limit: float = 0.5,
        network: str = "bridge"
    ) -> str:
        async with self._lock:
            try:
                try:
                    self.client.images.get(image)
                except docker.errors.ImageNotFound:
                    self.client.images.pull(image)

                port_bindings = {}
                for container_port, host_port in ports.items():
                    port_bindings[f"{container_port}/tcp"] = host_port

                container = self.client.containers.run(
                    image,
                    name=name,
                    detach=True,
                    ports=port_bindings,
                    environment=env,
                    labels=labels,
                    mem_limit=memory_limit,
                    cpu_period=100000,
                    cpu_quota=int(cpu_limit * 100000),
                    network=network,
                    restart_policy={"Name": "no"}
                )
                return container.id
            except Exception as e:
                raise RuntimeError(f"Failed to run container {name}: {e}")

    async def stop_container(self, container_id: str, timeout: int = 10):
        async with self._lock:
            try:
                container = self.client.containers.get(container_id)
                container.stop(timeout=timeout)
                container.remove()
            except docker.errors.NotFound:
                pass
            except Exception as e:
                print(f"Error stopping container {container_id}: {e}")

    async def kill_container(self, container_id: str):
        async with self._lock:
            try:
                container = self.client.containers.get(container_id)
                container.kill()
                container.remove()
            except docker.errors.NotFound:
                pass
            except Exception as e:
                print(f"Error killing container {container_id}: {e}")

    async def get_container(self, container_id: str) -> Optional[ContainerInfo]:
        try:
            container = self.client.containers.get(container_id)
            return ContainerInfo(
                id=container.id,
                name=container.name,
                status=container.status,
                image=container.image.tags[0] if container.image.tags else "unknown",
                labels=container.labels,
                created=container.attrs["Created"],
                ports=container.attrs.get("NetworkSettings", {}).get("Ports", {})
            )
        except docker.errors.NotFound:
            return None

    async def list_containers(self, filters: Dict[str, str] = None) -> List[ContainerInfo]:
        containers = self.client.containers.list(all=True, filters=filters or {})
        result = []
        for c in containers:
            result.append(ContainerInfo(
                id=c.id,
                name=c.name,
                status=c.status,
                image=c.image.tags[0] if c.image.tags else "unknown",
                labels=c.labels,
                created=c.attrs["Created"],
                ports=c.attrs.get("NetworkSettings", {}).get("Ports", {})
            ))
        return result

    async def get_container_stats(self, container_id: str) -> Dict:
        try:
            container = self.client.containers.get(container_id)
            stats = container.stats(stream=False)

            cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                       stats["precpu_stats"]["cpu_usage"]["total_usage"]
            system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                          stats["precpu_stats"]["system_cpu_usage"]
            cpu_percent = (cpu_delta / system_delta) * 100.0 if system_delta > 0 else 0.0

            memory_usage = stats["memory_stats"].get("usage", 0)
            memory_limit = stats["memory_stats"].get("limit", 1)
            memory_percent = (memory_usage / memory_limit) * 100.0

            return {
                "cpu_percent": round(cpu_percent, 2),
                "memory_usage": memory_usage,
                "memory_limit": memory_limit,
                "memory_percent": round(memory_percent, 2)
            }
        except Exception:
            return {"cpu_percent": 0.0, "memory_usage": 0, "memory_limit": 1, "memory_percent": 0.0}

    async def get_container_logs(self, container_id: str, tail: int = 100) -> str:
        try:
            container = self.client.containers.get(container_id)
            logs = container.logs(tail=tail, timestamps=True)
            return logs.decode('utf-8', errors='replace')
        except Exception as e:
            return f"Error fetching logs: {e}"

    async def inspect_container(self, container_id: str) -> Optional[Dict]:
        try:
            container = self.client.containers.get(container_id)
            return container.attrs
        except docker.errors.NotFound:
            return None