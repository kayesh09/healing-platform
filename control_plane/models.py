from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Literal
from datetime import datetime, timezone
from enum import Enum


def utcnow():
    return datetime.now(timezone.utc)


class ServiceStatus(str, Enum):
    PENDING = "pending"
    DEPLOYING = "deploying"
    RUNNING = "running"
    DEGRADED = "degraded"
    CIRCUIT_BROKEN = "circuit_broken"
    STOPPED = "stopped"


class HealthCheckConfig(BaseModel):
    endpoint: str = "/health"
    interval: int = 10
    timeout: int = 5
    retries: int = 3
    start_period: int = 10


class Resources(BaseModel):
    cpu_limit: float = Field(0.5, description="CPU cores limit")
    memory_limit: str = Field("512m", description="Memory limit")


class AutoscalingConfig(BaseModel):
    enabled: bool = True
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_percent: float = 70.0
    target_memory_percent: float = 80.0
    scale_up_cooldown: int = 60
    scale_down_cooldown: int = 300


class ServiceSpec(BaseModel):
    name: str = Field(..., description="Unique service name")
    image: str = Field(..., description="Docker image")
    port: int = Field(..., description="Container port")
    replicas: int = Field(1, description="Desired replica count")
    env: Dict[str, str] = Field(default_factory=dict)
    health_check: HealthCheckConfig = Field(default_factory=HealthCheckConfig)
    resources: Resources = Field(default_factory=Resources)
    autoscaling: AutoscalingConfig = Field(default_factory=AutoscalingConfig)
    command: Optional[List[str]] = None


class ServiceUpdate(BaseModel):
    image: Optional[str] = None
    replicas: Optional[int] = None
    env: Optional[Dict[str, str]] = None
    health_check: Optional[HealthCheckConfig] = None
    resources: Optional[Resources] = None
    autoscaling: Optional[AutoscalingConfig] = None


class ContainerState(BaseModel):
    id: str
    name: str
    status: str
    image: str
    restart_count: int = 0
    last_restart: Optional[datetime] = None
    health_status: Literal["healthy", "unhealthy", "starting", "unknown"] = "unknown"
    metrics: Dict = Field(default_factory=dict)


class ServiceState(BaseModel):
    spec: ServiceSpec
    status: ServiceStatus = ServiceStatus.PENDING
    containers: List[ContainerState] = Field(default_factory=list)
    restart_history: List[datetime] = Field(default_factory=list)
    circuit_broken_until: Optional[datetime] = None
    version: str = "v1"
    blue_green: Optional[Dict] = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class ScaleRequest(BaseModel):
    replicas: int


class DeployRequest(BaseModel):
    service_name: str
    image: str
    strategy: Literal["rolling", "blue-green"] = "rolling"


class RollbackRequest(BaseModel):
    service_name: str


class LogResponse(BaseModel):
    service: str
    container_id: str
    logs: str