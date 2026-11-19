import os
import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import (
    FastAPI,
    HTTPException,
    BackgroundTasks,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from pydantic import BaseModel

# Debug prints
print(f"REDIS_URL env: {os.getenv('REDIS_URL', 'NOT SET')}")
print("Starting control plane...")

from .models import (
    ServiceSpec,
    ServiceUpdate,
    ServiceState,
    ScaleRequest,
    DeployRequest,
    RollbackRequest,
    LogResponse,
    ServiceStatus,
    ContainerState,
)
from .state_store import StateStore
from .reconciler import Reconciler
from shared.events import EventBus, Events
from shared.metrics import (
    metrics_endpoint,
    deployments_total,
    scaling_events_total,
    http_requests_total,
    http_request_duration,
)
from shared.docker_client import DockerClient

# Global state
store: Optional[StateStore] = None
events: Optional[EventBus] = None
reconciler: Optional[Reconciler] = None
docker: Optional[DockerClient] = None


async def _connect_with_retry(connect_fn, name: str, max_retries: int = 5):
    """Retry a connect() coroutine with exponential backoff so transient
    outages (e.g. Redis still starting up) don't hard-crash the control plane."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            await connect_fn()
            print(f">>> {name} connected (attempt {attempt})")
            return
        except Exception as e:
            last_err = e
            wait = min(2 ** (attempt - 1), 10)
            print(f">>> {name} connect attempt {attempt}/{max_retries} "
                  f"failed: {type(e).__name__}: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)
    raise RuntimeError(f"{name} failed to connect after {max_retries} "
                       f"attempts: {last_err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store, events, reconciler, docker

    print(">>> Lifespan starting...")

    # Initialize Redis connection (with retry)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    print(f">>> Connecting to Redis at {redis_url}...")

    store = StateStore(redis_url)
    await _connect_with_retry(store.connect, "Redis")

    # Initialize event bus
    print(">>> Connecting to event bus...")
    events = EventBus(redis_url)
    await _connect_with_retry(events.connect, "Event bus")

    # Initialize Docker client
    print(">>> Initializing Docker client...")
    docker = DockerClient()
    print(">>> Docker client ready!")

    # Start reconciler
    print(">>> Starting reconciler...")
    reconciler = Reconciler(store, events)
    reconcile_task = asyncio.create_task(reconciler.start())
    print(">>> Reconciler started!")

    # Subscribe to events for logging
    asyncio.create_task(events.subscribe("platform.events", log_event))
    print(">>> Event subscriber started!")

    print(">>> All components initialized successfully!")

    yield

    # Cleanup
    print(">>> Shutting down...")
    reconcile_task.cancel()
    try:
        await asyncio.gather(store.close(), events.close(), return_exceptions=True)
    except Exception as e:
        print(f">>> Cleanup error: {type(e).__name__}: {e}")
    print(">>> Shutdown complete!")


async def log_event(event):
    """Log platform events"""
    event_type = event.get("type", "unknown")
    service = event.get("service", "N/A")
    print(f"[EVENT] {event_type}: {service}")


# Create FastAPI app
app = FastAPI(
    title="Self-Healing Platform Control Plane",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Metrics middleware
@app.middleware("http")
async def metrics_middleware(request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start

    http_requests_total.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
    ).inc()
    http_request_duration.labels(endpoint=request.url.path).observe(duration)

    return response


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    """Redirect root to the Swagger UI for convenience."""
    return RedirectResponse(url="/docs")


@app.post("/api/services", response_model=ServiceState)
async def register_service(spec: ServiceSpec):
    """Register a new service"""
    if not store:
        raise HTTPException(500, "State store not initialized")

    existing = await store.get_service(spec.name)
    if existing:
        raise HTTPException(400, f"Service {spec.name} already exists")

    state = await store.register_service(spec)
    await events.publish(Events.SERVICE_REGISTERED, {
        "service": spec.name,
        "spec": spec.model_dump() if hasattr(spec, "model_dump") else spec.dict(),
    })

    return state


@app.get("/api/services", response_model=List[ServiceState])
async def list_services():
    """List all services"""
    if not store:
        raise HTTPException(500, "State store not initialized")
    return await store.list_services()


@app.get("/api/services/{name}", response_model=ServiceState)
async def get_service(name: str):
    """Get service details"""
    if not store:
        raise HTTPException(500, "State store not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    actual = await store.get_actual_state(name)
    if actual:
        # Coerce dicts from actual_state into ContainerState so Pydantic
        # doesn't warn about type mismatch on serialization.
        raw_containers = actual.get("containers", [])
        service.containers = [
            ContainerState(**c) if isinstance(c, dict) else c
            for c in raw_containers
        ]

    return service


@app.put("/api/services/{name}", response_model=ServiceState)
async def update_service(name: str, update: ServiceUpdate):
    """Update service configuration"""
    if not store:
        raise HTTPException(500, "State store not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    if update.image:
        service.spec.image = update.image
    if update.replicas is not None:
        service.spec.replicas = update.replicas
    if update.env:
        service.spec.env.update(update.env)
    if update.health_check:
        service.spec.health_check = update.health_check
    if update.resources:
        service.spec.resources = update.resources
    if update.autoscaling:
        service.spec.autoscaling = update.autoscaling

    await store.update_service(service)

    update_data = (
        update.model_dump(exclude_none=True)
        if hasattr(update, "model_dump")
        else update.dict(exclude_none=True)
    )
    await events.publish(Events.SERVICE_UPDATED, {
        "service": name,
        "update": update_data,
    })

    return service


@app.delete("/api/services/{name}")
async def delete_service(name: str, background: BackgroundTasks):
    """Delete a service"""
    if not store or not docker:
        raise HTTPException(500, "Platform not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    actual = await store.get_actual_state(name)
    if actual:
        for container in actual.get("containers", []):
            await docker.stop_container(container["id"])

    await store.delete_service(name)
    await events.publish(Events.SERVICE_DELETED, {"service": name})

    return {"status": "deleted", "service": name}


@app.post("/api/services/{name}/deploy")
async def deploy_service(name: str, request: DeployRequest):
    """Trigger deployment"""
    if not store or not events:
        raise HTTPException(500, "Platform not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    if request.service_name != name:
        raise HTTPException(400, "Service name mismatch")

    service.spec.image = request.image
    service.status = ServiceStatus.DEPLOYING

    if request.strategy == "blue-green":
        service.blue_green = {
            "blue": {"image": service.spec.image, "active": True},
            "green": {"image": request.image, "active": False},
            "switch_ready": False,
        }
        await events.publish(Events.BLUE_GREEN_DEPLOY, {
            "service": name,
            "image": request.image,
        })

    await store.update_service(service)
    await events.publish(Events.DEPLOY_REQUESTED, {
        "service": name,
        "image": request.image,
        "strategy": request.strategy,
    })

    deployments_total.labels(service=name, status="requested").inc()

    return {"status": "deploying", "service": name, "strategy": request.strategy}


@app.post("/api/services/{name}/scale", response_model=ServiceState)
async def scale_service(name: str, request: ScaleRequest):
    """Manually scale service"""
    if not store or not events:
        raise HTTPException(500, "Platform not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    # Validate against autoscaling limits
    if service.spec.autoscaling.enabled:
        if request.replicas < service.spec.autoscaling.min_replicas:
            raise HTTPException(
                400,
                f"Replicas below minimum ({service.spec.autoscaling.min_replicas})",
            )
        if request.replicas > service.spec.autoscaling.max_replicas:
            raise HTTPException(
                400,
                f"Replicas above maximum ({service.spec.autoscaling.max_replicas})",
            )

    service.spec.replicas = request.replicas
    await store.update_service(service)

    await events.publish(Events.SCALE_REQUESTED, {
        "service": name,
        "replicas": request.replicas,
        "source": "manual",
    })

    scaling_events_total.labels(service=name, direction="manual").inc()

    return service


@app.post("/api/services/{name}/restart")
async def restart_service(name: str, background: BackgroundTasks):
    """Force restart all containers"""
    if not store or not docker or not events:
        raise HTTPException(500, "Platform not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    actual = await store.get_actual_state(name)
    if actual:
        for container in actual.get("containers", []):
            await docker.kill_container(container["id"])

    await events.publish(Events.DEPLOY_REQUESTED, {
        "service": name,
        "reason": "manual_restart",
    })

    return {"status": "restarting", "service": name}


@app.post("/api/services/{name}/rollback")
async def rollback_service(name: str, request: RollbackRequest):
    """Rollback to previous version"""
    if not store or not events:
        raise HTTPException(500, "Platform not initialized")

    service = await store.get_service(name)
    if not service:
        raise HTTPException(404, f"Service {name} not found")

    if not service.blue_green:
        raise HTTPException(400, "No blue-green deployment to rollback")

    active = service.blue_green.get("active", "blue")
    if active == "green":
        service.blue_green["blue"]["active"] = True
        service.blue_green["green"]["active"] = False
        service.spec.image = service.blue_green["blue"]["image"]

        await store.update_service(service)
        await events.publish(Events.ROLLBACK, {"service": name})

        return {"status": "rolled_back", "service": name}

    return {"status": "already_on_blue", "service": name}


@app.get("/api/services/{name}/logs", response_model=LogResponse)
async def get_logs(name: str, container_id: str, tail: int = 100):
    """Get container logs"""
    if not docker:
        raise HTTPException(500, "Docker not initialized")

    logs = await docker.get_container_logs(container_id, tail)
    return LogResponse(service=name, container_id=container_id, logs=logs)


@app.get("/api/services/{name}/metrics")
async def get_metrics(name: str):
    """Get service metrics"""
    if not store:
        raise HTTPException(500, "State store not initialized")

    actual = await store.get_actual_state(name)
    if not actual:
        raise HTTPException(404, "No metrics available")

    return {
        "service": name,
        "containers": actual.get("containers", []),
        "aggregated": actual.get("aggregated_metrics", {}),
    }


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint"""
    return metrics_endpoint()


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "component": "control-plane",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Web dashboard"""
    # Resolve dashboard.html relative to the project root (one level up from
    # control_plane/), regardless of the current working directory.
    dashboard_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "web",
        "dashboard.html",
    )
    try:
        with open(dashboard_path, encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Dashboard not found</h1>"
                    "<p>Make sure web/dashboard.html exists</p>",
            status_code=404,
        )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time updates via WebSocket"""
    await websocket.accept()
    try:
        while True:
            if store:
                services = await store.list_services()
                await websocket.send_json({
                    "type": "services_update",
                    "services": [
                        s.model_dump() if hasattr(s, "model_dump") else s.dict()
                        for s in services
                    ],
                })
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        print("WebSocket disconnected")
    except Exception as e:
        print(f"WebSocket error: {type(e).__name__}: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)