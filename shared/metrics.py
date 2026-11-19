from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response


# Platform metrics
services_total = Gauge('platform_services_total', 'Total number of services')
service_replicas = Gauge('platform_service_replicas', 'Desired replicas per service', ['service'])
service_running_replicas = Gauge('platform_service_running_replicas', 'Running replicas per service', ['service'])
container_restarts_total = Counter('platform_container_restarts_total', 'Total container restarts', ['service'])
deployments_total = Counter('platform_deployments_total', 'Total deployments', ['service', 'status'])
health_checks_total = Counter('platform_health_checks_total', 'Health checks performed', ['service', 'status'])
scaling_events_total = Counter('platform_scaling_events_total', 'Scaling events', ['service', 'direction'])
circuit_breaker_trips = Counter('platform_circuit_breaker_trips_total', 'Circuit breaker trips', ['service'])

# HTTP metrics
http_requests_total = Counter('http_requests_total', 'HTTP requests', ['method', 'endpoint', 'status'])
http_request_duration = Histogram('http_request_duration_seconds', 'HTTP request duration', ['endpoint'])


def metrics_endpoint():
    """Prometheus metrics endpoint handler"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)