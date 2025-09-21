package io.symplify

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.eclipse.microprofile.health.Readiness

@ApplicationScoped
@Liveness
class LivenessCheck : HealthCheck {
    override fun call(): HealthCheckResponse {
        return HealthCheckResponse.up("kafka-mirror-liveness")
    }
}

@ApplicationScoped
@Readiness
class ReadinessCheck : HealthCheck {
    override fun call(): HealthCheckResponse {
        return HealthCheckResponse.up("kafka-mirror-readiness")
    }
}