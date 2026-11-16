package io.ariel.common.context;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable request-scoped context propagated across Java 25 Virtual Threads via {@link ScopedValue}.
 */
public record RequestContext(
        String correlationId,
        String tenantId,
        String idempotencyKey,
        Instant receivedAt
) {
    public RequestContext {
        correlationId = (correlationId == null || correlationId.isBlank())
                ? UUID.randomUUID().toString()
                : correlationId.trim();
        tenantId = (tenantId == null || tenantId.isBlank())
                ? "default-tenant"
                : tenantId.trim();
        idempotencyKey = (idempotencyKey == null || idempotencyKey.isBlank())
                ? correlationId
                : idempotencyKey.trim();
        receivedAt = Objects.requireNonNullElseGet(receivedAt, Instant::now);
    }

    public static RequestContext systemDefault() {
        String id = UUID.randomUUID().toString();
        return new RequestContext(id, "default-tenant", id, Instant.now());
    }
}
