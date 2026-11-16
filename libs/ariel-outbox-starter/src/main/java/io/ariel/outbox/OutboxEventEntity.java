package io.ariel.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Transactional Outbox table entity written in the same ACID database transaction
 * as domain mutations to guarantee zero dual-write bugs.
 */
@Entity
@Table(
        name = "outbox_events",
        indexes = {
                @Index(name = "idx_outbox_unpublished", columnList = "published, created_at")
        }
)
public class OutboxEventEntity {

    @Id
    @Column(name = "event_id", nullable = false, length = 64)
    private String eventId;

    @Column(name = "aggregate_type", nullable = false, length = 64)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Lob
    @Column(name = "payload_json", nullable = false)
    private String payloadJson;

    @Column(name = "correlation_id", nullable = false, length = 64)
    private String correlationId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "published", nullable = false)
    private boolean published;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEventEntity() {
    }

    public OutboxEventEntity(
            String eventId,
            String aggregateType,
            String aggregateId,
            String topic,
            String payloadJson,
            String correlationId,
            String tenantId,
            Instant createdAt
    ) {
        this.eventId = eventId;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.payloadJson = payloadJson;
        this.correlationId = correlationId;
        this.tenantId = tenantId;
        this.published = false;
        this.createdAt = createdAt;
    }

    public void markPublished(Instant when) {
        this.published = true;
        this.publishedAt = when;
    }

    public String getEventId() {
        return eventId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public boolean isPublished() {
        return published;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
