package io.ariel.outbox;

import io.ariel.common.context.RequestContextHolder;
import java.time.Instant;
import java.util.UUID;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Persists an OutboxEventEntity inside the caller's active database transaction
 * (MANDATORY propagation ensures it cannot be called outside an ACID transaction).
 */
public class TransactionalOutboxPublisher {

    private final OutboxEventRepository outboxRepository;
    private final JsonMapper jsonMapper;

    public TransactionalOutboxPublisher(OutboxEventRepository outboxRepository, JsonMapper jsonMapper) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEventEntity stageEvent(String aggregateType, String aggregateId, String topic, Object payload) {
        var ctx = RequestContextHolder.current();
        String json = jsonMapper.writeValueAsString(payload);
        OutboxEventEntity entity = new OutboxEventEntity(
                UUID.randomUUID().toString(),
                aggregateType,
                aggregateId,
                topic,
                json,
                ctx.correlationId(),
                ctx.tenantId(),
                Instant.now()
        );
        return outboxRepository.save(entity);
    }
}
