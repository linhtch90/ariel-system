package io.ariel.outbox;

import io.ariel.common.context.RequestContextHolder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

/**
 * Asynchronous Outbox Relay worker that polls unpublished outbox records in micro-batches
 * and publishes them to Apache Kafka with correlation and tenant headers preserved.
 */
public class OutboxRelayWorker {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayWorker.class);

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayWorker(
            OutboxEventRepository outboxRepository,
            ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
    }

    @Scheduled(
            initialDelayString = "${ariel.outbox.initial-delay-ms:2000}",
            fixedDelayString = "${ariel.outbox.poll-interval-ms:500}"
    )
    @Transactional
    public int relayPendingBatch() {
        List<OutboxEventEntity> batch = outboxRepository.findUnpublishedBatch(PageRequest.of(0, 100));
        if (batch.isEmpty()) {
            return 0;
        }

        int relayed = 0;
        Instant now = Instant.now();
        for (OutboxEventEntity event : batch) {
            try {
                if (kafkaTemplate != null) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            event.getTopic(),
                            event.getAggregateId(),
                            event.getPayloadJson()
                    );
                    record.headers().add(
                            RequestContextHolder.HEADER_CORRELATION_ID,
                            event.getCorrelationId().getBytes(StandardCharsets.UTF_8)
                    );
                    record.headers().add(
                            RequestContextHolder.HEADER_TENANT_ID,
                            event.getTenantId().getBytes(StandardCharsets.UTF_8)
                    );
                    kafkaTemplate.send(record);
                }
                event.markPublished(now);
                relayed++;
            } catch (Exception e) {
                log.warn("Transient failure relaying outbox eventId={} to topic={}: {}",
                        event.getEventId(), event.getTopic(), e.getMessage());
            }
        }
        return relayed;
    }
}
