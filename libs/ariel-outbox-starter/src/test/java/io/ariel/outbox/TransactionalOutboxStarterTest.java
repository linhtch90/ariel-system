package io.ariel.outbox;

import io.ariel.common.context.RequestContext;
import io.ariel.common.context.RequestContextHolder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransactionalOutboxStarterTest {

    @Test
    void stagesOutboxEventWithScopedValueMetadataAndRelaysToKafkaWithHeaders() throws Exception {
        OutboxEventRepository repository = Mockito.mock(OutboxEventRepository.class);
        when(repository.save(any(OutboxEventEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        var publisher = new TransactionalOutboxPublisher(repository, JsonMapper.builder().build());
        var ctx = new RequestContext("corr-outbox-1", "tenant-bank", "idem-outbox-1", Instant.now());

        OutboxEventEntity staged = RequestContextHolder.callWith(ctx, () ->
                publisher.stageEvent("LedgerTransaction", "tx-555", "ariel.transactions.validated", Map.of("amount", 100))
        );

        assertThat(staged.getAggregateType()).isEqualTo("LedgerTransaction");
        assertThat(staged.getAggregateId()).isEqualTo("tx-555");
        assertThat(staged.getCorrelationId()).isEqualTo("corr-outbox-1");
        assertThat(staged.getTenantId()).isEqualTo("tenant-bank");
        assertThat(staged.isPublished()).isFalse();

        // Now test OutboxRelayWorker publishing to KafkaTemplate with headers
        when(repository.findUnpublishedBatch(any(Pageable.class))).thenReturn(List.of(staged));
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        @SuppressWarnings("unchecked")
        org.springframework.beans.factory.ObjectProvider<KafkaTemplate<String, String>> provider =
                Mockito.mock(org.springframework.beans.factory.ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(kafkaTemplate);

        var relayWorker = new OutboxRelayWorker(repository, provider);

        int relayedCount = relayWorker.relayPendingBatch();
        assertThat(relayedCount).isEqualTo(1);
        assertThat(staged.isPublished()).isTrue();
        assertThat(staged.getPublishedAt()).isNotNull();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, String> sent = recordCaptor.getValue();
        assertThat(sent.topic()).isEqualTo("ariel.transactions.validated");
        assertThat(sent.key()).isEqualTo("tx-555");
        assertThat(new String(sent.headers().lastHeader(RequestContextHolder.HEADER_CORRELATION_ID).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-outbox-1");
        assertThat(new String(sent.headers().lastHeader(RequestContextHolder.HEADER_TENANT_ID).value(), StandardCharsets.UTF_8))
                .isEqualTo("tenant-bank");
    }

    @Test
    void outboxRelayWorkerRetainsUnpublishedStateWhenKafkaBrokerThrowsException() {
        OutboxEventRepository repository = Mockito.mock(OutboxEventRepository.class);
        var event = new OutboxEventEntity(
                "ev-1", "Ledger", "tx-1", "ariel.transactions.validated", "{}", "corr-1", "t-1", Instant.now()
        );
        when(repository.findUnpublishedBatch(any(Pageable.class))).thenReturn(List.of(event));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> failingKafka = Mockito.mock(KafkaTemplate.class);
        when(failingKafka.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("Kafka Broker Unreachable"));

        @SuppressWarnings("unchecked")
        org.springframework.beans.factory.ObjectProvider<KafkaTemplate<String, String>> provider =
                Mockito.mock(org.springframework.beans.factory.ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(failingKafka);
        var relayWorker = new OutboxRelayWorker(repository, provider);

        int relayed = relayWorker.relayPendingBatch();
        assertThat(relayed).isZero();
        assertThat(event.isPublished()).isFalse();
        assertThat(event.getPublishedAt()).isNull();
    }
}
