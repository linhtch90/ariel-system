package io.ariel.stream.processor;

import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.domain.ValidatedTransactionEvent;
import io.ariel.stream.sink.PolyglotAnalyticsSink;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ValidatedTransactionStreamProcessorTest {

    @Test
    void gatherersWindowFixedMicroBatchesValidEventsAndIsolatesPoisonPillsToDlq() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var sink = new PolyglotAnalyticsSink(nearCache);
        var mapper = JsonMapper.builder().build();
        @SuppressWarnings("unchecked")
        var kafkaProvider = (org.springframework.beans.factory.ObjectProvider<KafkaTemplate<String, String>>)
                (org.springframework.beans.factory.ObjectProvider<?>)
                        new StaticListableBeanFactory().getBeanProvider(KafkaTemplate.class);

        var processor = new ValidatedTransactionStreamProcessor(sink, mapper, kafkaProvider, 3);

        List<String> rawBatch = new ArrayList<>();
        for (int i = 1; i <= 7; i++) {
            var ev = new ValidatedTransactionEvent(
                    "ev-" + i,
                    "tx-" + i,
                    "corr-" + i,
                    "tenant-stream",
                    "acc-" + i,
                    "merchant-1",
                    new BigDecimal("100.00"),
                    "USD",
                    "US-EAST",
                    0.04,
                    new BigDecimal("500.00"),
                    Instant.parse("2026-11-16T10:15:30Z")
            );
            rawBatch.add(mapper.writeValueAsString(ev));
        }
        // Add a malformed poison-pill payload
        rawBatch.add("{malformed-json-payload");

        int processed = processor.processRawBatch(rawBatch);

        assertThat(processed).isEqualTo(7);
        assertThat(sink.getClickHouseCountForTenant("tenant-stream")).isEqualTo(7);
        assertThat(sink.getScyllaPartition("tenant-stream", "2026-11-16")).hasSize(7);
        assertThat(processor.getDlqBuffer()).hasSize(1);
    }
}
