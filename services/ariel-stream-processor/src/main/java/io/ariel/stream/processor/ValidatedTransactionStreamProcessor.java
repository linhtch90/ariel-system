package io.ariel.stream.processor;

import io.ariel.common.domain.ValidatedTransactionEvent;
import io.ariel.stream.sink.PolyglotAnalyticsSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.StructuredTaskScope;
import java.util.stream.Gatherers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * High-Throughput Stream Processor using:
 * <ul>
 *   <li><b>Java 25 Stream Gatherers ({@code Gatherers.windowFixed})</b> for in-stream micro-batching</li>
 *   <li><b>Java 25 {@link StructuredTaskScope}</b> for parallel polyglot sink fan-out</li>
 *   <li><b>Dead Letter Queue ({@code ariel.dlq})</b> isolation for malformed poison-pill events</li>
 * </ul>
 */
@Service
public class ValidatedTransactionStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(ValidatedTransactionStreamProcessor.class);
    public static final String DLQ_TOPIC = "ariel.dlq";

    private final PolyglotAnalyticsSink polyglotSink;
    private final JsonMapper jsonMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int microBatchWindowSize;
    private final List<String> dlqBuffer = new CopyOnWriteArrayList<>();

    public ValidatedTransactionStreamProcessor(
            PolyglotAnalyticsSink polyglotSink,
            JsonMapper jsonMapper,
            ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider,
            @Value("${ariel.stream.micro-batch-size:100}") int microBatchWindowSize
    ) {
        this.polyglotSink = polyglotSink;
        this.jsonMapper = jsonMapper;
        this.kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
        this.microBatchWindowSize = microBatchWindowSize;
    }

    @KafkaListener(
            topics = "ariel.transactions.validated",
            groupId = "ariel-stream-processor-group",
            autoStartup = "${ariel.stream.kafka-listener.auto-startup:false}"
    )
    public void onKafkaMessageBatch(List<String> rawJsonPayloads) {
        processRawBatch(rawJsonPayloads);
    }

    public int processRawBatch(List<String> rawJsonPayloads) {
        List<ValidatedTransactionEvent> validEvents = new ArrayList<>(rawJsonPayloads.size());

        for (String rawJson : rawJsonPayloads) {
            try {
                ValidatedTransactionEvent event = jsonMapper.readValue(rawJson, ValidatedTransactionEvent.class);
                if (event.transactionId() == null || event.tenantId() == null) {
                    throw new IllegalArgumentException("Missing required transactionId or tenantId");
                }
                validEvents.add(event);
            } catch (Exception e) {
                routeToDeadLetterQueue(rawJson, e);
            }
        }

        if (validEvents.isEmpty()) {
            return 0;
        }

        // Use Java 25 Stream Gatherers (Gatherers.windowFixed) to partition stream into micro-batches
        List<List<ValidatedTransactionEvent>> microBatches = validEvents.stream()
                .gather(Gatherers.windowFixed(microBatchWindowSize))
                .toList();

        int processedCount = 0;
        for (List<ValidatedTransactionEvent> microBatch : microBatches) {
            fanOutMicroBatchInParallel(microBatch);
            processedCount += microBatch.size();
        }
        return processedCount;
    }

    private void fanOutMicroBatchInParallel(List<ValidatedTransactionEvent> microBatch) {
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Integer>awaitAllSuccessfulOrThrow(),
                cf -> cf.withName("polyglot-sink-fanout").withTimeout(Duration.ofSeconds(5)))) {

            scope.fork(() -> polyglotSink.writeScyllaTimeSeriesBatch(microBatch));
            scope.fork(() -> polyglotSink.writeClickHouseOlapBatch(microBatch));
            scope.fork(() -> polyglotSink.updateRedisAndInvalidateProjections(microBatch));

            scope.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during parallel polyglot sink write", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed parallel polyglot sink write: " + e.getMessage(), e);
        }
    }

    private void routeToDeadLetterQueue(String rawPayload, Exception cause) {
        log.warn("Routing poison-pill message to {}: {}", DLQ_TOPIC, cause.getMessage());
        dlqBuffer.add(rawPayload);
        if (kafkaTemplate != null) {
            try {
                kafkaTemplate.send(DLQ_TOPIC, "poison-pill", rawPayload);
            } catch (Exception e) {
                log.warn("DLQ broker publish deferred: {}", e.getMessage());
            }
        }
    }

    public List<String> getDlqBuffer() {
        return List.copyOf(dlqBuffer);
    }
}
