package io.ariel.ledger.service;

import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.ledger.domain.AccountBalanceRepository;
import io.ariel.ledger.domain.LedgerEntryRepository;
import io.ariel.outbox.OutboxEventRepository;
import io.ariel.outbox.OutboxRelayWorker;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "ariel.outbox.initial-delay-ms=600000",
        "ariel.outbox.poll-interval-ms=600000",
        "management.otlp.metrics.export.enabled=false"
})
class LedgerCommandServiceIntegrationTest {

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private LedgerCommandService ledgerCommandService;

    @Autowired
    private AccountBalanceRepository accountBalanceRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxRelayWorker outboxRelayWorker;

    @Test
    void commitsAccountBalanceLedgerEntryAndTransactionalOutboxAtomically() {
        var cmd1 = new LedgerEntryDto(
                "tx-9001", "corr-9001", "tenant-fin", "acc-ledger-1", "merchant-99",
                new BigDecimal("150.25"), "USD", "US-EAST", 0.03, BigDecimal.ZERO, Instant.now()
        );
        var cmd2 = new LedgerEntryDto(
                "tx-9002", "corr-9002", "tenant-fin", "acc-ledger-1", "merchant-99",
                new BigDecimal("49.75"), "USD", "US-EAST", 0.02, BigDecimal.ZERO, Instant.now()
        );

        LedgerEntryDto committed1 = ledgerCommandService.commitTransaction(cmd1);
        LedgerEntryDto committed2 = ledgerCommandService.commitTransaction(cmd2);

        assertThat(committed1.newBalance()).isEqualByComparingTo("150.25");
        assertThat(committed2.newBalance()).isEqualByComparingTo("200.00");

        var balance = accountBalanceRepository.findById("acc-ledger-1").orElseThrow();
        assertThat(balance.getBalance()).isEqualByComparingTo("200.00");
        assertThat(ledgerEntryRepository.count()).isGreaterThanOrEqualTo(2);

        // Verify Outbox events were staged in the same transaction
        var unpublished = outboxEventRepository.findUnpublishedBatch(PageRequest.of(0, 10));
        assertThat(unpublished).hasSizeGreaterThanOrEqualTo(2);

        // Verify OutboxRelayWorker marks them published
        int relayed = outboxRelayWorker.relayPendingBatch();
        assertThat(relayed).isGreaterThanOrEqualTo(2);
    }
}
