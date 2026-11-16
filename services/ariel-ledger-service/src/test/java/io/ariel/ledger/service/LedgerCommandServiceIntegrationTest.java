package io.ariel.ledger.service;

import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.ledger.domain.AccountBalanceEntity;
import io.ariel.ledger.domain.AccountBalanceRepository;
import io.ariel.ledger.domain.LedgerEntryRepository;
import io.ariel.outbox.OutboxEventRepository;
import io.ariel.outbox.OutboxRelayWorker;
import io.ariel.outbox.TransactionalOutboxPublisher;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.IllegalTransactionStateException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "ariel.outbox.initial-delay-ms=600000",
        "ariel.outbox.poll-interval-ms=600000",
        "management.otlp.metrics.export.enabled=false"
})
@AutoConfigureMockMvc
class LedgerCommandServiceIntegrationTest {

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MockMvc mockMvc;

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

    @Autowired
    private TransactionalOutboxPublisher outboxPublisher;

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

    @Test
    void optimisticLockingVersionPreventsLostUpdatesOnConcurrentAccountModification() {
        accountBalanceRepository.saveAndFlush(new AccountBalanceEntity(
                "acc-optimistic-1", "tenant-fin", new BigDecimal("500.00"), "USD", Instant.now()
        ));

        // Load two detached snapshots of the exact same account row (both have version = 0)
        AccountBalanceEntity snapshotA = accountBalanceRepository.findById("acc-optimistic-1").orElseThrow();
        AccountBalanceEntity snapshotB = accountBalanceRepository.findById("acc-optimistic-1").orElseThrow();

        snapshotA.applyMovement(new BigDecimal("100.00"), Instant.now());
        accountBalanceRepository.saveAndFlush(snapshotA); // Succeeds and increments version to 1

        snapshotB.applyMovement(new BigDecimal("50.00"), Instant.now());
        assertThatThrownBy(() -> accountBalanceRepository.saveAndFlush(snapshotB))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);
    }

    @Test
    void transactionalOutboxPublisherEnforcesMandatoryTransactionPropagation() {
        assertThatThrownBy(() -> outboxPublisher.stageEvent("Test", "id-1", "topic", "{}"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void ledgerControllerRecordsEntryOverHttp201Created() throws Exception {
        String body = """
                {
                  "transactionId": "tx-http-ledger-1",
                  "correlationId": "corr-http-1",
                  "tenantId": "tenant-http",
                  "accountId": "acc-http-ledger-1",
                  "counterpartyId": "merch-http-1",
                  "amount": 325.00,
                  "currency": "USD",
                  "region": "EU-WEST",
                  "riskScore": 0.04
                }
                """;

        mockMvc.perform(post("/api/v1/ledger/entries")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value("tx-http-ledger-1"))
                .andExpect(jsonPath("$.newBalance").value(325.00));
    }
}
