package io.ariel.ledger.service;

import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.ValidatedTransactionEvent;
import io.ariel.ledger.domain.AccountBalanceEntity;
import io.ariel.ledger.domain.AccountBalanceRepository;
import io.ariel.ledger.domain.LedgerEntryEntity;
import io.ariel.ledger.domain.LedgerEntryRepository;
import io.ariel.outbox.TransactionalOutboxPublisher;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Authoritative ACID Ledger command handler that updates account balances, records
 * immutable ledger entries, and stages an Outbox event in a single atomic database transaction.
 */
@Service
public class LedgerCommandService {

    public static final String VALIDATED_TRANSACTIONS_TOPIC = "ariel.transactions.validated";

    private final AccountBalanceRepository accountBalanceRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final TransactionalOutboxPublisher outboxPublisher;

    public LedgerCommandService(
            AccountBalanceRepository accountBalanceRepository,
            LedgerEntryRepository ledgerEntryRepository,
            TransactionalOutboxPublisher outboxPublisher
    ) {
        this.accountBalanceRepository = accountBalanceRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.outboxPublisher = outboxPublisher;
    }

    @Transactional
    public LedgerEntryDto commitTransaction(LedgerEntryDto command) {
        Instant now = Instant.now();
        String txId = (command.transactionId() == null || command.transactionId().isBlank())
                ? UUID.randomUUID().toString()
                : command.transactionId();

        // 1. Update Account Balance with @Version optimistic locking
        AccountBalanceEntity account = accountBalanceRepository.findById(command.accountId())
                .orElseGet(() -> new AccountBalanceEntity(
                        command.accountId(),
                        command.tenantId(),
                        BigDecimal.ZERO,
                        command.currency(),
                        now
                ));
        BigDecimal newBalance = account.applyMovement(command.amount(), now);
        accountBalanceRepository.save(account);

        // 2. Persist immutable Ledger Entry
        LedgerEntryEntity entry = new LedgerEntryEntity(
                txId,
                command.correlationId(),
                command.tenantId(),
                command.accountId(),
                command.counterpartyId(),
                command.amount(),
                command.currency(),
                command.region(),
                command.riskScore(),
                newBalance,
                now
        );
        ledgerEntryRepository.save(entry);

        // 3. Stage Transactional Outbox event in the exact same ACID transaction
        ValidatedTransactionEvent eventPayload = new ValidatedTransactionEvent(
                UUID.randomUUID().toString(),
                txId,
                command.correlationId(),
                command.tenantId(),
                command.accountId(),
                command.counterpartyId(),
                command.amount(),
                command.currency(),
                command.region(),
                command.riskScore(),
                newBalance,
                now
        );
        outboxPublisher.stageEvent("LedgerTransaction", txId, VALIDATED_TRANSACTIONS_TOPIC, eventPayload);

        return new LedgerEntryDto(
                txId,
                command.correlationId(),
                command.tenantId(),
                command.accountId(),
                command.counterpartyId(),
                command.amount(),
                command.currency(),
                command.region(),
                command.riskScore(),
                newBalance,
                now
        );
    }
}
