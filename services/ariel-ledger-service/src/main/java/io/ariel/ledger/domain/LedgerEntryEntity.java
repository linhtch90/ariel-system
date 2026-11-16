package io.ariel.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(
        name = "ledger_entries",
        indexes = {
                @Index(name = "idx_ledger_tenant_account", columnList = "tenant_id, account_id, committed_at")
        }
)
public class LedgerEntryEntity {

    @Id
    @Column(name = "transaction_id", nullable = false, length = 64)
    private String transactionId;

    @Column(name = "correlation_id", nullable = false, length = 64)
    private String correlationId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "account_id", nullable = false, length = 64)
    private String accountId;

    @Column(name = "counterparty_id", nullable = false, length = 64)
    private String counterpartyId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 12)
    private String currency;

    @Column(name = "region", nullable = false, length = 32)
    private String region;

    @Column(name = "risk_score", nullable = false)
    private double riskScore;

    @Column(name = "resulting_balance", nullable = false, precision = 19, scale = 4)
    private BigDecimal resultingBalance;

    @Column(name = "committed_at", nullable = false)
    private Instant committedAt;

    protected LedgerEntryEntity() {
    }

    public LedgerEntryEntity(
            String transactionId,
            String correlationId,
            String tenantId,
            String accountId,
            String counterpartyId,
            BigDecimal amount,
            String currency,
            String region,
            double riskScore,
            BigDecimal resultingBalance,
            Instant committedAt
    ) {
        this.transactionId = transactionId;
        this.correlationId = correlationId;
        this.tenantId = tenantId;
        this.accountId = accountId;
        this.counterpartyId = counterpartyId;
        this.amount = amount;
        this.currency = currency;
        this.region = region;
        this.riskScore = riskScore;
        this.resultingBalance = resultingBalance;
        this.committedAt = committedAt;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getCounterpartyId() {
        return counterpartyId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getRegion() {
        return region;
    }

    public double getRiskScore() {
        return riskScore;
    }

    public BigDecimal getResultingBalance() {
        return resultingBalance;
    }

    public Instant getCommittedAt() {
        return committedAt;
    }
}
