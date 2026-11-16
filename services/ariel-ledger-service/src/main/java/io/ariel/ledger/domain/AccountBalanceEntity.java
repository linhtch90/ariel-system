package io.ariel.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "account_balances")
public class AccountBalanceEntity {

    @Id
    @Column(name = "account_id", nullable = false, length = 64)
    private String accountId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "balance", nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Column(name = "currency", nullable = false, length = 12)
    private String currency;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected AccountBalanceEntity() {
    }

    public AccountBalanceEntity(String accountId, String tenantId, BigDecimal balance, String currency, Instant updatedAt) {
        this.accountId = accountId;
        this.tenantId = tenantId;
        this.balance = balance;
        this.currency = currency;
        this.updatedAt = updatedAt;
    }

    public BigDecimal applyMovement(BigDecimal delta, Instant when) {
        this.balance = this.balance.add(delta);
        this.updatedAt = when;
        return this.balance;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public String getCurrency() {
        return currency;
    }

    public long getVersion() {
        return version;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
