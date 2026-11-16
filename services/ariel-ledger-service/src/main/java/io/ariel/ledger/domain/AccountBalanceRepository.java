package io.ariel.ledger.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountBalanceRepository extends JpaRepository<AccountBalanceEntity, String> {
}
