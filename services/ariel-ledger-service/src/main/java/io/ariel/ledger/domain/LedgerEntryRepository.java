package io.ariel.ledger.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntryEntity, String> {
}
