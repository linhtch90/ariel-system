package io.ariel.ledger.web;

import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.ledger.service.LedgerCommandService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/ledger")
public class LedgerController {

    private final LedgerCommandService ledgerCommandService;

    public LedgerController(LedgerCommandService ledgerCommandService) {
        this.ledgerCommandService = ledgerCommandService;
    }

    @PostMapping("/entries")
    @ResponseStatus(HttpStatus.CREATED)
    public LedgerEntryDto recordEntry(@Valid @RequestBody LedgerEntryDto command) {
        return ledgerCommandService.commitTransaction(command);
    }
}
