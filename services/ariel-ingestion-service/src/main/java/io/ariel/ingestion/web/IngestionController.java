package io.ariel.ingestion.web;

import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.ingestion.usecase.IngestionOrchestrationUseCase;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
public class IngestionController {

    private final IngestionOrchestrationUseCase orchestrationUseCase;

    public IngestionController(IngestionOrchestrationUseCase orchestrationUseCase) {
        this.orchestrationUseCase = orchestrationUseCase;
    }

    @PostMapping
    public ResponseEntity<TransactionReceipt> ingest(@Valid @RequestBody TransactionCommand command) {
        TransactionReceipt receipt = orchestrationUseCase.processTransaction(command);
        HttpStatus status = receipt.idempotentReplay() ? HttpStatus.OK : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).body(receipt);
    }
}
