package io.ariel.ingestion.web;

import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.ingestion.client.FastPathClientsConfig.LedgerServiceClient;
import io.ariel.ingestion.client.FastPathClientsConfig.RiskEngineClient;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "management.otlp.metrics.export.enabled=false")
@AutoConfigureMockMvc
class IngestionControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RiskEngineClient riskEngineClient;

    @MockitoBean
    private LedgerServiceClient ledgerServiceClient;

    @Test
    void ingestsTransactionOverHttpAndDeduplicatesRetry() throws Exception {
        when(riskEngineClient.evaluate(any()))
                .thenReturn(new RiskDecision.Approved(0.03, "FAST_LANE", 350L));
        when(ledgerServiceClient.recordTransaction(any()))
                .thenReturn(new LedgerEntryDto(
                        "tx-http-101", "corr-101", "tenant-http", "acc-1", "merch-1",
                        new BigDecimal("99.95"), "USD", "US-EAST", 0.03, new BigDecimal("500.00"), Instant.now()
                ));

        String json = """
                {
                  "accountId": "acc-1",
                  "counterpartyId": "merch-1",
                  "amount": 99.95,
                  "currency": "USD",
                  "region": "US-EAST",
                  "deviceFingerprint": "fp-trusted"
                }
                """;

        mockMvc.perform(post("/api/v1/transactions")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-http")
                        .header(RequestContextHolder.HEADER_IDEMPOTENCY_KEY, "idem-http-101")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.transactionId").value("tx-http-101"))
                .andExpect(jsonPath("$.idempotentReplay").value(false));

        mockMvc.perform(post("/api/v1/transactions")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-http")
                        .header(RequestContextHolder.HEADER_IDEMPOTENCY_KEY, "idem-http-101")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value("tx-http-101"))
                .andExpect(jsonPath("$.idempotentReplay").value(true));
    }
}
