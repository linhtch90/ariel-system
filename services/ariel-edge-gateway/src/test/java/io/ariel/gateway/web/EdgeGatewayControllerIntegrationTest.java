package io.ariel.gateway.web;

import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.AnalyticsSummaryDto;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.gateway.client.DownstreamClientsConfig.IngestionGatewayClient;
import io.ariel.gateway.client.DownstreamClientsConfig.QueryGatewayClient;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "ariel.gateway.rate-limit.burst-capacity=5",
        "management.otlp.metrics.export.enabled=false"
})
@AutoConfigureMockMvc
class EdgeGatewayControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private IngestionGatewayClient ingestionGatewayClient;

    @MockitoBean
    private QueryGatewayClient queryGatewayClient;

    @Test
    void routesValidTransactionReturning202ForNewAnd200ForIdempotentReplay() throws Exception {
        when(ingestionGatewayClient.submitTransaction(any()))
                .thenReturn(new TransactionReceipt(
                        "tx-gw-1", "corr-gw-1", "tenant-gw", "acc-1",
                        new BigDecimal("100.00"), "USD", "APPROVED_AND_COMMITTED", 0.02, false, 4L, Instant.now()
                ))
                .thenReturn(new TransactionReceipt(
                        "tx-gw-1", "corr-gw-1", "tenant-gw", "acc-1",
                        new BigDecimal("100.00"), "USD", "APPROVED_AND_COMMITTED", 0.02, true, 1L, Instant.now()
                ));

        String payload = """
                {
                  "accountId": "acc-1",
                  "counterpartyId": "merch-1",
                  "amount": 100.00,
                  "currency": "USD",
                  "region": "US-EAST",
                  "deviceFingerprint": "fp-1"
                }
                """;

        // 1. Initial submission returns 202 Accepted
        mockMvc.perform(post("/api/v1/transactions")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-gw")
                        .header(RequestContextHolder.HEADER_IDEMPOTENCY_KEY, "idem-gw-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("X-RateLimit-Remaining"))
                .andExpect(jsonPath("$.transactionId").value("tx-gw-1"))
                .andExpect(jsonPath("$.idempotentReplay").value(false));

        // 2. Idempotent replay returns 200 OK
        mockMvc.perform(post("/api/v1/transactions")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-gw")
                        .header(RequestContextHolder.HEADER_IDEMPOTENCY_KEY, "idem-gw-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.idempotentReplay").value(true));
    }

    @Test
    void rejectsInvalidPayloadWithRfc9457ValidationProblemDetailAtPerimeter() throws Exception {
        String invalidPayload = """
                {
                  "accountId": "",
                  "counterpartyId": "merch-1",
                  "amount": -10.00,
                  "currency": "USD",
                  "region": "US-EAST"
                }
                """;

        mockMvc.perform(post("/api/v1/transactions")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-val")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidPayload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.tenantId").value("tenant-val"));
    }

    @Test
    void routesAnalyticsQueryAndEnforcesRateLimitCeilingWith429ProblemDetail() throws Exception {
        when(queryGatewayClient.getTenantSummary("tenant-burst"))
                .thenReturn(new AnalyticsSummaryDto(
                        "tenant-burst", 10L, new BigDecimal("500.00"), 0.03, 0.10, 0L, Instant.now()
                ));

        for (int i = 0; i < 5; i++) {
            mockMvc.perform(get("/api/v1/analytics/tenants/tenant-burst/summary")
                            .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-burst"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.tenantId").value("tenant-burst"));
        }

        // 6th call in same second exceeds burst-capacity=5 -> HTTP 429 ProblemDetail
        mockMvc.perform(get("/api/v1/analytics/tenants/tenant-burst/summary")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-burst"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.title").value("TENANT_RATE_LIMIT_EXCEEDED"));
    }
}
