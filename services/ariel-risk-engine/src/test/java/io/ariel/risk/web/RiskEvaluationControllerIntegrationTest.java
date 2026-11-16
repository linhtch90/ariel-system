package io.ariel.risk.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "management.otlp.metrics.export.enabled=false")
@AutoConfigureMockMvc
class RiskEvaluationControllerIntegrationTest {

    @MockitoBean
    private StringRedisTemplate redisTemplate;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void evaluatesRiskOverHttpAndSerializesPolymorphicSealedSubtype() throws Exception {
        String requestJson = """
                {
                  "correlationId": "corr-risk-http-1",
                  "tenantId": "tenant-risk",
                  "accountId": "acc-risk-1",
                  "counterpartyId": "merch-risk-1",
                  "amount": 199.99,
                  "currency": "USD",
                  "region": "US-EAST",
                  "deviceFingerprint": "fp-ios-ok"
                }
                """;

        mockMvc.perform(post("/api/v1/risk/evaluate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.routingTier").value("FAST_LANE"));
    }
}
