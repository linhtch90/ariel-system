package io.ariel.query.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "management.otlp.metrics.export.enabled=false")
@AutoConfigureMockMvc
class AnalyticsQueryControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void servesTenantSummaryOverHttpFromReadThroughNearCache() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/tenants/tenant-dashboard-1/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value("tenant-dashboard-1"))
                .andExpect(jsonPath("$.totalTransactions").value(1250));
    }
}
