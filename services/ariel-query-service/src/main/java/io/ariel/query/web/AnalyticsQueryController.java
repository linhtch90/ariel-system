package io.ariel.query.web;

import io.ariel.common.domain.AnalyticsSummaryDto;
import io.ariel.query.service.AnalyticsQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsQueryController {

    private final AnalyticsQueryService analyticsQueryService;

    public AnalyticsQueryController(AnalyticsQueryService analyticsQueryService) {
        this.analyticsQueryService = analyticsQueryService;
    }

    @GetMapping("/tenants/{tenantId}/summary")
    public AnalyticsSummaryDto getTenantSummary(@PathVariable String tenantId) {
        return analyticsQueryService.getTenantSummary(tenantId);
    }
}
