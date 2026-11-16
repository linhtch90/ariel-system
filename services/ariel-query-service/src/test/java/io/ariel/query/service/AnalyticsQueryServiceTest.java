package io.ariel.query.service;

import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.domain.AnalyticsSummaryDto;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class AnalyticsQueryServiceTest {

    @Test
    void servesSummaryFromTwoTierNearCacheAndInvalidatesOnUpdate() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(5), Duration.ofMinutes(30));
        var queryService = new AnalyticsQueryService(nearCache, JsonMapper.builder().build());

        AnalyticsSummaryDto initial = queryService.getTenantSummary("tenant-enterprise");
        assertThat(initial.totalTransactions()).isEqualTo(1_250L);

        queryService.recordProjectionUpdate(new AnalyticsSummaryDto(
                "tenant-enterprise",
                5_000L,
                new BigDecimal("1950000.00"),
                0.031,
                0.220,
                5L,
                Instant.now()
        ));

        AnalyticsSummaryDto updated = queryService.getTenantSummary("tenant-enterprise");
        assertThat(updated.totalTransactions()).isEqualTo(5_000L);
        assertThat(updated.totalVolume()).isEqualByComparingTo("1950000.00");
    }
}
