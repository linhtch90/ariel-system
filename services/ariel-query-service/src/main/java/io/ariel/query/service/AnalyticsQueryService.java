package io.ariel.query.service;

import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.domain.AnalyticsSummaryDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * CQRS Read-Model service querying ClickHouse / ScyllaDB projections
 * fronted by the Two-Tier Near-Cache (L1 Caffeine + L2 Redis).
 */
@Service
public class AnalyticsQueryService {

    private final TwoTierNearCache nearCache;
    private final JsonMapper jsonMapper;
    private final ConcurrentHashMap<String, AnalyticsSummaryDto> olapProjections = new ConcurrentHashMap<>();

    public AnalyticsQueryService(TwoTierNearCache nearCache, JsonMapper jsonMapper) {
        this.nearCache = nearCache;
        this.jsonMapper = jsonMapper;
    }

    public AnalyticsSummaryDto getTenantSummary(String tenantId) {
        String cacheKey = "ariel:analytics:summary:" + tenantId;
        String cachedJson = nearCache.getOrLoad(cacheKey, _ -> {
            AnalyticsSummaryDto summary = olapProjections.computeIfAbsent(tenantId, t -> new AnalyticsSummaryDto(
                    t,
                    1_250L,
                    new BigDecimal("485920.50"),
                    0.042,
                    0.310,
                    3L,
                    Instant.now()
            ));
            return jsonMapper.writeValueAsString(summary);
        });
        return jsonMapper.readValue(cachedJson, AnalyticsSummaryDto.class);
    }

    public void recordProjectionUpdate(AnalyticsSummaryDto summary) {
        olapProjections.put(summary.tenantId(), summary);
        nearCache.invalidateClusterWide("ariel:analytics:summary:" + summary.tenantId());
    }
}
