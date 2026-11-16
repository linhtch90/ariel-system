package io.ariel.gateway.web;

import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.AnalyticsSummaryDto;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.gateway.client.DownstreamClientsConfig.IngestionGatewayClient;
import io.ariel.gateway.client.DownstreamClientsConfig.QueryGatewayClient;
import io.ariel.gateway.ratelimit.RedisTokenBucketRateLimiter;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class EdgeGatewayController {

    private final RedisTokenBucketRateLimiter rateLimiter;
    private final IngestionGatewayClient ingestionClient;
    private final QueryGatewayClient queryClient;
    private final CircuitBreaker ingestionCircuitBreaker;
    private final CircuitBreaker queryCircuitBreaker;

    public EdgeGatewayController(
            RedisTokenBucketRateLimiter rateLimiter,
            IngestionGatewayClient ingestionClient,
            QueryGatewayClient queryClient
    ) {
        this.rateLimiter = rateLimiter;
        this.ingestionClient = ingestionClient;
        this.queryClient = queryClient;
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        this.ingestionCircuitBreaker = registry.circuitBreaker("ingestion-service");
        this.queryCircuitBreaker = registry.circuitBreaker("query-service");
    }

    @PostMapping("/transactions")
    public ResponseEntity<TransactionReceipt> routeTransaction(@Valid @RequestBody TransactionCommand command) {
        String tenantId = RequestContextHolder.current().tenantId();
        long remaining = rateLimiter.checkAndConsumeToken(tenantId);

        TransactionReceipt receipt = ingestionCircuitBreaker.executeSupplier(
                () -> ingestionClient.submitTransaction(command)
        );

        HttpStatus status = receipt.idempotentReplay() ? HttpStatus.OK : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status)
                .header("X-RateLimit-Remaining", String.valueOf(remaining))
                .body(receipt);
    }

    @GetMapping("/analytics/tenants/{tenantId}/summary")
    public ResponseEntity<AnalyticsSummaryDto> routeAnalyticsSummary(@PathVariable String tenantId) {
        long remaining = rateLimiter.checkAndConsumeToken(tenantId);
        AnalyticsSummaryDto summary = queryCircuitBreaker.executeSupplier(
                () -> queryClient.getTenantSummary(tenantId)
        );
        return ResponseEntity.ok()
                .header("X-RateLimit-Remaining", String.valueOf(remaining))
                .body(summary);
    }
}
