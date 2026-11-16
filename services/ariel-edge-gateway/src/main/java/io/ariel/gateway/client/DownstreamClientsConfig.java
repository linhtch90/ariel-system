package io.ariel.gateway.client;

import io.ariel.common.domain.AnalyticsSummaryDto;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class DownstreamClientsConfig {

    @HttpExchange
    public interface IngestionGatewayClient {
        @PostExchange("/api/v1/transactions")
        TransactionReceipt submitTransaction(@RequestBody TransactionCommand command);
    }

    @HttpExchange
    public interface QueryGatewayClient {
        @GetExchange("/api/v1/analytics/tenants/{tenantId}/summary")
        AnalyticsSummaryDto getTenantSummary(@PathVariable("tenantId") String tenantId);
    }

    @Bean
    public IngestionGatewayClient ingestionGatewayClient(
            RestClient.Builder restClientBuilder,
            @Value("${ariel.services.ingestion-url:http://localhost:8081}") String ingestionUrl
    ) {
        RestClient client = restClientBuilder.baseUrl(ingestionUrl).build();
        return HttpServiceProxyFactory.builderFor(RestClientAdapter.create(client))
                .build()
                .createClient(IngestionGatewayClient.class);
    }

    @Bean
    public QueryGatewayClient queryGatewayClient(
            RestClient.Builder restClientBuilder,
            @Value("${ariel.services.query-url:http://localhost:8085}") String queryUrl
    ) {
        RestClient client = restClientBuilder.baseUrl(queryUrl).build();
        return HttpServiceProxyFactory.builderFor(RestClientAdapter.create(client))
                .build()
                .createClient(QueryGatewayClient.class);
    }
}
