package io.ariel.ingestion.client;

import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class FastPathClientsConfig {

    @HttpExchange
    public interface RiskEngineClient {
        @PostExchange("/api/v1/risk/evaluate")
        RiskDecision evaluate(@RequestBody RiskEvaluationRequest request);
    }

    @HttpExchange
    public interface LedgerServiceClient {
        @PostExchange("/api/v1/ledger/entries")
        LedgerEntryDto recordTransaction(@RequestBody LedgerEntryDto command);
    }

    @Bean
    public RiskEngineClient riskEngineClient(
            RestClient.Builder restClientBuilder,
            @Value("${ariel.services.risk-url:http://localhost:8082}") String riskUrl
    ) {
        RestClient client = restClientBuilder.baseUrl(riskUrl).build();
        return HttpServiceProxyFactory.builderFor(RestClientAdapter.create(client))
                .build()
                .createClient(RiskEngineClient.class);
    }

    @Bean
    public LedgerServiceClient ledgerServiceClient(
            RestClient.Builder restClientBuilder,
            @Value("${ariel.services.ledger-url:http://localhost:8083}") String ledgerUrl
    ) {
        RestClient client = restClientBuilder.baseUrl(ledgerUrl).build();
        return HttpServiceProxyFactory.builderFor(RestClientAdapter.create(client))
                .build()
                .createClient(LedgerServiceClient.class);
    }
}
