package io.ariel.observability;

import io.ariel.common.context.RequestContextHolder;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.client.ClientHttpRequestInitializer;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class ArielObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ScopedValueCorrelationFilter scopedValueCorrelationFilter() {
        return new ScopedValueCorrelationFilter();
    }

    @Bean
    @ConditionalOnMissingBean
    public GlobalProblemDetailExceptionHandler globalProblemDetailExceptionHandler() {
        return new GlobalProblemDetailExceptionHandler();
    }

    /**
     * Automatically propagates ScopedValue correlation, tenant, and idempotency headers
     * to all outgoing RestClient / @HttpExchange requests.
     */
    @Bean
    public ClientHttpRequestInitializer arielContextPropagationInitializer() {
        return request -> {
            var ctx = RequestContextHolder.current();
            request.getHeaders().set(RequestContextHolder.HEADER_CORRELATION_ID, ctx.correlationId());
            request.getHeaders().set(RequestContextHolder.HEADER_TENANT_ID, ctx.tenantId());
            request.getHeaders().set(RequestContextHolder.HEADER_IDEMPOTENCY_KEY, ctx.idempotencyKey());
        };
    }
}
