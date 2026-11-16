package io.ariel.observability;

import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.error.ArielBusinessException;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ObservabilityFilterAndProblemDetailIntegrationTest {

    private MockMvc mockMvc;
    private final ArielObservabilityAutoConfiguration autoConfig = new ArielObservabilityAutoConfiguration();

    record SamplePayload(@NotBlank(message = "name must not be blank") String name) {}

    @RestController
    static class TestProbeController {

        @GetMapping("/probe/context")
        String currentContext() {
            var ctx = RequestContextHolder.current();
            return ctx.correlationId() + "|" + ctx.tenantId() + "|" + ctx.idempotencyKey();
        }

        @PostMapping("/probe/validate")
        String validate(@Valid @RequestBody SamplePayload payload) {
            return payload.name();
        }

        @GetMapping("/probe/business-error")
        String businessError() {
            throw new ArielBusinessException(HttpStatus.UNPROCESSABLE_ENTITY, "INSUFFICIENT_FUNDS", "Account balance too low");
        }

        @GetMapping("/probe/circuit-open")
        String circuitOpen() {
            CircuitBreaker cb = CircuitBreaker.ofDefaults("ledger-downstream");
            cb.transitionToOpenState();
            throw CallNotPermittedException.createCallNotPermittedException(cb);
        }

        @GetMapping("/probe/bulkhead-full")
        String bulkheadFull() {
            throw BulkheadFullException.createBulkheadFullException(
                    io.github.resilience4j.bulkhead.Bulkhead.ofDefaults("risk-bulkhead")
            );
        }
    }

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.standaloneSetup(new TestProbeController())
                .setControllerAdvice(autoConfig.globalProblemDetailExceptionHandler())
                .addFilters(autoConfig.scopedValueCorrelationFilter())
                .build();
    }

    @Test
    void bindsRequestHeadersIntoScopedValueAndEchoesResponseHeaders() throws Exception {
        mockMvc.perform(get("/probe/context")
                        .header(RequestContextHolder.HEADER_CORRELATION_ID, "corr-abc-001")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-fintech")
                        .header(RequestContextHolder.HEADER_IDEMPOTENCY_KEY, "idem-xyz-999"))
                .andExpect(status().isOk())
                .andExpect(header().string(RequestContextHolder.HEADER_CORRELATION_ID, "corr-abc-001"))
                .andExpect(header().string(RequestContextHolder.HEADER_TENANT_ID, "tenant-fintech"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.content()
                        .string("corr-abc-001|tenant-fintech|idem-xyz-999"));
    }

    @Test
    void formatsValidationFailureAsRfc9457ProblemDetailWithCorrelationMetadata() throws Exception {
        mockMvc.perform(post("/probe/validate")
                        .header(RequestContextHolder.HEADER_CORRELATION_ID, "corr-val-400")
                        .header(RequestContextHolder.HEADER_TENANT_ID, "tenant-acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("name must not be blank")))
                .andExpect(jsonPath("$.correlationId").value("corr-val-400"))
                .andExpect(jsonPath("$.tenantId").value("tenant-acme"));
    }

    @Test
    void formatsBusinessCircuitBreakerAndBulkheadExceptionsAsStandardProblemDetails() throws Exception {
        mockMvc.perform(get("/probe/business-error")
                        .header(RequestContextHolder.HEADER_CORRELATION_ID, "corr-biz-422"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.title").value("INSUFFICIENT_FUNDS"))
                .andExpect(jsonPath("$.correlationId").value("corr-biz-422"));

        mockMvc.perform(get("/probe/circuit-open"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("CIRCUIT_BREAKER_OPEN"))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("ledger-downstream")));

        mockMvc.perform(get("/probe/bulkhead-full"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.title").value("RATE_LIMIT_OR_BULKHEAD_EXCEEDED"));
    }

    @Test
    void clientHttpRequestInitializerPropagatesScopedValueHeadersToOutboundRestClientCalls() throws Exception {
        var initializer = autoConfig.arielContextPropagationInitializer();
        var ctx = new io.ariel.common.context.RequestContext("corr-out-77", "tenant-out", "idem-out-88", null);

        RequestContextHolder.runWith(ctx, () -> {
            MockClientHttpRequest outboundReq = new MockClientHttpRequest();
            initializer.initialize(outboundReq);

            assertThat(outboundReq.getHeaders().getFirst(RequestContextHolder.HEADER_CORRELATION_ID)).isEqualTo("corr-out-77");
            assertThat(outboundReq.getHeaders().getFirst(RequestContextHolder.HEADER_TENANT_ID)).isEqualTo("tenant-out");
            assertThat(outboundReq.getHeaders().getFirst(RequestContextHolder.HEADER_IDEMPOTENCY_KEY)).isEqualTo("idem-out-88");
        });
    }
}
