package io.ariel.observability;

import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.error.ArielBusinessException;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Standardized RFC 9457 ProblemDetail exception translator across all Ariel microservices.
 */
@RestControllerAdvice
public class GlobalProblemDetailExceptionHandler {

    @ExceptionHandler(ArielBusinessException.class)
    public ProblemDetail handleBusinessException(ArielBusinessException ex) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(ex.getStatus(), ex.getMessage());
        detail.setTitle(ex.getErrorCode());
        detail.setType(URI.create("https://ariel.io/errors/" + ex.getErrorCode().toLowerCase()));
        enrichWithContext(detail);
        return detail;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        String errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .collect(Collectors.joining(", "));
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, errors);
        detail.setTitle("VALIDATION_FAILED");
        detail.setType(URI.create("https://ariel.io/errors/validation-failed"));
        enrichWithContext(detail);
        return detail;
    }

    @ExceptionHandler(CallNotPermittedException.class)
    public ProblemDetail handleCircuitBreakerOpen(CallNotPermittedException ex) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Downstream service circuit breaker is OPEN: " + ex.getCausingCircuitBreakerName()
        );
        detail.setTitle("CIRCUIT_BREAKER_OPEN");
        detail.setType(URI.create("https://ariel.io/errors/circuit-breaker-open"));
        enrichWithContext(detail);
        return detail;
    }

    @ExceptionHandler({RequestNotPermitted.class, BulkheadFullException.class})
    public ProblemDetail handleRateLimitOrBulkhead(RuntimeException ex) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.TOO_MANY_REQUESTS,
                "Concurrency or rate limit exceeded: " + ex.getMessage()
        );
        detail.setTitle("RATE_LIMIT_OR_BULKHEAD_EXCEEDED");
        detail.setType(URI.create("https://ariel.io/errors/rate-limit-exceeded"));
        enrichWithContext(detail);
        return detail;
    }

    private void enrichWithContext(ProblemDetail detail) {
        var ctx = RequestContextHolder.current();
        detail.setProperty("correlationId", ctx.correlationId());
        detail.setProperty("tenantId", ctx.tenantId());
        detail.setProperty("timestamp", Instant.now().toString());
    }
}
