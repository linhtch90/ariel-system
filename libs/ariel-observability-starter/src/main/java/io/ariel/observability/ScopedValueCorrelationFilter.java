package io.ariel.observability;

import io.ariel.common.context.RequestContext;
import io.ariel.common.context.RequestContextHolder;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Binds incoming request headers into a Java 25 {@link ScopedValue} for the duration
 * of the Virtual Thread request execution, and echoes correlation headers back on the response.
 */
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class ScopedValueCorrelationFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        String correlationId = request.getHeader(RequestContextHolder.HEADER_CORRELATION_ID);
        String tenantId = request.getHeader(RequestContextHolder.HEADER_TENANT_ID);
        String idempotencyKey = request.getHeader(RequestContextHolder.HEADER_IDEMPOTENCY_KEY);

        RequestContext context = new RequestContext(correlationId, tenantId, idempotencyKey, Instant.now());

        response.setHeader(RequestContextHolder.HEADER_CORRELATION_ID, context.correlationId());
        response.setHeader(RequestContextHolder.HEADER_TENANT_ID, context.tenantId());

        try {
            ScopedValue.where(RequestContextHolder.CONTEXT, context).call(() -> {
                filterChain.doFilter(request, response);
                return null;
            });
        } catch (ServletException | IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }
}
