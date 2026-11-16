package io.ariel.common.context;

import java.util.concurrent.Callable;

/**
 * Java 25 {@link ScopedValue} carrier replacing ThreadLocal for zero-overhead,
 * immutable context propagation across Virtual Threads and StructuredTaskScope subtasks.
 */
public final class RequestContextHolder {

    public static final ScopedValue<RequestContext> CONTEXT = ScopedValue.newInstance();

    public static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
    public static final String HEADER_TENANT_ID = "X-Tenant-Id";
    public static final String HEADER_IDEMPOTENCY_KEY = "Idempotency-Key";

    private RequestContextHolder() {
    }

    public static RequestContext current() {
        return CONTEXT.isBound() ? CONTEXT.get() : RequestContext.systemDefault();
    }

    public static void runWith(RequestContext context, Runnable action) {
        ScopedValue.where(CONTEXT, context).run(action);
    }

    public static <T> T callWith(RequestContext context, Callable<T> action) throws Exception {
        return ScopedValue.where(CONTEXT, context).call(action::call);
    }
}
