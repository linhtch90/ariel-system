package io.ariel.chaos;

import io.ariel.common.context.RequestContext;
import io.ariel.common.domain.AnalyticsSummaryDto;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.common.domain.ValidatedTransactionEvent;
import io.ariel.gateway.web.EdgeGatewayController;
import io.ariel.ingestion.usecase.IngestionOrchestrationUseCase;
import io.ariel.ingestion.web.IngestionController;
import io.ariel.ledger.service.LedgerCommandService;
import io.ariel.ledger.web.LedgerController;
import io.ariel.query.service.AnalyticsQueryService;
import io.ariel.query.web.AnalyticsQueryController;
import io.ariel.risk.service.ParallelRiskEvaluationService;
import io.ariel.risk.web.RiskEvaluationController;
import io.ariel.stream.processor.ValidatedTransactionStreamProcessor;
import io.ariel.stream.sink.PolyglotAnalyticsSink;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.Repository;
import org.springframework.web.bind.annotation.RestController;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Java 25 Architectural Governance Suite enforcing clean/hexagonal boundaries,
 * immutable domain modeling, and constructor-injection standards across all Ariel modules.
 */
class ArielArchitectureGovernanceTest {

    private static final List<Class<?>> PRODUCTION_COMPONENTS = List.of(
            EdgeGatewayController.class,
            IngestionController.class,
            IngestionOrchestrationUseCase.class,
            RiskEvaluationController.class,
            ParallelRiskEvaluationService.class,
            LedgerController.class,
            LedgerCommandService.class,
            ValidatedTransactionStreamProcessor.class,
            PolyglotAnalyticsSink.class,
            AnalyticsQueryController.class,
            AnalyticsQueryService.class
    );

    private static final List<Class<?>> DOMAIN_CONTRACTS = List.of(
            RequestContext.class,
            TransactionCommand.class,
            RiskEvaluationRequest.class,
            RiskDecision.class,
            RiskDecision.Approved.class,
            RiskDecision.Rejected.class,
            RiskDecision.ManualReviewRequired.class,
            TransactionReceipt.class,
            LedgerEntryDto.class,
            ValidatedTransactionEvent.class,
            AnalyticsSummaryDto.class
    );

    private static final List<Class<?>> WEB_CONTROLLERS = List.of(
            EdgeGatewayController.class,
            IngestionController.class,
            RiskEvaluationController.class,
            LedgerController.class,
            AnalyticsQueryController.class
    );

    @Test
    void noProductionComponentsUseAutowiredFieldInjection() {
        for (Class<?> component : PRODUCTION_COMPONENTS) {
            for (Field field : component.getDeclaredFields()) {
                assertThat(field.isAnnotationPresent(Autowired.class))
                        .as("Class %s field %s must not use @Autowired field injection",
                                component.getSimpleName(), field.getName())
                        .isFalse();
            }
        }
    }

    @Test
    void allCommonDomainContractsAreJava25RecordsOrSealedInterfaces() {
        for (Class<?> contract : DOMAIN_CONTRACTS) {
            boolean isRecordOrSealed = contract.isRecord() || contract.isSealed();
            assertThat(isRecordOrSealed)
                    .as("Domain contract %s must be a Java 25 record or sealed interface", contract.getName())
                    .isTrue();
        }
    }

    @Test
    void allWebControllersResideInWebPackageAndNeverInjectRepositoriesDirectly() {
        for (Class<?> controller : WEB_CONTROLLERS) {
            assertThat(controller.isAnnotationPresent(RestController.class)).isTrue();
            assertThat(controller.getPackageName()).endsWith(".web");

            for (Field field : controller.getDeclaredFields()) {
                assertThat(Repository.class.isAssignableFrom(field.getType()))
                        .as("Controller %s must delegate to a service/usecase instead of injecting Repository %s",
                                controller.getSimpleName(), field.getType().getSimpleName())
                        .isFalse();
            }
        }
    }
}
