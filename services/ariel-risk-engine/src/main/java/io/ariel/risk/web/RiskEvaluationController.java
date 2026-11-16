package io.ariel.risk.web;

import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import io.ariel.risk.service.ParallelRiskEvaluationService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/risk")
public class RiskEvaluationController {

    private final ParallelRiskEvaluationService evaluationService;

    public RiskEvaluationController(ParallelRiskEvaluationService evaluationService) {
        this.evaluationService = evaluationService;
    }

    @PostMapping("/evaluate")
    public RiskDecision evaluate(@Valid @RequestBody RiskEvaluationRequest request) {
        return evaluationService.evaluate(request);
    }
}
