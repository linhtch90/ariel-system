package io.ariel.ledger;

import io.ariel.outbox.OutboxEventEntity;
import io.ariel.outbox.OutboxEventRepository;
import io.ariel.outbox.OutboxRelayWorker;
import io.ariel.outbox.TransactionalOutboxPublisher;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.json.JsonMapper;

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackageClasses = {LedgerServiceApplication.class, OutboxEventEntity.class})
@EnableJpaRepositories(basePackageClasses = {LedgerServiceApplication.class, OutboxEventRepository.class})
public class LedgerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
    }

    @Bean
    public TransactionalOutboxPublisher transactionalOutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            JsonMapper jsonMapper
    ) {
        return new TransactionalOutboxPublisher(outboxEventRepository, jsonMapper);
    }

    @Bean
    public OutboxRelayWorker outboxRelayWorker(
            OutboxEventRepository outboxEventRepository,
            ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider
    ) {
        return new OutboxRelayWorker(outboxEventRepository, kafkaTemplateProvider);
    }
}
