package io.ariel.outbox;

import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {

    @Query("SELECT o FROM OutboxEventEntity o WHERE o.published = false ORDER BY o.createdAt ASC")
    List<OutboxEventEntity> findUnpublishedBatch(Pageable pageable);
}
