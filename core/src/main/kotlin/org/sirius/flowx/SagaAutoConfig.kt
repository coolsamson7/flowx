package org.sirius.flowx
/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import org.sirius.flowx.storage.JpaSagaStorage
import org.sirius.flowx.storage.SagaEntityRepository
import org.sirius.flowx.storage.SagaStepEntityRepository
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SagaAutoConfiguration {
    @ConditionalOnMissingBean(SagaLock::class)
    @Bean fun sagaLock() = LocalSagaLock()

    @ConditionalOnMissingBean(TimeoutQueue::class)
    @Bean fun timeoutQueue() = LocalTimeoutQueue()

    // JPA storage — default when JPA repos are on the classpath
    @Bean
    @ConditionalOnMissingBean(SagaStorage::class)
    fun sagaStorage(
        sagaRepo: SagaEntityRepository,
        stepRepo: SagaStepEntityRepository,
        registry: SagaRegistry
    ): SagaStorage = JpaSagaStorage(sagaRepo, stepRepo, registry)

    // JPA event store — default
    @Bean
    @ConditionalOnMissingBean(SagaEventStore::class)
    fun sagaEventStore(
        eventRepo: SagaEventEntityRepository
    ): SagaEventStore = JpaSagaEventStore(eventRepo)
}