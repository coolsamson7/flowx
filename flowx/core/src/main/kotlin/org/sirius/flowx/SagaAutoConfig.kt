package org.sirius.flowx

import org.sirius.flowx.storage.JpaSagaStorage
import org.sirius.flowx.storage.SagaEntityRepository
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SagaAutoConfiguration {

    @ConditionalOnMissingBean(SagaLock::class)
    @Bean fun sagaLock() = LocalSagaLock()

    @Bean
    @ConditionalOnMissingBean(SagaStorage::class)
    fun sagaStorage(
        sagaRepo: SagaEntityRepository,
        registry: SagaRegistry
    ): SagaStorage = JpaSagaStorage(sagaRepo, registry)

    @Bean
    @ConditionalOnMissingBean(SagaEventStore::class)
    fun sagaEventStore(
        eventRepo: SagaEventEntityRepository
    ): SagaEventStore = JpaSagaEventStore(eventRepo)
}