package org.sirius.flowx

import org.springframework.beans.factory.config.AutowireCapableBeanFactory
import org.springframework.stereotype.Component

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */
@Component
class SagaFactory(
    private val beanFactory: AutowireCapableBeanFactory
) {

    @PublishedApi
    internal fun <T : Saga<*>> createInternal(
        clazz: Class<T>,
        init: T.() -> Unit = {}
    ): T {
        val bean = beanFactory.createBean(clazz)
        init(bean)
        return bean
    }

    final inline fun <reified T : Saga<*>> create(
        clazz: Class<T>,
        noinline init: T.() -> Unit = {}
    ): T = createInternal(clazz, init)
}