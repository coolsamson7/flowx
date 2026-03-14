package org.sirius.flowx

import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.jpa.repository.config.EnableJpaRepositories

@Configuration
@ComponentScan(basePackages = ["org.sirius.flowx"])
@EntityScan(basePackages = ["org.sirius.flowx"])
@EnableJpaRepositories(basePackages = ["org.sirius.flowx"])
class FlowxConfiguration