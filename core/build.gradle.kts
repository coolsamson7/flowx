plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    kotlin("plugin.jpa")
    `java-library`
    id("io.spring.dependency-management")
}

dependencies {
    // Spring
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    // Jackson
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    // Redis (optional — only needed for RedisSagaLock / RedisTimeoutQueue)
    implementation("org.springframework.boot:spring-boot-starter-data-redis")

    testImplementation("io.mockk:mockk:1.13.10")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
repositories {
    mavenCentral()
}