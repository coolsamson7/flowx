// flowx/redis/build.gradle.kts
plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    `java-library`
    id("io.spring.dependency-management")
}

kotlin {
    jvmToolchain(17)   // ← add this
}

dependencies {
    api(project(":flowx:core"))
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
}