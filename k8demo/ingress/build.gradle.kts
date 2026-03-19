plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
    id("com.google.cloud.tools.jib") version "3.4.4"
}

kotlin {
    jvmToolchain(17)   // always compile to Java 21 regardless of Gradle JVM
}

dependencies {
    api(project(":flowx:core"))

    implementation("org.springframework.boot:spring-boot-starter")
    //implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

jib {
    from {
        image = "eclipse-temurin:21-jre"
    }
    to {
        image = "ghcr.io/coolsamson7/ingress"
    }
    container {
        ports = listOf("8080")
    }
}