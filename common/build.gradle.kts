// common/build.gradle.kts
plugins {
    kotlin("jvm")
    `java-library`
    //id("io.spring.dependency-management")
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    //api("org.springframework.context")
}