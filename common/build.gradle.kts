// common/build.gradle.kts
plugins {
    kotlin("jvm")
    `java-library`
    //id("io.spring.dependency-management")
}

kotlin {
    jvmToolchain(17)   // ← add this
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    //api("org.springframework.context")
}