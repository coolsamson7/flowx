plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    api(project(":flowx:core"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    //implementation("org.jetbrains.kotlin:kotlin-reflect")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    runtimeOnly("com.h2database:h2")  // ← add this

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
repositories {
    mavenCentral()
}