plugins {
  kotlin("jvm") version "1.9.21"
  id("application")
}

repositories {
  mavenCentral()
}

dependencies {
  implementation("aws.sdk.kotlin:s3:1.0.19")
  implementation("org.testcontainers:minio:1.19.3")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
  implementation("ch.qos.logback:logback-classic:1.4.14")
}

application {
  mainClass.set("de.felixscheinost.MainKt")
}