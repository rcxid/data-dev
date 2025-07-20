import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.hive:hive-exec:3.1.3")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

val buildTime: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"))

tasks.shadowJar {
    relocate("org.example.udf", "v${buildTime}.org.example.udf")
    relocate("org.example.util", "v${buildTime}.org.example.util")
}

tasks.test {
    useJUnitPlatform()
}