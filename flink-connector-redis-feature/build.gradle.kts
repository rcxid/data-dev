plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.flink:flink-table-common:1.20.1")
    implementation("org.apache.flink:flink-table-api-java-bridge:1.20.1")
    implementation("io.lettuce:lettuce-core:6.2.1.RELEASE")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}
