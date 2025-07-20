plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // dataStream api
    implementation("org.apache.flink:flink-streaming-java:1.20.2")
    implementation("org.apache.flink:flink-clients:1.20.2")
    // table api
    implementation("org.apache.flink:flink-table-api-java:1.20.2")
    implementation("org.apache.flink:flink-table-api-java-bridge:1.20.2")
    implementation("org.apache.flink:flink-table-planner_2.12:1.20.2")
    implementation("org.apache.flink:flink-table-common:1.20.2")
    // 第三方 table connector redis
    implementation("io.github.jeff-zou:flink-connector-redis:1.4.3")
    implementation("io.lettuce:lettuce-core:6.2.1.RELEASE")
    // connector
    implementation("org.apache.flink:flink-connector-files:1.20.2")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    // user custom connector
    implementation(project(":flink-connector-redis-feature"))
    // paimon
    implementation("org.apache.paimon:paimon-flink-1.20:1.0.1")
    // hadoop
    implementation("org.apache.hadoop:hadoop-client:3.3.4")
    // test
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}