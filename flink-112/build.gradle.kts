plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.flink:flink-java:1.12.7")
    compileOnly("org.apache.flink:flink-streaming-java_2.12:1.12.7")
    compileOnly("org.apache.flink:flink-streaming-scala_2.12:1.12.7")
    compileOnly("org.apache.flink:flink-clients_2.12:1.12.7")
    compileOnly("org.apache.flink:flink-runtime-web_2.12:1.12.7")
//    compileOnly("org.apache.flink:flink-table-planner_2.12:1.12.7")
    compileOnly("org.apache.flink:flink-table-planner-blink_2.12:1.12.7")
//    compileOnly("org.apache.flink:flink-table-api-java-bridge_2.12:1.12.7")
    implementation("org.apache.flink:flink-csv:1.12.7")
    implementation("org.apache.flink:flink-json:1.12.7")
    implementation("org.apache.flink:flink-cep_2.12:1.12.7")
    implementation("org.apache.flink:flink-connector-kafka_2.12:1.12.7")
    implementation("org.apache.flink:flink-connector-jdbc_2.12:1.12.7")
    implementation("org.apache.flink:flink-connector-files:1.12.7")
    implementation("org.apache.flink:flink-connector-elasticsearch6_2.12:1.12.7")
    implementation("org.apache.bahir:flink-connector-redis_2.12:1.1.0")
//    compileOnly("org.apache.hadoop:hadoop-client:3.1.4")
//    implementation("mysql:mysql-connector-java:8.0.23")
//    <slf4j.version>1.7.30</slf4j.version>
//    compileOnly("org.slf4j:slf4j-api:${slf4j.version}")
//    implementation("org.slf4j:slf4j-log4j12:${slf4j.version}")
//    implementation("org.apache.logging.log4j:log4j-to-slf4j:2.13.3")
    implementation("com.alibaba:fastjson:2.0.57")
    implementation("org.projectlombok:lombok:1.18.38")


    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}