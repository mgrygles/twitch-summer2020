plugins {
  java
}

repositories {
    mavenCentral()
}

dependencies {
   implementation("io.vertx:vertx-core:3.9.2")
    implementation("io.vertx:vertx-config:3.9.2")
    implementation("io.vertx:vertx-rx-java2:3.9.2")
   implementation("ch.qos.logback:logback-classic:1.2.3")

    compile("io.vertx:vertx-rx-java2:3.9.2")
}

tasks.create<JavaExec>("run") {
    group="Wednesday Twitch on #Java"
    description="Vert.x Verticle examples"
    main = project.properties.getOrDefault("mainClass","EmptyVerticle") as String
    classpath = sourceSets["main"].runtimeClasspath
    systemProperties["vertx.logger-delegate-factory-class-name"] = "io.vertx.core.logging.SLF4JLogDelegateFactory"
}

java {
   sourceCompatibility = JavaVersion.VERSION_1_8
}
