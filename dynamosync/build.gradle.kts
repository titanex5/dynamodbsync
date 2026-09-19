plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "pl.twojserwer"
version = "1.0.0"

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    // Nowy schemat wersjonowania Paper dla MC 26.x: zakres [26.2.build,) sam
    // lapie najnowszy dostepny build.
    compileOnly("io.papermc.paper:paper-api:[26.2.build,)")

    // Klient DynamoDB - wykluczamy domyslny transport Netty, uzywamy lekkiego
    // url-connection-client, zeby nie napuchac shaded jara.
    implementation("software.amazon.awssdk:dynamodb:2.28.11") {
        exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
        exclude(group = "software.amazon.awssdk", module = "apache-client")
    }
    implementation("software.amazon.awssdk:url-connection-client:2.28.11")
}

tasks {
    shadowJar {
        archiveBaseName.set("DynamoSync")
        archiveClassifier.set("")
        archiveVersion.set(project.version.toString())

        relocate("software.amazon.awssdk", "pl.twojserwer.dynamosync.libs.awssdk")
        relocate("org.reactivestreams", "pl.twojserwer.dynamosync.libs.reactivestreams")
        relocate("org.slf4j", "pl.twojserwer.dynamosync.libs.slf4j")

        exclude("module-info.class")
        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    }

    build {
        dependsOn(shadowJar)
    }

    compileJava {
        options.encoding = "UTF-8"
    }

    processResources {
        filteringCharset = "UTF-8"
    }
}
