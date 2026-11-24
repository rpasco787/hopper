plugins {
    `java-library`
}

dependencies {
    api(project(":hopper-proto"))

    testImplementation(testFixtures(project(":hopper-proto")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// The tests tagged "compose" run only here, against a real broker
// (scripts/java-smoke.sh passes -PsmokeBroker and -PsmokeOut).
tasks.register<Test>("composeTest") {
    description = "Runs the compose-tagged smoke tests against -PsmokeBroker."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform { includeTags("compose") }
    systemProperty("hopper.smoke.broker", providers.gradleProperty("smokeBroker").getOrElse(""))
    systemProperty("hopper.smoke.out", providers.gradleProperty("smokeOut").getOrElse(""))
    systemProperty("junit.jupiter.execution.timeout.default", "120 s")
    outputs.upToDateWhen { false }
}
