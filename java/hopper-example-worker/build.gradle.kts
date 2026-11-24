plugins {
    application
}

dependencies {
    implementation(project(":hopper-worker"))
    implementation(libs.jackson.databind)

    testImplementation(testFixtures(project(":hopper-proto")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("io.github.rpasco787.hopper.example.Main")
    applicationName = "hopper-example-worker"
    applicationDefaultJvmArgs = listOf(
        "-XX:MaxRAMPercentage=75",
        "-XX:+ExitOnOutOfMemoryError",
        // Without these, Netty makes the JDK print warnings to stderr, which
        // test/accounting counts as unparsed lines (plan §6).
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
    )
}
