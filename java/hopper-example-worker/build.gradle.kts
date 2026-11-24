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

tasks.named<Test>("test") {
    // MainProcessTest starts the worker as a JVM of its own, with the start
    // script's JVM flags, to see the exit code a signal leaves.
    val runtime = sourceSets["test"].runtimeClasspath
    val flags = application.applicationDefaultJvmArgs
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-Dhopper.example.classpath=${runtime.asPath}",
            "-Dhopper.example.jvmArgs=${flags.joinToString(" ")}",
        )
    })
}
