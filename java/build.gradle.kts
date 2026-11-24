import org.gradle.api.tasks.testing.logging.TestExceptionFormat

// Conventions every module shares (docs/design/java-worker-sdk.md). Versions
// live in gradle/libs.versions.toml; nothing here names one.
subprojects {
    group = "io.github.rpasco787"
    version = "0.1.0-SNAPSHOT"

    plugins.withType<JavaPlugin> {
        configurations.configureEach {
            resolutionStrategy.failOnNonReproducibleResolution()
        }
        tasks.withType<JavaCompile>().configureEach {
            options.release.set(25)
            options.encoding = "UTF-8"
            // The generated stubs (hopper-proto's main source set) are exempt.
            val generated = project.name == "hopper-proto" && name == "compileJava"
            if (!generated) {
                options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
            }
        }
        tasks.withType<Test>().configureEach {
            maxHeapSize = "512m"
            // Netty on JDK 24+ warns on stderr without these (plan §6).
            jvmArgs("--sun-misc-unsafe-memory-access=allow", "--enable-native-access=ALL-UNNAMED")
            // A stuck test fails with its name instead of hanging the build,
            // like the Go fake's barrier.
            systemProperty("junit.jupiter.execution.timeout.default", "30 s")
            testLogging {
                events("failed")
                exceptionFormat = TestExceptionFormat.FULL
            }
            val module = project.path
            afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ suite, result ->
                if (suite.parent == null) {
                    println("RESULT $module ${result.resultType} tests=${result.testCount} " +
                        "passed=${result.successfulTestCount} failed=${result.failedTestCount} " +
                        "skipped=${result.skippedTestCount}")
                }
            }))
        }
        tasks.named<Test>("test") {
            useJUnitPlatform { excludeTags("compose") }
        }
        tasks.withType<Jar>().configureEach {
            manifest {
                attributes(
                    "Automatic-Module-Name" to "io.github.rpasco787.hopper." +
                        project.name.removePrefix("hopper-").replace('-', '.'),
                )
                // No other jar may add classes to these packages, so the
                // package-private test seams stay private (Decision D18).
                // hopper-proto is the exception: its own tests live in its
                // packages and see its jars through the test fixtures, and a
                // sealed package can't be defined from two code sources. It
                // holds no seam D18 protects (Decision D35).
                if (project.name != "hopper-proto") {
                    attributes("Sealed" to "true")
                }
            }
        }
    }
}
