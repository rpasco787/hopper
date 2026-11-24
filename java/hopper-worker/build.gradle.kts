plugins {
    `java-library`
}

dependencies {
    api(project(":hopper-proto"))

    testImplementation(testFixtures(project(":hopper-proto")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    // Only to parse the JSON log in JsonLinesLogTest; the SDK itself has no JSON dependency.
    testImplementation(libs.jackson.databind)
    testRuntimeOnly(libs.junit.platform.launcher)
}
