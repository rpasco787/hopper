plugins {
    `java-library`
    `java-test-fixtures`
}

dependencies {
    api(platform(libs.grpc.bom))
    api(libs.grpc.api)
    api(libs.grpc.stub)
    api(libs.grpc.protobuf)
    // grpc-protobuf declares protobuf-java 3.25.9; the stubs need 4.36.2.
    api(libs.protobuf.java)
    // The transport behind ChannelDefaults.open, found by ServiceLoader.
    runtimeOnly(libs.grpc.netty.shaded)

    testFixturesApi(platform(libs.grpc.bom))
    testFixturesApi(libs.grpc.inprocess)
    // FakeBroker.startOnLoopback binds a Netty server to 127.0.0.1 only.
    testFixturesImplementation(libs.grpc.netty.shaded)
    testFixturesApi(platform(libs.junit.bom))
    testFixturesApi(libs.junit.jupiter)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
