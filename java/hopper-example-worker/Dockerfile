# syntax=docker/dockerfile:1
# hopper-example-worker: the Java worker SDK serving cmd/worker's three queues,
# the Java side of the Phase 3c conformance runs. Build from java/:
#   docker build -t hopper-example-worker:dev -f java/hopper-example-worker/Dockerfile java
FROM eclipse-temurin:25.0.5_8-jdk-noble@sha256:3c35c595580ae3342eb34e5832e8e516ded8e767efa4a5fe61b8644d44847b8e AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
COPY hopper-proto hopper-proto
COPY hopper-client hopper-client
COPY hopper-worker hopper-worker
COPY hopper-example-worker hopper-example-worker
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :hopper-example-worker:installDist

FROM eclipse-temurin:25.0.5_8-jre-noble@sha256:23481dc33a759b79d62814ceb1ec27a0d1a84da9d7aab0609577427176fbb1b7
COPY --from=build /src/hopper-example-worker/build/install/hopper-example-worker /app
# Numeric, as in deploy/docker/Dockerfile: a kubelet enforcing runAsNonRoot can
# verify a uid but not a name.
USER 65532:65532
# The start script execs java, so the JVM is PID 1 and receives SIGTERM itself.
ENTRYPOINT ["/app/bin/hopper-example-worker"]
