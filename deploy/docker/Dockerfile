FROM golang:1.27.1-alpine AS build

ARG BINARY=broker
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The build cache outlives this stage, so building broker, worker and hopperctl
# one after another compiles the shared packages once.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${BINARY}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
# Numeric on purpose. A kubelet enforcing runAsNonRoot can verify a uid but not
# a name, and refuses "nonroot:nonroot" as unverifiable. 65532 is the
# distroless nonroot user, so the running identity is unchanged.
USER 65532:65532
ENTRYPOINT ["/app"]
