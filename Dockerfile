# syntax=docker/dockerfile:1

# The build stage runs natively and cross-compiles (CGO off: pure Go SQLite),
# so multi-arch images build without emulating Go under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/taskiq-admin ./cmd/taskiq-admin \
 && mkdir -p /out/database

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/taskiq-admin /taskiq-admin
COPY --from=build --chown=nonroot:nonroot /out/database /usr/database
ENV HOST=0.0.0.0 \
    PORT=3000 \
    TASKIQ_ADMIN_DB_DRIVER=sqlite \
    TASKIQ_ADMIN_DB_FILE_PATH=/usr/database/database.db
VOLUME /usr/database
EXPOSE 3000
# distroless has no shell or curl: the binary probes itself. Change the port
# with PORT so the probe sees it too.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --start-interval=2s --retries=3 \
  CMD ["/taskiq-admin", "healthcheck"]
ENTRYPOINT ["/taskiq-admin"]
