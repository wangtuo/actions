# 多阶段镜像：静态编译 -> 精简 distroless 运行时
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/hello ./cmd/hello

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hello /hello
USER nonroot:nonroot
ENTRYPOINT ["/hello"]
