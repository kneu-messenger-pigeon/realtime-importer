ARG GO_VERSION=${GO_VERSION:-1.19}

FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:${GO_VERSION}-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
ARG BUILDPLATFORM

RUN apk update && apk add --no-cache git

WORKDIR /src

COPY ./go.mod ./go.sum ./
RUN go mod download

COPY . .

RUN cat /etc/passwd | grep nobody > /etc/passwd.nobody

# Build the binary.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -tags=nomsgpack -o /app .

# build a small image
FROM alpine

ENV TZ=Europe/Kyiv
RUN apk add tzdata

ENV STORAGE_DIR /storage
RUN mkdir /storage && chmod 777 -R /storage

COPY --from=builder /etc/passwd.nobody /etc/passwd
COPY --from=builder /app /app

# Run
USER nobody
ENTRYPOINT ["/app"]
