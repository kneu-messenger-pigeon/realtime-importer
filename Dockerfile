ARG GO_VERSION=${GO_VERSION:-1.19}

FROM golang:${GO_VERSION}-alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR /src

COPY ./go.mod ./go.sum ./
RUN go mod download

COPY . .

RUN cat /etc/passwd | grep nobody > /etc/passwd.nobody

# Build the binary.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -tags=nomsgpack -o /app .

# build a small image
FROM alpine

ENV TZ=Europe/Kyiv
RUN apk add tzdata

ENV STORAGE_DIR /storage
RUN mkdir /storage && touch /storage/storage.txt && chmod 777 -R /storage/storage.txt

COPY --from=builder /etc/passwd.nobody /etc/passwd
COPY --from=builder /app /app

# Run
USER nobody
ENTRYPOINT ["/app"]
