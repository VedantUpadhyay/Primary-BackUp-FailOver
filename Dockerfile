FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o webserver webserver.go

FROM golang:latest

WORKDIR /app

COPY --from=builder /app/webserver .

EXPOSE 8081:8081

CMD ["./webserver"]
