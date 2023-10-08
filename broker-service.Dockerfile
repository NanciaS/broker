# base go image
FROM golang:1.21-alpine as builder

WORKDIR /app

COPY . /app/

RUN CGO_ENABLED=0 go build -o broker ./cmd/api

RUN chmod +x /app/broker

# build a tiny docker image
FROM alpine:latest 

RUN mkdir /app

COPY --from=builder /app/broker /app

CMD ["./app/broker"]