FROM golang:alpine as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN go build -o proxy main.go

FROM alpine:3.15.3
COPY --from=builder /build/proxy /app/proxy
WORKDIR /app
EXPOSE 6666
CMD ["./proxy"]
