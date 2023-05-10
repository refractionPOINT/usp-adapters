FROM golang:1.20 AS builder

ADD . .
ENV GOPATH=/
RUN go build -v ./containers/general/ && cp general / && chmod +x /general

FROM gcr.io/distroless/base:latest

WORKDIR /
COPY --from=builder /general /

ENTRYPOINT [ "/general" ]