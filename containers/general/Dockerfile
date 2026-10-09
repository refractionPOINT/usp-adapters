FROM golang:1.27.1-bookworm AS builder

ADD . .
ENV GOPATH=/
RUN go build -v ./containers/general/ && cp general / && chmod +x /general

FROM gcr.io/distroless/base:latest

WORKDIR /
COPY --from=builder /general /

ENTRYPOINT [ "/general" ]