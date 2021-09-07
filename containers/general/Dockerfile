FROM golang:1.16 AS builder

ADD . .
ENV GOPATH=/
RUN go build -v ./containers/general/

FROM gcr.io/distroless/static

COPY --from=builder ./general /

ENTRYPOINT [ "general" ]