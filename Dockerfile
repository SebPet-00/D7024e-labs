FROM golang:1.25.4-alpine AS build
WORKDIR /build
COPY go.mod ./
COPY main.go shell.go ./
COPY src ./src
RUN CGO_ENABLED=0 go build -trimpath -o /kadlab .

FROM alpine:3.22
RUN adduser -D -u 10001 kadlab
COPY --from=build /kadlab /usr/local/bin/kadlab
USER kadlab
WORKDIR /home/kadlab
EXPOSE 8000/udp
ENTRYPOINT ["kadlab"]
CMD ["-listen", "auto:8000"]
