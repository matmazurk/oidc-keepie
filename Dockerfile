FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /keepie ./cmd/keepie

FROM alpine:3.21

COPY --from=build /keepie /keepie
ENTRYPOINT ["/keepie"]
