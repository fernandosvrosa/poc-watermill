# Stage 1: build do binário
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copia os arquivos de dependências primeiro para aproveitar o cache de camadas
COPY go.mod go.sum ./
RUN go mod download

# Copia o restante do código-fonte
COPY . .

# Compila o binário a partir do entry point em cmd/server
RUN go build -o /app ./cmd/server

# Stage 2: imagem final mínima
FROM alpine:latest

WORKDIR /

# Copia apenas o binário compilado
COPY --from=builder /app /app

EXPOSE 8090

CMD ["/app"]
