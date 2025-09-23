# ---------- Stage 1: Build ----------
FROM golang:1.24 AS builder

WORKDIR /workspace

# Cache go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o bastion-backup ./cmd/controller/main.go

# Build apiserver binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o custom-api-server cmd/apiserver/main.go

# ---------- Stage 2: Controller Run ----------
FROM mcr.microsoft.com/cbl-mariner/distroless/base:2.0 AS controller

WORKDIR /

COPY --from=builder /workspace/bastion-backup .

# No user in distroless, just run it — best if binary is not privileged.
ENTRYPOINT ["/bastion-backup"]


# ---------- Stage 2: API Server Run ----------
FROM mcr.microsoft.com/cbl-mariner/distroless/base:2.0 AS apiserver

WORKDIR /

COPY --from=builder /workspace/custom-api-server .

# No user in distroless, just run it — best if binary is not privileged.
ENTRYPOINT ["/custom-api-server"]
