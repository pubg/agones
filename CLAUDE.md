# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Agones is a library for hosting, running and scaling dedicated game servers on Kubernetes. It provides native Kubernetes capabilities to create, run, manage and scale dedicated game server processes using standard Kubernetes APIs and tooling.

## Key Architecture

### Core Components

- **Controller**: Main Agones controller managing GameServer lifecycle
- **Extensions**: Extensions server for allocation endpoints
- **SDK Server**: Sidecar container providing SDK gRPC/REST APIs to game servers
- **Allocator**: Service for allocating GameServers from Fleets
- **Processor**: Binary processor for game server operations

### Custom Resources

- **GameServer**: Single dedicated game server instance
- **Fleet**: Collection of GameServer replicas
- **GameServerSet**: Manages replicas for a Fleet
- **FleetAutoscaler**: Autoscaling policies for Fleets
- **GameServerAllocation**: Request to allocate a GameServer

## Development Commands

### Building

```bash
# Build everything (images + SDKs)
cd build && make build

# Build controller and images
make build-images

# Build specific component
make build-controller-image
make build-agones-sdk-image

# Build all SDKs
make build-sdks

# Build specific SDK
make build-sdk SDK_FOLDER=go
```

### Testing

```bash
# Run all tests
cd build && make test

# Run Go tests only
make test-go

# Run specific package tests
make test-go ARGS="-run TestName agones.dev/agones/pkg/..."

# Run end-to-end tests
make test-e2e

# Run integration tests with specific features
make test-e2e-integration FEATURE_GATES="PlayerTracking=true"
```

### Code Quality

```bash
# Run linter (15min timeout by default)
cd build && make lint

# Run with custom timeout
make lint LINT_TIMEOUT=30m
```

### Local Development

```bash
# Install Agones to current cluster
cd build && make install FEATURE_GATES="PlayerTracking=true"

# Uninstall Agones
make uninstall

# Run development shell
make shell

# Generate CRD code after API changes
make gen-crd-code

# Generate protobuf/gRPC code
make gen-allocation-grpc
make gen-all-sdk-grpc
```

### Working with Clusters

#### Minikube

```bash
# Create test cluster
cd build && make minikube-test-cluster

# Push images to minikube
make minikube-push

# Install Agones
make minikube-install
```

#### Kind

```bash
# Create test cluster
cd build && make kind-test-cluster

# Push images to kind
make kind-push

# Install Agones
make kind-install
```

#### GKE

```bash
# Initialize gcloud
cd build && make gcloud-init

# Create test cluster
make gcloud-test-cluster

# Authenticate to cluster
make gcloud-auth-cluster
```

## Project Structure

- `/cmd/` - Entry points for binaries (controller, allocator, sdk-server, etc.)
- `/pkg/` - Core library code
  - `/apis/` - CRD API definitions
  - `/client/` - Generated Kubernetes clients
  - `/gameservers/` - GameServer controller logic
  - `/fleets/` - Fleet controller logic
  - `/sdkserver/` - SDK server implementation
- `/sdks/` - Client SDKs (Go, Rust, C++, Node.js, C#, Unity)
- `/proto/` - Protocol buffer definitions
- `/test/` - Test suites (e2e, load, upgrade tests)
- `/build/` - Build system and Dockerfiles
- `/install/` - Installation manifests (Helm charts, YAML)
- `/examples/` - Example game servers

## Working with Protobufs

The project uses protobuf for SDK communication. After modifying `.proto` files:

```bash
# Regenerate all SDK gRPC code
cd build && make gen-all-sdk-grpc

# Regenerate specific SDK
make gen-sdk-grpc SDK_FOLDER=go
```

## Feature Gates

Control experimental features via FEATURE_GATES environment variable:

- Beta features: `AutopilotPassthroughPort`, `CountsAndLists`, `PortRanges`, etc.
- Alpha features: `PlayerTracking`, `PlayerAllocationFilter`, `SidecarContainers`

Example:

```bash
make install FEATURE_GATES="PlayerTracking=true,CountsAndLists=true"
```

## Environment Variables

Key variables used in Makefile:

- `VERSION`: Image version tag (defaults to dev build)
- `REGISTRY`: Docker registry for images
- `KUBECONFIG`: Kubernetes config file location
- `GO_BUILD_TAGS`: Build tags for conditional compilation
- `FEATURE_GATES`: Enable/disable features
