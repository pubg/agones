# Copyright 2018 Google LLC All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM --platform=$BUILDPLATFORM golang:1.25 AS builder

WORKDIR /src
COPY . .

RUN bash build/extract-licenses.sh && bash build/build-required-src-dist.sh

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG GO_BUILD_TAGS=none
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -mod=vendor -trimpath -tags "$GO_BUILD_TAGS" -ldflags "-s -w -X agones.dev/agones/pkg.Version=$VERSION" -o /controller ./cmd/controller

FROM gcr.io/distroless/static-debian13:nonroot

WORKDIR /
COPY --from=builder /controller /controller
COPY --from=builder /tmp/LICENSES .
COPY --from=builder /tmp/dependencies-src.tgz .

USER nonroot:nonroot
ENTRYPOINT ["/controller"]
