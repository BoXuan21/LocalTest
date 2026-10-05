#!/usr/bin/env bash
# Delete the k3d cluster and everything in it.
set -euo pipefail
k3d cluster delete metrics-stack
