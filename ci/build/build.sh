#!/bin/bash
# SPDX-License-Identifier: Marvell-MIT
# Copyright (c) 2025 Marvell.
#
# Script will build rdma-core package in <build-root>/build and install in <build-root>/prefix
#

set -euo pipefail

PROJECT_ROOT=$PWD
BUILD_DIR=build

cd $PROJECT_ROOT

# Clean up previous compilation
rm -rf $BUILD_DIR

# Build for Host
bash build.sh
