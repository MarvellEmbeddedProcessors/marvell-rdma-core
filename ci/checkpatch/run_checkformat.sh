#!/bin/bash
# SPDX-License-Identifier: Marvell-MIT
# Copyright (c) 2025 Marvell.

PROJECT_ROOT=${PROJECT_ROOT:-$PWD}
cd $PROJECT_ROOT
git format-patch -n1 -s -q
./ci/checkpatch/checkformat.sh 0001*
