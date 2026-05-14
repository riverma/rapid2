#!/usr/bin/env -S bash --login
set -euo pipefail
# Installs RAPID2 and its dependencies for MAAP DPS.

basedir=$( cd "$(dirname "$0")" ; pwd -P )

conda env update -f "${basedir}/environment.yml"
