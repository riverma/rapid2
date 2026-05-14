#!/usr/bin/env -S bash --login
set -euo pipefail
# Entry point for MAAP DPS execution of RAPID2.
# DPS downloads all file inputs into ./input/ and passes positional args
# for non-file parameters.
#
# DPS file inputs (in registration order):
#   1. Qex_ncf  - external inflow NetCDF
#   2. Q00_ncf  - initial discharge NetCDF
#   3. con_csv  - river connectivity CSV
#   4. kpr_csv  - Muskingum k parameter CSV
#   5. xpr_csv  - Muskingum x parameter CSV
#   6. bas_csv  - basin identifier CSV
#
# DPS positional inputs (in registration order):
#   $1  IS_dtR  - routing time step in seconds (e.g. 900)

basedir=$(dirname "$(readlink -f "$0")")

IS_dtR="$1"

# Set up working dirs expected by the namelist
mkdir -p input output

# Stage DPS-downloaded inputs (DPS puts them flat in ./input/)
Qex_ncf=$(ls -d input/*.nc4 | sed -n '1p')
Q00_ncf=$(ls -d input/*.nc4 | sed -n '2p')
con_csv=$(ls -d input/*.csv | grep -i connect)
kpr_csv=$(ls -d input/*.csv | grep -iE "(^|/)k_|_k_")
xpr_csv=$(ls -d input/*.csv | grep -iE "(^|/)x_|_x_")
bas_csv=$(ls -d input/*.csv | grep -i riv_bas)

# Generate a namelist pointing at the staged input files and output dir
cat > namelist_maap.yml <<EOF
Qex_ncf: '${Qex_ncf}'
Q00_ncf: '${Q00_ncf}'
con_csv: '${con_csv}'
kpr_csv: '${kpr_csv}'
xpr_csv: '${xpr_csv}'
bas_csv: '${bas_csv}'
IS_dtR: ${IS_dtR}
Qou_ncf: 'output/Qout_maap.nc4'
Qfi_ncf: 'output/Qfinal_maap.nc4'
EOF

conda run --live-stream --name rapid2 rapid2 --namelist namelist_maap.yml
