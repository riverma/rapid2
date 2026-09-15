#!/usr/bin/env -S bash --login
set -euo pipefail
# Entry point for MAAP DPS execution of RAPID2.
# DPS downloads all file inputs into ./input/ and passes positional args
# for non-file parameters.
#
# DPS file inputs (in registration order):
#   1. Qex_ncf  - external inflow NetCDF
#   2. Q00_ncf  - initial discharge NetCDF
#   3. con_pqt  - river connectivity Parquet
#   4. kpr_pqt  - Muskingum k parameter Parquet
#   5. xpr_pqt  - Muskingum x parameter Parquet
#   6. bas_pqt  - basin identifier Parquet
#
# DPS positional inputs (in registration order):
#   $1  IS_dtR  - routing time step in seconds (e.g. 900)
#
# Note: rapid2 >= 2.0.0b2 reads static inputs as Parquet (pyarrow) and the
# namelist uses *_pqt keys. Static Parquet files come from Zenodo record
# 20672740 (MERIT Basins v1.0 / GLDAS v2.0). con/kpr/xpr/bas are consumed here;
# crd/cpl are only needed by cpllsm (run locally in the notebook), so they are
# not DPS inputs.

basedir=$(dirname "$(readlink -f "$0")")

IS_dtR="$1"

# Set up working dirs expected by the namelist
mkdir -p input output

# Stage DPS-downloaded inputs (DPS puts them flat in ./input/).
# Match by filename so month-to-month chaining works: the initial-state input
# is a cold-start "Qinit_*" for chunk 0 but a prior chunk's "Qfinal_*" after
# that, so Q00 is simply "the NetCDF that is not the Qext external inflow".
Qex_ncf=$(ls -d input/*.nc4 | grep -iE '/Qex' | sed -n '1p')
Q00_ncf=$(ls -d input/*.nc4 | grep -ivE '/Qex' | sed -n '1p')
con_pqt=$(ls -d input/*.parquet | grep -iE '(^|/)con_')
kpr_pqt=$(ls -d input/*.parquet | grep -iE '(^|/)kpr_')
xpr_pqt=$(ls -d input/*.parquet | grep -iE '(^|/)xpr_')
bas_pqt=$(ls -d input/*.parquet | grep -iE '(^|/)bas_')

# Generate a namelist pointing at the staged input files and output dir
cat > namelist_maap.yml <<EOF
Qex_ncf: '${Qex_ncf}'
Q00_ncf: '${Q00_ncf}'
con_pqt: '${con_pqt}'
kpr_pqt: '${kpr_pqt}'
xpr_pqt: '${xpr_pqt}'
bas_pqt: '${bas_pqt}'
IS_dtR: ${IS_dtR}
Qou_ncf: 'output/Qout_maap.nc4'
Qfi_ncf: 'output/Qfinal_maap.nc4'
EOF

conda run --live-stream --name rapid2 rapid2 --namelist namelist_maap.yml
