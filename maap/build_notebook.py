#!/usr/bin/env python3
"""Build the multi-month chunked-stream RAPID2 MAAP notebook.

Assembles maap/rapid2_tutorial_maap.ipynb from the cell sources below. Kept as a
builder script so the notebook is reviewable as plain text and regenerable.
Run: python build_notebook.py <output_ipynb_path>
"""
import json
import sys

md = lambda s: {"cell_type": "markdown", "metadata": {}, "source": s.splitlines(keepends=True)}
code = lambda s: {"cell_type": "code", "metadata": {}, "execution_count": None,
                  "outputs": [], "source": s.strip("\n").splitlines(keepends=True)}

cells = []

# ---------------------------------------------------------------------------
cells.append(md("""\
# RAPID2 on MAAP DPS — Multi-Month Chunked-Stream Simulation

This notebook runs **RAPID2 river routing across many basins over many months** on the MAAP
platform. It generalizes the single-month tutorial into a **configurable chunked-stream
orchestrator** that chains months per basin and is designed to scale from a 1-year test to
multi-decade production runs.

This notebook runs on the **MAAP Hub** (the hosted JupyterLab workspace). It does the light
data prep here on the Hub and offloads the heavy routing to MAAP DPS.

## Design in one picture

- **Local (this notebook, on the MAAP Hub):** download GLDAS runoff, couple it to each basin's
  river network (`cpllsm`), and build a cold-start initial state (`zeroqinit`). All forcing is
  pre-generated up front — months, basins, and LSMs have no forcing-side dependency on each other.
- **Remote (MAAP DPS, on EC2 Spot workers):** only the compute-heavy `rapid2` routing runs as a
  DPS job — **one job per (basin, LSM, chunk)**.

## Basins and land-surface models

Which basins and which land-surface models (LSMs) get simulated are both plain arrays in the
Configuration cell below — `BASINS_TO_RUN` and `LSMS_TO_RUN`. Each defaults to a small, cheap
selection (4 basins, 1 LSM); widen either by editing that one line (e.g.
`BASINS_TO_RUN = ALL_BASINS` to run every basin in the dataset). No prompts, no separate config
files — just edit the array.

## Why "chunked streams"

Each **(basin, LSM) pair** is an independent **chronological stream**: month *M+1* is routed with
month *M*'s **final** state as its **initial** state (`Qfinal → Q00`). That handoff, persisted to
S3, is both the physics continuity *and* a **checkpoint** — DPS Spot workers can be reclaimed at
any time, so if a chunk's job dies it simply re-runs from the last `Qfinal`, never recomputing
earlier months. Streams advance **independently and concurrently** — no global per-month barrier
— so small basins (or a faster LSM) race ahead while large ones grind.

Because `rapid2` routes *every* timestep in its external-inflow file and writes a single final
state, a **chunk** is simply "however many months of forcing we put in one `Qext` file."
`CHUNK_MONTHS` is a knob: `1` for this test, larger for production.

**Prerequisites:**
- Algorithm `rapid2` (version `maap`) is registered on MAAP DPS **with Parquet inputs**
  (`con_pqt`/`kpr_pqt`/`xpr_pqt`/`bas_pqt`). Re-register from this branch if needed —
  `run_rapid2.sh` and `algorithm_config.yaml` were updated for the Parquet contract.
- A **NASA Earthdata token** for downloading GLDAS (see the auth cell below). This matches the
  official [Mississippi tutorial](https://github.com/c-h-david/rapid-hub/blob/main/docs/user-guide/examples/tutorial-mississippi.ipynb):
  we authenticate with `earthaccess` and download runoff with the `dgldas2` CLI."""))

# ---------------------------------------------------------------------------
cells.append(md("## Setup"))

cells.append(code("""\
# Pinned to match the registered DPS algorithm and the Parquet static dataset.
# rapid2 >= 2.0.0b2 reads static inputs as Parquet; an unpinned install silently
# broke the CSV-era notebook when the fork synced with upstream.
!pip install -q "rapid2==2.0.0b3" """))

cells.append(code("""\
import os
import time
import zipfile
import urllib.request
from pathlib import Path
from urllib.parse import urlparse
import boto3
from maap.maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")

# This notebook uses the maap-py 4.x job API (submitJob(...) returning a DPSJob
# with .status/.outputs/.retrieve_metrics()), which matches the MAAP Hub and the
# maap_base:v4.2.0 DPS container. maap-py 5.x renamed these (submit_job(...)), so
# fail early with a clear message rather than deep inside the orchestrator.
if not hasattr(MAAP, "submitJob"):
    raise RuntimeError(
        "This notebook targets the maap-py 4.x job API, but the installed maap-py "
        "does not provide submitJob(). On maap-py 5.x, port submitJob/retrieve_* "
        "to submit_job(process_id, inputs, queue). Check `pip show maap-py`."
    )

WORK_DIR = Path("rapid2_multimonth")
INPUT_DIR = WORK_DIR / "input"
OUTPUT_DIR = WORK_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- MAAP user info ---
username = maap.profile.account_info()["username"]
print(f"Username: {username}")

# --- Workspace bucket credentials (reused for upload/download) ---
print("Fetching workspace bucket credentials...")
ws_creds = maap.aws.workspace_bucket_credentials()
creds = ws_creds["credentials"]
s3_ws = boto3.client(
    "s3",
    aws_access_key_id=creds["aws_access_key_id"],
    aws_secret_access_key=creds["aws_secret_access_key"],
    aws_session_token=creds["aws_session_token"],
)
BUCKET = "maap-ops-workspace"
print(f"  Workspace bucket: s3://{BUCKET}/{username}/rapid2/multimonth")
print("Setup complete.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Earthdata authentication (same as the official tutorial)

GLDAS runoff is downloaded with the `dgldas2` CLI, which authenticates to NASA Earthdata via
`earthaccess`. This mirrors the official [Mississippi
tutorial](https://github.com/c-h-david/rapid-hub/blob/main/docs/user-guide/examples/tutorial-mississippi.ipynb):
generate a token at [urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov) under *"Generate
Token"*, run the cell below, and paste it at the prompt. The token is stored in the
`EARTHDATA_TOKEN` environment variable, which the `dgldas2` subprocess calls inherit for
authentication.

You may also need to accept the NASA GES DISC end-user license agreement once. If you have a
`~/.netrc` with Earthdata credentials, `earthaccess` will pick that up and you can skip the
prompt."""))

cells.append(code("""\
import getpass
import earthaccess

# Prefer an existing token in the environment or a ~/.netrc; otherwise prompt.
if not os.environ.get("EARTHDATA_TOKEN"):
    try:
        entered = getpass.getpass("Earthdata token (blank to try ~/.netrc): ").strip()
    except Exception:
        entered = ""
    if entered:
        os.environ["EARTHDATA_TOKEN"] = entered

if os.environ.get("EARTHDATA_TOKEN"):
    auth = earthaccess.login(strategy="environment")
else:
    auth = earthaccess.login(strategy="netrc")

if not auth.authenticated:
    raise RuntimeError(
        "[ERROR] Earthdata login failed. Generate a token at "
        "https://urs.earthdata.nasa.gov under 'Generate Token' and re-run this cell."
    )
print("[OK] - Earthdata authenticated for this session.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Configuration — span, chunking, basins, and LSMs

`CHUNK_MONTHS` is the one knob that turns this from a test into production. The test config
below is **1-month chunks over 1 year** (12 chunks per basin). Set `CHUNK_MONTHS = 3` for
quarterly chunks, `12` for annual, etc.; set a longer `N_MONTHS` (e.g. `360`) for multi-decade
runs. `CHUNKS` is the ordered list of month-groups each stream steps through.

`BASINS_TO_RUN` and `LSMS_TO_RUN` control scope. Both default to a small, cheap selection — edit
either array to widen it. **Running all 61 basins multiplies the job count ~61x**, so treat
`BASINS_TO_RUN = ALL_BASINS` as a deliberate, informed choice, not a default."""))

cells.append(code("""\
from datetime import date

# --- Simulation span ---
START_YEAR, START_MONTH = 2010, 1
N_MONTHS = 12          # test = one year; set 360 for a 30-year production run
CHUNK_MONTHS = 1       # months per DPS job (per chunk). Configurable.

# --- Forcing ---
PHASE = "2.1"          # GLDAS phase (dgldas2 supports 2.0 and 2.1)

# --- Land-surface model selection ---
# ALL_LSMS lists every LSM dgldas2/cpllsm support. LSMS_TO_RUN is what actually
# runs -- edit this line to widen, e.g. LSMS_TO_RUN = ALL_LSMS for all three.
ALL_LSMS = ["VIC", "CLSM", "NOAH"]
LSMS_TO_RUN = ["VIC"]        # default: single LSM

# --- Routing time step (seconds) ---
IS_dtR = "900"

# --- Basin selection ---
# ALL_BASINS lists every basin available in the dataset (61 PFAF level-2 codes,
# from Zenodo record 20672740). BASINS_TO_RUN is what actually gets simulated --
# edit this line to change scope:
#   BASINS_TO_RUN = ALL_BASINS                     # every basin (~61x the jobs -- costly)
#   BASINS_TO_RUN = ["pfaf_74", "pfaf_62"]          # just these basins
ALL_BASINS = ["pfaf_11", "pfaf_12", "pfaf_13", "pfaf_14", "pfaf_15", "pfaf_16",
              "pfaf_17", "pfaf_18",
              "pfaf_21", "pfaf_22", "pfaf_23", "pfaf_24", "pfaf_25", "pfaf_26",
              "pfaf_27", "pfaf_28", "pfaf_29",
              "pfaf_31", "pfaf_32", "pfaf_33", "pfaf_34", "pfaf_35", "pfaf_36",
              "pfaf_41", "pfaf_42", "pfaf_43", "pfaf_44", "pfaf_45", "pfaf_46",
              "pfaf_47", "pfaf_48", "pfaf_49",
              "pfaf_51", "pfaf_52", "pfaf_53", "pfaf_54", "pfaf_55", "pfaf_56",
              "pfaf_57",
              "pfaf_61", "pfaf_62", "pfaf_63", "pfaf_64", "pfaf_65", "pfaf_66",
              "pfaf_67",
              "pfaf_71", "pfaf_72", "pfaf_73", "pfaf_74", "pfaf_75", "pfaf_76",
              "pfaf_77", "pfaf_78",
              "pfaf_81", "pfaf_82", "pfaf_83", "pfaf_84", "pfaf_85", "pfaf_86",
              "pfaf_91"]
BASINS_TO_RUN = ["pfaf_74", "pfaf_76", "pfaf_62", "pfaf_22"]  # Miss., Columbia, Amazon, Danube

# --- Campaign tag: groups all this run's DPS jobs for batch monitoring ---
CAMPAIGN = f"mm_{START_YEAR}{START_MONTH:02d}_n{N_MONTHS}_c{CHUNK_MONTHS}"


def month_list(y, m, n):
    \"\"\"Return n consecutive 'YYYY-MM' strings starting at year y, month m.\"\"\"
    out = []
    for i in range(n):
        mm = (m - 1 + i) % 12 + 1
        yy = y + (m - 1 + i) // 12
        out.append(f"{yy}-{mm:02d}")
    return out


MONTHS = month_list(START_YEAR, START_MONTH, N_MONTHS)
# Group months into chunks (each chunk becomes one DPS job).
CHUNKS = [MONTHS[i:i + CHUNK_MONTHS] for i in range(0, len(MONTHS), CHUNK_MONTHS)]

n_streams = len(BASINS_TO_RUN) * len(LSMS_TO_RUN)
print(f"Campaign : {CAMPAIGN}")
print(f"Span     : {MONTHS[0]} .. {MONTHS[-1]}  ({N_MONTHS} months)")
print(f"Chunking : {CHUNK_MONTHS} month(s)/chunk  ->  {len(CHUNKS)} chunks/stream")
print(f"Basins   : {len(BASINS_TO_RUN)}/{len(ALL_BASINS)}  {BASINS_TO_RUN}")
print(f"LSMs     : {len(LSMS_TO_RUN)}/{len(ALL_LSMS)}  {LSMS_TO_RUN}")
print(f"Streams  : {n_streams} (basin x LSM)  ->  {n_streams * len(CHUNKS)} DPS jobs total")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 1: Download static basin files (Parquet) from Zenodo

Static river-network files come from Zenodo record
[20672740](https://doi.org/10.5281/zenodo.20672740) — *"RAPID2 static files for global
implementation with MERIT Basins v1.0 and GLDAS v2.0"* — in **Parquet** format, one zip per
PFAF level-2 basin. Each basin zip contains `con_`, `crd_`, `cpl_…_GLDAS`, `kpr_…_nrm`,
`xpr_…_nrm`, and `bas_…_topo` parquet files.

To keep the test light we download only the basins in `BASINS_TO_RUN`; set
`BASINS_TO_RUN = ALL_BASINS` (in the Configuration cell above) to pull all 61."""))

cells.append(code("""\
ZENODO_RECORD = "20672740"
ZENODO_BASE = f"https://zenodo.org/records/{ZENODO_RECORD}/files"
ZIP_TMPL = "MERIT_Basins_v1.0_pfaf_{ii}_GLDAS_v2.0_RAPID_v2.0.zip"

# BASINS_TO_RUN (Configuration cell) is the selection -- fetch exactly those.
want = [b.replace("pfaf_", "") for b in BASINS_TO_RUN]

_t0 = time.time()
print(f"Downloading {len(want)} basin bundle(s) from Zenodo record {ZENODO_RECORD}...")
for ii in want:
    zf = ZIP_TMPL.format(ii=ii)
    dest = INPUT_DIR / zf
    if not dest.exists():
        print(f"  Downloading {zf} ...")
        urllib.request.urlretrieve(f"{ZENODO_BASE}/{zf}?download=1", dest)
    else:
        print(f"  Have {zf}")
    # Basin zips are flat (files at the archive root).
    with zipfile.ZipFile(dest, "r") as z:
        z.extractall(INPUT_DIR)

print(f"\\nExtracted parquet files in {time.time() - _t0:.1f}s. Sample:")
for f in sorted(INPUT_DIR.glob("*_pfaf_*.parquet"))[:8]:
    print(f"  {f.name}")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 2: Download GLDAS runoff for every month, for each LSM in the span

GLDAS is global gridded data — **one download per (month, LSM), shared by all basins** running
that LSM. We use the **`dgldas2` CLI** from the `rapid2` package (exactly as the official
Mississippi tutorial does): it searches NASA CMR, downloads the granules via `earthaccess`
(authenticated above), concatenates them, and applies the phase-specific time fix and unit
conversion — producing a single ready-to-use monthly runoff file. `dgldas2 --time` takes one
`YYYY-MM` (≤300 granules, ~37.5 days), which is one month per call — a clean fit for our
per-month loop.

Downloads are cached on disk (`dgldas2` skips a month whose output already exists), so
re-running is cheap. The helper `gldas_month(lsm, month)` returns that (LSM, month)'s file
path."""))

cells.append(code("""\
import netCDF4
import numpy as np
import subprocess

GLDAS_DIR = INPUT_DIR / "gldas"
GLDAS_DIR.mkdir(exist_ok=True)

# Per-step timing captured for the instrumentation dashboard.
timings = {"gldas_download": {}}


def gldas_month(lsm, month):
    \"\"\"Download one month ('YYYY-MM') of GLDAS runoff for one LSM via dgldas2.

    dgldas2 searches CMR, downloads via earthaccess (using EARTHDATA_TOKEN set
    in the auth cell), concatenates, and applies the phase time-fix + unit
    conversion. Cached: dgldas2 skips if the output already exists. Returns Path.
    \"\"\"
    out = GLDAS_DIR / f"GLDAS_{PHASE}_{lsm}_{month}.nc4"
    if out.exists():
        return out

    _t0 = time.time()
    # Retry with exponential backoff: transient granule-transfer failures are
    # expected across hundreds of files, and already-downloaded granules are
    # skipped on retry (as in the official tutorial).
    delay, attempts = 10, 5
    for attempt in range(1, attempts + 1):
        r = subprocess.run(
            ["dgldas2",
             "--phase", PHASE,
             "--model", lsm,
             "--time", month,
             "--land_surface_model", str(out)],
            capture_output=True, text=True,
        )
        if r.returncode == 0 and out.exists():
            break
        if attempt < attempts:
            print(f"    dgldas2 {lsm} {month} attempt {attempt} failed; retry in {delay}s")
            time.sleep(delay)
            delay *= 2
    if not out.exists():
        raise RuntimeError(
            f"dgldas2 failed for {lsm} {month} after {attempts} attempts. "
            f"If errors mention authorization, re-run the Earthdata auth cell "
            f"(token may be mistyped/expired) and accept the GES DISC EULA.\\n"
            f"{r.stderr.strip()[-400:]}")
    timings["gldas_download"][(lsm, month)] = time.time() - _t0
    return out


print(f"Downloading GLDAS for {len(MONTHS)} month(s) x {len(LSMS_TO_RUN)} LSM(s) "
      f"via dgldas2 (cached; shared across basins)...")
for lsm in LSMS_TO_RUN:
    for mo in MONTHS:
        path = gldas_month(lsm, mo)
        dt = timings["gldas_download"].get((lsm, mo))
        print(f"  {lsm} {mo}: {path.name}" + (f"  [{dt:.1f}s]" if dt else "  [cached]"))
print("GLDAS ready.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 3: Build per-basin, per-chunk external inflow (`cpllsm` + `concat_qext`)

For each basin and month we run `cpllsm` to couple that month's runoff onto the basin's river
network (→ a monthly `Qext`). When `CHUNK_MONTHS > 1`, `concat_qext` merges a chunk's monthly
`Qext` files along the time axis into a single file (a no-op passthrough for the 1-month test).

`concat_qext` enforces the two conditions `rapid2` requires: a uniform external time step
(`IS_dtE` constant, and `IS_dtE % IS_dtR == 0`) and a monotonic, gap-free `time` across the
month boundaries."""))

cells.append(code("""\
import shutil
import subprocess


def basin_static(basin):
    \"\"\"Paths to the 6 static parquet files for a basin (from Zenodo 20672740).\"\"\"
    return {
        "con": INPUT_DIR / f"con_{basin}.parquet",
        "crd": INPUT_DIR / f"crd_{basin}.parquet",
        "cpl": INPUT_DIR / f"cpl_{basin}_GLDAS.parquet",
        "kpr": INPUT_DIR / f"kpr_{basin}_nrm.parquet",
        "xpr": INPUT_DIR / f"xpr_{basin}_nrm.parquet",
        "bas": INPUT_DIR / f"bas_{basin}_topo.parquet",
    }


def qext_month(basin, lsm, month):
    \"\"\"Run cpllsm for one basin+LSM+month; return the monthly Qext path (cached).\"\"\"
    out = INPUT_DIR / f"Qext_{basin}_{PHASE}_{lsm}_{month}.nc4"
    if out.exists():
        return out
    st = basin_static(basin)
    r = subprocess.run(
        ["cpllsm",
         "--land_surface_model", str(gldas_month(lsm, month)),
         "--connectivity", str(st["con"]),
         "--coordinates", str(st["crd"]),
         "--coupling", str(st["cpl"]),
         "--external_inflow", str(out)],
        capture_output=True, text=True,
    )
    if r.returncode != 0 or not out.exists():
        raise RuntimeError(f"cpllsm failed for {basin} {lsm} {month}: {r.stderr.strip()[-300:]}")
    return out


def concat_qext(month_files, out_path):
    \"\"\"Join several months of external inflow into one chunk file for a single job.

    Only needed when CHUNK_MONTHS > 1; a single month passes straight through.
    \"\"\"
    if len(month_files) == 1:
        if out_path.resolve() != month_files[0].resolve():
            shutil.copy(month_files[0], out_path)
        return out_path

    # RAPID needs a steady time step that divides the routing step, and the
    # months must line up end-to-start with no gap. Check that before joining so
    # a bad chunk fails here with a clear message instead of deep inside rapid2.
    dtE = None
    prev_end = None
    for mf in month_files:
        with netCDF4.Dataset(mf, "r") as ds:
            tb = np.array(ds.variables["time_bnds"][:], dtype=np.int64)
        step = int(tb[0, 1] - tb[0, 0])
        if dtE is None:
            dtE = step
        elif step != dtE:
            raise ValueError(f"Runoff time step changes within the chunk: {step} != {dtE}")
        if int(IS_dtR) and dtE % int(IS_dtR) != 0:
            raise ValueError(f"Runoff step ({dtE}s) is not a multiple of IS_dtR ({IS_dtR}s)")
        if prev_end is not None and tb[0, 0] != prev_end:
            raise ValueError(
                f"Time gap before {mf.name}: it starts at {tb[0, 0]} but the "
                f"previous month ended at {prev_end}")
        prev_end = int(tb[-1, 1])

    # The river network never changes, so copy rivid/lon/lat once and stack the
    # time-varying Qext (with its time / time_bnds) month after month.
    with netCDF4.Dataset(month_files[0], "r") as src, netCDF4.Dataset(out_path, "w") as dst:
        dst.setncatts({a: src.getncattr(a) for a in src.ncattrs()})
        for name, dim in src.dimensions.items():
            dst.createDimension(name, None if dim.isunlimited() else len(dim))
        for name, var in src.variables.items():
            dst.createVariable(name, var.datatype, var.dimensions).setncatts(
                {a: var.getncattr(a) for a in var.ncattrs()})
        for name, var in src.variables.items():
            if "time" not in var.dimensions:
                dst.variables[name][:] = var[:]
    ti = 0
    for mf in month_files:
        with netCDF4.Dataset(mf, "r") as src, netCDF4.Dataset(out_path, "a") as dst:
            n = src.dimensions["time"].size
            for name in ("Qext", "time", "time_bnds"):
                dst.variables[name][ti:ti + n] = src.variables[name][:]
            ti += n  # keep appending where the previous month left off
    return out_path


def chunk_qext(basin, lsm, chunk_idx):
    \"\"\"Build (or reuse) the concatenated Qext for one (basin, LSM, chunk).\"\"\"
    months = CHUNKS[chunk_idx]
    month_files = [qext_month(basin, lsm, mo) for mo in months]
    if len(months) == 1:
        return month_files[0]
    out = INPUT_DIR / f"Qext_{basin}_{PHASE}_{lsm}_{months[0]}_to_{months[-1]}.nc4"
    if not out.exists():
        concat_qext(month_files, out)
    return out


print("Helpers ready: basin_static, qext_month, concat_qext, chunk_qext.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 4: Confirm basins are ready and build the cold-start initial state

We verify every basin in `BASINS_TO_RUN` actually has a complete static set on disk (catches a
partial/failed Zenodo download early). Each **(basin, LSM)** pair is one independent simulation
stream — `STREAMS` is the full list of pairs this run will simulate. For each stream we build the
**chunk-0 external inflow** and a matching `zeroqinit` cold start — `zeroqinit` copies
`Qext.time[0]` into `Qinit`, which satisfies `rapid2`'s requirement that the initial state's
first timestamp equals the inflow's first timestamp."""))

cells.append(code("""\
def discover_basins():
    \"\"\"Every basin with a complete static file set on disk.\"\"\"
    found = []
    for con in sorted(INPUT_DIR.glob("con_pfaf_*.parquet")):
        basin = con.stem.replace("con_", "")           # -> pfaf_NN
        st = basin_static(basin)
        if all(p.exists() for p in st.values()):
            found.append(basin)
    return found


_ready = set(discover_basins())
SIM_BASINS = [b for b in BASINS_TO_RUN if b in _ready]
_missing = [b for b in BASINS_TO_RUN if b not in _ready]
if _missing:
    print(f"WARNING: {len(_missing)} requested basin(s) missing static files, skipping: "
          f"{_missing}")
print(f"Basins to simulate ({len(SIM_BASINS)}): {', '.join(SIM_BASINS)}")

# One independent stream per (basin, LSM) pair.
STREAMS = [(b, lsm) for b in SIM_BASINS for lsm in LSMS_TO_RUN]
print(f"Streams to simulate ({len(STREAMS)}): {STREAMS}")


def cold_start(basin, lsm):
    \"\"\"Build chunk-0 Qext and a zeroqinit cold-start Q00 for a stream. Returns Q00 path.\"\"\"
    q0_qext = chunk_qext(basin, lsm, 0)
    qinit = INPUT_DIR / f"Qinit_{basin}_{PHASE}_{lsm}_{CHUNKS[0][0]}.nc4"
    if not qinit.exists():
        r = subprocess.run(
            ["zeroqinit", "--external_inflow", str(q0_qext),
             "--initial_outflow", str(qinit)],
            capture_output=True, text=True,
        )
        if r.returncode != 0 or not qinit.exists():
            raise RuntimeError(f"zeroqinit failed for {basin} {lsm}: {r.stderr.strip()[-300:]}")
    return qinit


print("cold_start() ready.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 5: S3 staging helpers

Each DPS job needs six inputs in S3: the chunk `Qext`, the initial state `Q00` (a cold start for
chunk 0, or the **previous chunk's `Qfinal`** afterward), and the four static parquet files
(`con`, `kpr`, `xpr`, `bas`). Outputs (`Qout`, `Qfinal`) are written by DPS to its own S3
location; we parse those URLs to fetch results and to chain `Qfinal` into the next chunk."""))

cells.append(code("""\
S3_ROOT = f"{username}/rapid2/multimonth/{CAMPAIGN}"

# Every local file that RAPID2's own code (cpllsm, zeroqinit, rapid2 routing)
# actually consumed or produced for a succeeded chunk -- the minimum set
# needed to reproduce results, as opposed to notebook-only scratch (monthly
# Qext pieces superseded by their chunk concat, raw GLDAS downloads, Zenodo
# zips). Populated by stage_chunk_inputs(); read by the disk-usage summary
# after Step 9. A set, so files reused across chunks/LSMs (the 6 static
# parquet files are the same for every chunk of a basin) aren't double-counted.
staged_input_paths = set()


def s3_upload(local_path, key):
    s3_ws.upload_file(str(local_path), BUCKET, key)
    return f"s3://{BUCKET}/{key}"


def parse_dps_s3_url(url):
    \"\"\"Split a DPS output URL into (bucket, key). DPS sometimes returns a
    path-style URL with a host:port, so handle both forms.\"\"\"
    p = urlparse(url)
    host = p.netloc
    if "amazonaws.com" in host:
        parts = p.path.lstrip("/").split("/", 1)
        return parts[0], (parts[1] if len(parts) > 1 else "")
    return host, p.path.lstrip("/")


def align_q00_time(q00_path, qext_path):
    \"\"\"Make the initial-state file's first timestamp match the inflow's.

    RAPID stamps a month's Qfinal with that month's last time, but the next
    month's inflow starts one GLDAS step later, so chaining Qfinal -> Q00 trips
    RAPID's check that Q00.time[0] == Qex.time[0]. Only the timestamp differs;
    the discharge values are the true carried-over state. Copy the inflow's
    first timestamp onto Q00 so the months join cleanly.
    \"\"\"
    with netCDF4.Dataset(qext_path, "r") as qx:
        t0 = np.array(qx.variables["time"][:])[0]
    with netCDF4.Dataset(q00_path, "a") as q0:
        q0.variables["time"][0] = t0
    return q00_path


def stage_chunk_inputs(basin, lsm, chunk_idx, q00_local_or_s3):
    \"\"\"Upload the 6 inputs for one (basin, LSM, chunk). q00 may be a local Path
    (cold start) or an already-in-S3 s3:// URL (a prior chunk's Qfinal). Returns
    dict of s3 URLs keyed by the DPS input names. Staged under a path namespaced
    by LSM so multiple LSMs for the same basin never collide.\"\"\"
    st = basin_static(basin)
    qext = chunk_qext(basin, lsm, chunk_idx)
    pfx = f"{S3_ROOT}/{basin}/{lsm}/chunk_{chunk_idx:03d}"

    urls = {}
    urls["Qex_ncf"] = s3_upload(qext, f"{pfx}/{qext.name}")
    if isinstance(q00_local_or_s3, str) and q00_local_or_s3.startswith("s3://"):
        # Chained start state: pull the previous chunk's Qfinal down, align its
        # timestamp to this chunk's inflow, and upload the aligned copy.
        bkt, key = parse_dps_s3_url(q00_local_or_s3)
        q00 = INPUT_DIR / f"Q00_{basin}_{lsm}_chunk_{chunk_idx:03d}.nc4"
        s3_ws.download_file(bkt, key, str(q00))
        align_q00_time(q00, qext)
    else:
        # Cold start (chunk 0): zeroqinit already matches Qext.time[0].
        q00 = Path(q00_local_or_s3)
    urls["Q00_ncf"] = s3_upload(q00, f"{pfx}/{q00.name}")
    urls["con_pqt"] = s3_upload(st["con"], f"{pfx}/{st['con'].name}")
    urls["kpr_pqt"] = s3_upload(st["kpr"], f"{pfx}/{st['kpr'].name}")
    urls["xpr_pqt"] = s3_upload(st["xpr"], f"{pfx}/{st['xpr'].name}")
    urls["bas_pqt"] = s3_upload(st["bas"], f"{pfx}/{st['bas'].name}")

    # Record every file RAPID2's own code consumed for this chunk, for the
    # minimum-footprint disk-usage summary. Includes crd/cpl -- not uploaded
    # to DPS, but required by cpllsm (Step 3) to build Qext in the first
    # place, so they're part of the real reproducibility footprint.
    staged_input_paths.update([qext, q00, st["con"], st["kpr"], st["xpr"],
                               st["bas"], st["crd"], st["cpl"]])
    return urls


print("S3 helpers ready. Job inputs will live under:")
print(f"  s3://{BUCKET}/{S3_ROOT}/<basin>/<lsm>/chunk_NNN/")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 6: The async per-(basin, LSM) chunked-stream orchestrator

This is the heart of the notebook. Each **(basin, LSM) pair** is a **stream** that steps through
its chunks in order; a chunk is submitted to DPS only once the previous chunk in that stream has
succeeded (feeding its `Qfinal` in as `Q00`). Streams run **concurrently** with no global
barrier — that's true whether the parallelism comes from more basins, more LSMs, or both.

One small handoff detail: a chunk's `Qfinal` is stamped with that chunk's *last* timestamp, but
the next chunk's inflow begins one runoff step later, so `align_q00_time` (in Step 5) copies the
inflow's first timestamp onto the carried-over state before submitting — the discharge values are
untouched, only the label is aligned so RAPID accepts it as the initial condition.

The event loop polls all in-flight jobs. On each poll:
- **succeeded** → record the chunk's `Qfinal` S3 URL, advance that stream (submit its next
  chunk, or mark it done);
- **failed / revoked** (Spot reclaim) → re-submit that same chunk from the last good `Q00`,
  up to `MAX_RETRIES`;
- **running / queued** → leave it.

State per (basin, LSM, chunk) is captured for the instrumentation dashboard."""))

cells.append(code("""\
QUEUE = "maap-dps-worker-8gb"
POLL_SECONDS = 30              # how often to check job status
MAX_RETRIES = 3               # per chunk, mainly to ride out Spot interruptions
MAX_WALL_MINUTES = 24 * 60    # stop the loop after a day so it never hangs forever

# DPS job statuses that mean "finished". A "bad" status includes Revoked, which
# is what a reclaimed Spot worker looks like — those chunks are simply retried.
TERMINAL_OK = {"Succeeded"}
TERMINAL_BAD = {"Failed", "Deleted", "Dismissed", "Offline", "Revoked"}


def submit_chunk(basin, lsm, chunk_idx, q00):
    \"\"\"Stage inputs and submit one (basin, LSM, chunk) DPS job. Returns the job handle.\"\"\"
    urls = stage_chunk_inputs(basin, lsm, chunk_idx, q00)
    job = maap.submitJob(
        identifier=f"{CAMPAIGN}_{lsm}_{basin}_c{chunk_idx:03d}",
        algo_id="rapid2",
        version="maap",
        queue=QUEUE,
        username=username,
        Qex_ncf=urls["Qex_ncf"],
        Q00_ncf=urls["Q00_ncf"],
        con_pqt=urls["con_pqt"],
        kpr_pqt=urls["kpr_pqt"],
        xpr_pqt=urls["xpr_pqt"],
        bas_pqt=urls["bas_pqt"],
        IS_dtR=IS_dtR,
    )
    return job


def qfinal_url(job):
    \"\"\"Find the Qfinal S3 URL among a completed job's outputs.\"\"\"
    job.retrieve_result()
    for url in job.outputs:
        if url.startswith("s3://"):
            bkt, prefix = parse_dps_s3_url(url)
            resp = s3_ws.list_objects_v2(Bucket=bkt, Prefix=prefix)
            for obj in resp.get("Contents", []):
                if obj["Key"].split("/")[-1].startswith("Qfinal"):
                    return f"s3://{bkt}/{obj['Key']}"
    raise RuntimeError("No Qfinal in job outputs")


def run_campaign(stream_keys):
    \"\"\"Async chunked-stream orchestrator. stream_keys is a list of (basin, lsm)
    pairs. Returns per-(basin,lsm,chunk) records.\"\"\"
    n_chunks = len(CHUNKS)
    streams = {}          # per-stream progress: where each (basin, lsm) is in its timeline
    records = []          # one row per finished chunk, for the summary below

    # Start every stream on its first chunk at once; they then advance on their own.
    print(f"Staging inputs and submitting {len(stream_keys)} stream(s) -- this can take a")
    print(f"while for large CHUNK_MONTHS (concatenating months, uploading bigger files)...")
    for basin, lsm in stream_keys:
        job = submit_chunk(basin, lsm, 0, cold_start(basin, lsm))
        streams[(basin, lsm)] = {"next": 0, "q00": None, "job": job, "retries": 0,
                                 "done": False, "t_submit": {0: time.time()}}
        print(f"  launched {basin} {lsm} chunk 0 -> {job.id}")

    start = time.time()
    print(f"\\nAll {len(stream_keys)} stream(s) launched. Waiting for completion "
          f"(polling every {POLL_SECONDS}s)...")
    _poll_num = 0
    while not all(s["done"] for s in streams.values()):
        elapsed = time.time() - start
        if elapsed > MAX_WALL_MINUTES * 60:
            print("Wall-clock cap reached; exiting poll loop (re-runnable).")
            break
        time.sleep(POLL_SECONDS)
        _poll_num += 1

        counts = {}
        for (basin, lsm), s in streams.items():
            if s["done"]:
                continue
            job, ci = s["job"], s["next"]
            try:
                job.retrieve_status()
                status = job.status
            except Exception:
                status = "PollError"
            counts[status] = counts.get(status, 0) + 1

            if status in TERMINAL_OK:
                qfin = qfinal_url(job)
                records.append({"basin": basin, "lsm": lsm, "chunk": ci,
                                "status": "Succeeded", "job_id": job.id, "job": job,
                                "qfinal": qfin, "t_submit": s["t_submit"].get(ci),
                                "t_finish": time.time()})
                nxt = ci + 1
                if nxt >= n_chunks:
                    s["done"] = True          # this stream reached the end of its timeline
                    print(f"  {basin} {lsm}: DONE ({n_chunks} chunks)")
                else:
                    # Feed this chunk's final state in as the next chunk's start.
                    s["q00"] = qfin
                    s["next"] = nxt
                    s["retries"] = 0
                    s["job"] = submit_chunk(basin, lsm, nxt, qfin)
                    s["t_submit"][nxt] = time.time()
                    print(f"  {basin} {lsm}: chunk {ci} ok -> launched chunk {nxt}")
            elif status in TERMINAL_BAD:
                if s["retries"] < MAX_RETRIES:
                    # Re-run this same chunk. Its start state is unchanged (the
                    # cold start for chunk 0, or the last good Qfinal otherwise),
                    # so no earlier work is repeated.
                    s["retries"] += 1
                    q00 = cold_start(basin, lsm) if ci == 0 else s["q00"]
                    s["job"] = submit_chunk(basin, lsm, ci, q00)
                    print(f"  {basin} {lsm}: chunk {ci} {status} -> "
                          f"retry {s['retries']}/{MAX_RETRIES}")
                else:
                    s["done"] = True
                    records.append({"basin": basin, "lsm": lsm, "chunk": ci,
                                    "status": f"FAILED:{status}", "job_id": job.id,
                                    "job": job, "qfinal": None})
                    print(f"  {basin} {lsm}: chunk {ci} {status} -> "
                          f"gave up after {MAX_RETRIES} retries")

        active = sum(1 for s in streams.values() if not s["done"])
        print(f"  [{time.strftime('%H:%M:%S')}  poll #{_poll_num}, "
              f"+{(time.time() - start) / 60:.1f} min elapsed] active streams={active}  " +
              "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    return streams, records


print("Orchestrator ready. Run the next cell to launch the campaign.")"""))

cells.append(code("""\
print(f"BATCH KEY for this run: {CAMPAIGN!r}")
print(f"  Every job's identifier starts with this. Find them all in the MAAP")
print(f"  Jobs UI by searching '{CAMPAIGN}', or programmatically via the next cell.\\n")

_t_campaign = time.time()
streams, records = run_campaign(STREAMS)
timings["campaign_wall"] = time.time() - _t_campaign

n_ok = sum(1 for r in records if r["status"] == "Succeeded")
n_bad = sum(1 for r in records if r["status"].startswith("FAILED"))
print(f"\\nCampaign wall-clock: {timings['campaign_wall'] / 60:.1f} min")
print(f"Chunks succeeded: {n_ok}   failed: {n_bad}")
done = sum(1 for s in streams.values() if s['done'])
print(f"Streams finished : {done}/{len(streams)}")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
### Finding this batch's jobs later

Every job in a run shares one **batch key** — the `CAMPAIGN` string (e.g.
`mm_201001_n12_c1`) — because each job's `identifier` is built as
`<CAMPAIGN>_<lsm>_<basin>_c<chunk>`. maap-py 4.x has no separate job-tag field, so
the `identifier` *is* the tag: `listJobs` matches on it. Use the helper below to
pull every job in this batch back from DPS — handy after a kernel restart, or to
audit a run in bulk without the in-memory `records`."""))

cells.append(code("""\
def find_campaign_jobs(campaign=None, status=None):
    \"\"\"List DPS jobs for this batch (campaign) straight from the server.

    maap-py's `tag` filter matches the job identifier, and every job in a run
    shares the CAMPAIGN prefix, so we ask DPS directly with tag=CAMPAIGN.
    `status` optionally narrows to e.g. 'job-completed' or 'job-failed'.
    Returns the parsed job list (or the raw response if the shape is unexpected),
    so you can inspect the real structure on your deployment.
    \"\"\"
    campaign = campaign or CAMPAIGN
    resp = maap.listJobs(algo_id="rapid2", version="maap", tag=campaign,
                         status=status, get_job_details=False, page_size=1000)
    # listJobs returns a requests.Response; try JSON, fall back to text.
    try:
        data = resp.json()
    except Exception:
        data = resp.text if hasattr(resp, "text") else resp
    print(f"Batch {campaign!r}"
          + (f", status={status}" if status else "")
          + " — queried DPS (tag filter = campaign key).")
    print("  Also searchable in the MAAP Jobs UI by this key.")
    return data


# Examples (uncomment to use):
#   all_jobs   = find_campaign_jobs()                     # everything in this batch
#   failed_only = find_campaign_jobs(status="job-failed") # just the failures
print(f"find_campaign_jobs() ready. Batch key = {CAMPAIGN!r}")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 7: Verify chaining and continuity

The **critical correctness check**: within each stream (basin, LSM), chunk *M*'s
`Qfinal.time[0]` must equal chunk *M+1*'s `Qext.time[0]` — that is what makes the months a
continuous simulation and what `rapid2` asserts internally. We download each stream's `Qfinal`
files and confirm the boundary timestamps line up, and that each chunk's `Qout` spans the
expected number of timesteps."""))

cells.append(code("""\
def download_qfinals(records):
    \"\"\"Download every succeeded chunk's Qfinal to output/<basin>/<lsm>/.\"\"\"
    got = {}
    for r in records:
        if r["status"] != "Succeeded" or not r["qfinal"]:
            continue
        bkt, key = parse_dps_s3_url(r["qfinal"])
        d = OUTPUT_DIR / r["basin"] / r["lsm"]
        d.mkdir(parents=True, exist_ok=True)
        local = d / f"Qfinal_chunk_{r['chunk']:03d}.nc4"
        s3_ws.download_file(bkt, key, str(local))
        got[(r["basin"], r["lsm"], r["chunk"])] = local
    return got


qfinals = download_qfinals(records)

# Each month's Qfinal is stamped with that month's LAST time; the next month's
# inflow starts one runoff step later. So a correct, gap-free chain has the two
# differing by exactly one step (that offset is what align_q00_time bridges).
# A zero difference or a gap larger than one step would mean the months don't
# join cleanly.
print("=== Continuity check: gap between Qfinal[M] and Qext[M+1] should be 1 step ===")
ok_all = True
for basin, lsm in sorted({(r["basin"], r["lsm"]) for r in records if r["status"] == "Succeeded"}):
    # Determine this run's runoff step from chunk 0's inflow (time_bnds width).
    with netCDF4.Dataset(chunk_qext(basin, lsm, 0), "r") as ds:
        tb = np.array(ds.variables["time_bnds"][:])
        step = int(tb[0, 1] - tb[0, 0])
    for ci in range(len(CHUNKS) - 1):
        qf = qfinals.get((basin, lsm, ci))
        if not qf:
            continue
        with netCDF4.Dataset(qf, "r") as ds:
            t_final = int(np.array(ds.variables["time"][:])[0])
        with netCDF4.Dataset(chunk_qext(basin, lsm, ci + 1), "r") as ds:
            t_next = int(np.array(ds.variables["time"][:])[0])
        gap = t_next - t_final
        good = (gap == step)
        ok_all &= good
        if not good:
            kind = "OVERLAP/DUP" if gap <= 0 else "GAP"
            print(f"  {kind} {basin} {lsm} chunk {ci}->{ci+1}: "
                  f"Qfinal.t0={t_final} Qext.t0={t_next} gap={gap}s (expected {step}s)")
print("All boundaries contiguous (1 step apart)." if ok_all else
      "Boundaries NOT contiguous — investigate before trusting the joined series.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 8: Timing & Spot instrumentation dashboard

The real deliverable of the test run: separate **fixed per-job overhead** (container/conda
startup + S3 staging) from **linear routing time**, and record how often Spot reclaims happened.
These numbers determine the optimal `CHUNK_MONTHS` for a 30-year production campaign. Per-job
DPS run times come from each job's own metrics (`job.retrieve_metrics()` →
`job.job_duration_seconds`). The same call also returns `machine_type` and `directory_size`,
reused by the disk-usage and cost-estimation sections below — one metrics fetch per job, not
three."""))

cells.append(code("""\
import statistics

# Retry/reclaim tally from the campaign records + stream state.
retries = sum(s.get("retries", 0) for s in streams.values())
n_ok = sum(1 for r in records if r["status"] == "Succeeded")
n_bad = sum(1 for r in records if r["status"].startswith("FAILED"))


def job_metrics(job):
    \"\"\"DPS-reported metrics for a completed job: duration, machine type, and
    working-directory size (bytes) -- straight from the platform's own record,
    more reliable than timing the poll loop or measuring local disk.

    job.metrics (the raw dict retrieve_metrics() populated) is kept in the
    result under "raw" so a missing field can be diagnosed instead of just
    silently absent -- see the diagnostic cell right after this one.
    \"\"\"
    out = {"duration_s": None, "machine_type": None, "directory_size": None,
          "raw": None, "error": None}
    try:
        job.retrieve_metrics()
    except Exception as e:
        out["error"] = f"retrieve_metrics() raised: {e!r}"
        return out
    out["raw"] = dict(job.metrics) if job.metrics else {}
    if job.job_duration_seconds is not None:
        out["duration_s"] = float(job.job_duration_seconds)
    elif job.job_start_time and job.job_end_time:
        from datetime import datetime
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
        t0 = datetime.strptime(job.job_start_time[:26], fmt)
        t1 = datetime.strptime(job.job_end_time[:26], fmt)
        out["duration_s"] = (t1 - t0).total_seconds()
    if job.machine_type:
        out["machine_type"] = job.machine_type
    if job.directory_size:
        try:
            out["directory_size"] = int(job.directory_size)
        except (TypeError, ValueError):
            out["error"] = f"directory_size present but not int-able: {job.directory_size!r}"
    return out


# One metrics fetch per succeeded job, reused by Steps 8 (timing), disk usage,
# and cost estimation below.
_used_wallclock_fallback = False
job_metrics_by_record = {}
for i, r in enumerate(records):
    if r["status"] == "Succeeded" and r.get("job") is not None:
        m = job_metrics(r["job"])
        if not m["duration_s"] and r.get("t_submit") and r.get("t_finish"):
            # HySDS/DPS didn't return a duration -- fall back to our own
            # submit->completion wall-clock time. Coarser than the platform's
            # figure (it includes queueing/staging, not just routing), but
            # it's real, measured data rather than nothing.
            m["duration_s"] = r["t_finish"] - r["t_submit"]
            m["duration_is_wallclock_fallback"] = True
            _used_wallclock_fallback = True
        job_metrics_by_record[i] = m

durations = [m["duration_s"] for m in job_metrics_by_record.values() if m["duration_s"]]
if _used_wallclock_fallback:
    print("NOTE: job_duration_seconds unavailable from job.retrieve_metrics() on this")
    print("  deployment -- some/all durations below are our own submit->completion")
    print("  wall-clock time instead (includes queue/staging time, not pure routing).")

# Diagnostic: if machine_type/directory_size are missing, show exactly what
# the platform sent back instead of a silent "not available" -- print the raw
# metrics dict for one job so the actual field names on this deployment are
# visible (they may differ from the maap-py docstring's sample).
_missing_mtype = sum(1 for m in job_metrics_by_record.values() if not m["machine_type"])
_missing_dsize = sum(1 for m in job_metrics_by_record.values() if not m["directory_size"])
if job_metrics_by_record and (_missing_mtype or _missing_dsize):
    _sample = next(iter(job_metrics_by_record.values()))
    print(f"NOTE: machine_type missing for {_missing_mtype}/{len(job_metrics_by_record)} jobs, "
          f"directory_size missing for {_missing_dsize}/{len(job_metrics_by_record)}.")
    if _sample["error"]:
        print(f"  Sample error: {_sample['error']}")
    print(f"  Sample raw job.metrics dict: {_sample['raw']}")

print("=" * 60)
print(f"  CAMPAIGN: {CAMPAIGN}")
print("=" * 60)
print(f"  Basins             : {len(SIM_BASINS)}")
print(f"  LSMs               : {len(LSMS_TO_RUN)}  {LSMS_TO_RUN}")
print(f"  Streams (basin x LSM): {len(STREAMS)}")
print(f"  Chunks/stream       : {len(CHUNKS)}  ({CHUNK_MONTHS} month(s) each)")
print(f"  Total DPS jobs      : {n_ok + n_bad}  (ok={n_ok}, failed={n_bad})")
print(f"  Spot retries        : {retries}")
print(f"  Campaign wall-clock : {timings.get('campaign_wall', 0) / 60:.1f} min")
gd = timings.get("gldas_download", {})
if gd:
    dl = list(gd.values())
    print(f"  GLDAS dl / (lsm,month): mean {statistics.mean(dl):.1f}s over {len(dl)} download(s)")
if durations:
    print("\\n  --- Per-job DPS routing time (chunk = "
          f"{CHUNK_MONTHS} month(s)) ---")
    print(f"    n={len(durations)}  min={min(durations):.0f}s  "
          f"mean={statistics.mean(durations):.0f}s  max={max(durations):.0f}s")
    per_month = statistics.mean(durations) / CHUNK_MONTHS
    print(f"    => ~{per_month:.0f}s per month (routing, incl. per-job overhead)")
    print("\\n  For chunk-size selection: run this notebook at two CHUNK_MONTHS")
    print("  values (e.g. 1 and 3) and fit T(job) = overhead + k * CHUNK_MONTHS.")
    print("  Large overhead -> favor bigger chunks; but keep jobs short for Spot.")
    print("\\n  30-year (360-month) projection at this per-month rate, per stream:")
    print(f"    ~{per_month * 360 / 3600:.1f} hr of routing spread over "
          f"{360 // CHUNK_MONTHS} chunks (streams run concurrently).")
else:
    print("\\n  (Per-job DPS durations were unavailable from job.retrieve_metrics()")
    print("   on this deployment — check the Jobs UI timestamps instead.)")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## AWS cost estimation

DPS reports each job's `machine_type` (e.g. `c5.4xlarge`) and `job_duration_seconds`. To turn
that into a dollar estimate we fetch **live, public EC2 pricing** from
[ec2instances.github.io](https://ec2instances.github.io/data/US%20West%20(Oregon).json) (no AWS
credentials needed) and look up each job's machine type.

**If `machine_type` isn't populated on this deployment**, we still have real
`job_duration_seconds` (used in Step 8), so we fall back to an **assumed instance type** you set
below (`ASSUMED_MACHINE_TYPE`) — pick one that matches the DPS `QUEUE` size (e.g.
`maap-dps-worker-8gb` ≈ an 8 GB-class instance such as `m5.large`/`m5.xlarge`; check the MAAP
Jobs UI or ask a MAAP admin for the actual instance type your queue uses, and edit the constant
below to match).

**Important caveat regardless of which path is used:** that pricing feed is **on-demand**
pricing only — it has no Spot price field — while DPS workers run on **EC2 Spot**, which is
typically 60–70% cheaper. So we report both: the on-demand-equivalent cost computed directly
from real duration data, and an approximate Spot-adjusted estimate using an editable
`SPOT_DISCOUNT_FACTOR`. Treat both as estimates; for a production-scale campaign, cross-check
against the AWS Pricing Calculator or `describe-spot-price-history` (needs AWS credentials).

**If the live pricing fetch itself fails** (network restrictions on the Hub, endpoint down,
etc.), we fall back to a **hardcoded on-demand rate for `m5.xlarge`** (`$0.376/hr`, a snapshot of
US West (Oregon) pricing) so the cost section still produces a number instead of nothing. Edit
`FALLBACK_PRICE_PER_HR`/`FALLBACK_MACHINE_TYPE` below if that snapshot goes stale."""))

cells.append(code("""\
import json as _json
import urllib.request as _urlreq

EC2_PRICING_URL = "https://ec2instances.github.io/data/US%20West%20(Oregon).json"
# Spot is typically 60-70% cheaper than on-demand for these instance families;
# adjust to your observed rate (e.g. from the Jobs UI or a spot-advisor check).
SPOT_DISCOUNT_FACTOR = 0.3    # 0.3 = pay ~30% of on-demand (a ~70% Spot discount)
# Used only when a job's real machine_type isn't available from the platform.
# maap-dps-worker-8gb suggests an ~8 GB-class instance -- confirm with MAAP and
# edit this to match your queue's actual instance type for an accurate estimate.
ASSUMED_MACHINE_TYPE = "m5.xlarge"
# Used only if the live pricing fetch below fails outright. A snapshot rate,
# not live -- update it if it goes stale.
FALLBACK_MACHINE_TYPE = "m5.xlarge"
FALLBACK_PRICE_PER_HR = 0.376


def fetch_ec2_pricing(url=EC2_PRICING_URL):
    \"\"\"Fetch and index on-demand $/hr by instance type. Cached for this session.\"\"\"
    with _urlreq.urlopen(url, timeout=30) as resp:
        rows = _json.loads(resp.read())
    price_by_type = {}
    for row in rows:
        itype = row.get("Instance Type")
        price = row.get("price")
        if itype and price:
            try:
                price_by_type[itype] = float(price)
            except (TypeError, ValueError):
                continue
    return price_by_type


_used_fallback_price = False
try:
    _price_by_type = fetch_ec2_pricing()
    print(f"Fetched on-demand pricing for {len(_price_by_type)} instance types "
          f"(US West Oregon).")
except Exception as e:
    _price_by_type = {FALLBACK_MACHINE_TYPE: FALLBACK_PRICE_PER_HR}
    _used_fallback_price = True
    print(f"WARNING: could not fetch live EC2 pricing ({e}).")
    print(f"  Falling back to a hardcoded rate: {FALLBACK_MACHINE_TYPE} = "
          f"${FALLBACK_PRICE_PER_HR}/hr (snapshot, not live).")
    # If machine_type is also unavailable, make sure the assumed type used
    # below actually matches the one rate we have on hand.
    ASSUMED_MACHINE_TYPE = FALLBACK_MACHINE_TYPE

cost_by_record = {}
unknown_types = set()
_used_assumed_type = False
for i, m in job_metrics_by_record.items():
    if not m["duration_s"]:
        continue
    mtype = m["machine_type"]
    if not mtype:
        mtype = ASSUMED_MACHINE_TYPE
        _used_assumed_type = True
    rate = _price_by_type.get(mtype)
    if rate is None:
        unknown_types.add(mtype)
        continue
    cost_by_record[i] = (m["duration_s"] / 3600.0) * rate

print("=" * 60)
print("  AWS COST ESTIMATE")
print("=" * 60)
if _used_fallback_price:
    print(f"  NOTE: live EC2 pricing fetch failed -- using a hardcoded snapshot rate")
    print(f"  ({FALLBACK_MACHINE_TYPE} = ${FALLBACK_PRICE_PER_HR}/hr) instead.\\n")
if _used_assumed_type:
    print(f"  NOTE: real machine_type unavailable from this deployment -- costs below")
    print(f"  assume every job ran on '{ASSUMED_MACHINE_TYPE}'. Edit ASSUMED_MACHINE_TYPE")
    print(f"  above to match your queue's actual instance type for an accurate estimate.\\n")
if cost_by_record:
    on_demand_total = sum(cost_by_record.values())
    spot_total = on_demand_total * SPOT_DISCOUNT_FACTOR
    print(f"  Jobs priced          : {len(cost_by_record)}/{n_ok}"
          + (f"  (unknown types: {sorted(unknown_types)})" if unknown_types else ""))
    print(f"  On-demand equivalent : ${on_demand_total:,.2f}")
    print(f"  Spot estimate (~{SPOT_DISCOUNT_FACTOR*100:.0f}% of on-demand): "
          f"${spot_total:,.2f}")

    # Per-(basin, lsm) breakdown.
    by_stream = {}
    for i, cost in cost_by_record.items():
        key = (records[i]["basin"], records[i]["lsm"])
        by_stream.setdefault(key, 0.0)
        by_stream[key] += cost
    print("\\n  --- On-demand-equivalent cost per (basin, LSM) ---")
    for (basin, lsm), cost in sorted(by_stream.items()):
        print(f"    {basin:<10} {lsm:<6} on-demand ${cost:,.2f}  "
              f"spot ~${cost * SPOT_DISCOUNT_FACTOR:,.2f}")

    # Projection to a 30-year, all-streams run (same per-chunk rate assumption).
    if durations:
        mean_cost_per_chunk = on_demand_total / len(cost_by_record)
        proj_chunks = len(STREAMS) * (360 // CHUNK_MONTHS)
        proj_on_demand = mean_cost_per_chunk * proj_chunks
        print(f"\\n  30-year projection ({len(STREAMS)} stream(s) x "
              f"{360 // CHUNK_MONTHS} chunks):")
        print(f"    On-demand equivalent : ${proj_on_demand:,.2f}")
        print(f"    Spot estimate        : ${proj_on_demand * SPOT_DISCOUNT_FACTOR:,.2f}")
    print("\\n  NOTE: on-demand pricing overstates true Spot cost (typically 2-4x).")
    print("  Cross-check against the AWS Pricing Calculator or")
    print("  describe-spot-price-history for a production-scale campaign.")
else:
    print("  (No priceable jobs -- no succeeded jobs had a usable duration, the")
    print("   pricing fetch failed above, or ASSUMED_MACHINE_TYPE isn't in the")
    print("   pricing table -- check its spelling against 'Instance Type' values")
    print("   in the fetched pricing data.)")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 9: Discharge hydrographs per (basin, LSM) stream (full simulated span)

For every stream that finished, this stitches the per-chunk `Qout` files back into one continuous
series and plots discharge over the whole span (e.g. Jan–Dec). Each stream's **main-stem reach**
(the one with the highest mean discharge — effectively the outlet) is shown, so one line
summarizes it. The figure has one stacked panel per (basin, LSM) stream and scales to however
many were run (4 basins x 1 LSM in the sample, up to 61 basins x 3 LSMs)."""))

cells.append(code("""\
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta


def qout_url_from_qfinal(qfinal_s3_url):
    \"\"\"The Qout file sits in the same DPS output prefix as the Qfinal.\"\"\"
    return qfinal_s3_url.rsplit("/", 1)[0] + "/Qout_maap.nc4"


def collect_stream_series(records):
    \"\"\"Download each (basin, lsm) stream's per-chunk Qout and concatenate along time.

    Returns {(basin, lsm): (time[array], qout[np.ndarray time x reach], rivid)}.
    Chunks are ordered by chunk index so the timeline is chronological.
    \"\"\"
    by_stream = {}
    for r in sorted((r for r in records if r["status"] == "Succeeded" and r["qfinal"]),
                    key=lambda r: (r["basin"], r["lsm"], r["chunk"])):
        bkt, key = parse_dps_s3_url(qout_url_from_qfinal(r["qfinal"]))
        d = OUTPUT_DIR / r["basin"] / r["lsm"]
        d.mkdir(parents=True, exist_ok=True)
        local = d / f"Qout_chunk_{r['chunk']:03d}.nc4"
        if not local.exists():
            s3_ws.download_file(bkt, key, str(local))
        by_stream.setdefault((r["basin"], r["lsm"]), []).append(local)

    series = {}
    for key, files in by_stream.items():
        times, qouts, rivid = [], [], None
        for f in files:
            with netCDF4.Dataset(f, "r") as ds:
                times.append(np.array(ds.variables["time"][:]))
                qouts.append(np.array(ds.variables["Qout"][:]))
                if rivid is None:
                    rivid = np.array(ds.variables["rivid"][:])
        t = np.concatenate(times)
        q = np.concatenate(qouts, axis=0)
        order = np.argsort(t)                       # guard against any out-of-order chunk
        series[key] = (t[order], q[order], rivid)
    return series


series = collect_stream_series(records)
print(f"Assembled continuous Qout for {len(series)} stream(s): "
      f"{', '.join(f'{b}/{lsm}' for b, lsm in sorted(series))}")

if series:
    n = len(series)
    fig, axes = plt.subplots(n, 1, figsize=(12, 3.2 * n), sharex=False)
    if n == 1:
        axes = [axes]
    epoch = datetime(1970, 1, 1)

    for ax, (basin, lsm) in zip(axes, sorted(series)):
        t, q, rivid = series[(basin, lsm)]
        dates = [epoch + timedelta(seconds=int(s)) for s in t]
        main = int(np.argmax(q.mean(axis=0)))       # main-stem reach = highest mean flow
        ax.plot(dates, q[:, main], linewidth=1.3, color="#1f77b4")
        ax.fill_between(dates, 0, q[:, main], alpha=0.15, color="#1f77b4")
        ax.set_title(f"{basin} [{lsm}] — main-stem reach {int(rivid[main])} "
                     f"({len(rivid):,} reaches, {len(dates)} steps)", fontsize=10)
        ax.set_ylabel("Discharge (m3 s-1)")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))   # month labels

    axes[-1].set_xlabel("Month")
    fig.suptitle(f"RAPID2 discharge hydrographs — {CAMPAIGN}", fontsize=13, y=1.0)
    plt.tight_layout()
    png = OUTPUT_DIR / f"hydrographs_{CAMPAIGN}.png"
    plt.savefig(str(png), dpi=150, bbox_inches="tight")
    plt.show()
    print(f"Saved {png}")
else:
    print("No completed streams to plot — run Step 6 first.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Step 10: Animated basin-location map (showcase basins)

A compact visual summary for the showcase basins in `BASINS_TO_RUN` (defaults to Mississippi
`pfaf_74`, Columbia `pfaf_76`, Amazon `pfaf_62`, Danube `pfaf_22`): each basin's main-stem reach
(the same reach Step 9 plots) is placed as a dot on a flat world map at its outlet coordinates,
and the dot's size/color pulses month-by-month with that reach's monthly-mean discharge.

Rendered with `matplotlib` + `cartopy` (no browser/Chrome dependency — an earlier Plotly +
`kaleido` version was abandoned here because `kaleido`'s bundled headless Chrome failed to launch
inside the MAAP Hub container). `cartopy` needs its `cartopy.feature` coastline/ocean/border
shapefiles, which download from Natural Earth on first use if not already cached — if the Hub
blocks that host, this step will fail the same way the Chrome approach did, just for a different
reason; there's no local fallback for that case today.

Discharge magnitude varies enormously across basins (the Amazon's mean flow is roughly one to two
orders of magnitude larger than the Danube's). Marker size/color use **one shared log-scaled
range across all basins**, not a per-basin scale — so the Amazon's dot is genuinely, visibly
bigger than the Danube's (true to reality), while a size cap (`MAX_SIZE` below) keeps it from
dominating the map. This trades off some of each basin's own month-to-month pulsing visibility
for an honest cross-basin size comparison; the hydrograph panel above (Step 9) is where absolute
magnitudes are read precisely, in physical units."""))

cells.append(code("""\
# cartopy (coastlines/land/ocean) + imageio (GIF stitching) are only needed
# for this step's basin-location map, not the core RAPID2 routing path, so
# installed separately from rapid2 itself, right where first used. matplotlib
# is already used by Step 9. cartopy's own compiled dependencies (GEOS, PROJ)
# are commonly preinstalled via conda on geospatial-oriented images -- if this
# pip install fails, try `conda install -c conda-forge cartopy` instead.
!pip install -q "cartopy>=0.23.0" "imageio>=2.36.0" """))

cells.append(code("""\
import pyarrow.parquet as pq
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import imageio.v2 as imageio

BASIN_LABELS = {
    "pfaf_74": "Mississippi", "pfaf_76": "Columbia",
    "pfaf_62": "Amazon", "pfaf_22": "Danube",
}


def basin_outlet_lonlat(basin, rivid_order, main_idx):
    \"\"\"(lon, lat) of the main-stem reach Step 9 already picked for this basin.

    Re-reads crd_<basin>.parquet (riv -> lon/lat) and looks up main_idx's river
    ID there, so the map dot and the Step 9 hydrograph line refer to the
    exact same physical reach -- rather than independently re-deriving "the
    outlet" from con's dwn==0 topology, which can have >1 candidate reach for
    a basin with multiple river-mouths/deltas. Returns (None, None) with a
    warning if the ID isn't found (a static-file mismatch).
    \"\"\"
    st = basin_static(basin)
    main_riv = int(rivid_order[main_idx])
    crd = pq.read_table(st["crd"], columns=["riv", "lon", "lat"])
    riv = crd.column("riv").to_numpy()
    match = np.where(riv == main_riv)[0]
    if len(match) == 0:
        print(f"  WARNING: riv {main_riv} ({basin}) not found in {st['crd'].name}")
        return None, None
    i = match[0]
    return float(crd.column("lon").to_numpy()[i]), float(crd.column("lat").to_numpy()[i])


def monthly_mean_discharge(t, q, main_idx, months):
    \"\"\"One mean-discharge scalar per calendar month, for q[:, main_idx].

    t is seconds-since-epoch (as returned by collect_stream_series); months is
    the notebook's MONTHS list, so every basin's series lines up to the same
    frame index even if a stream's exact timestamps differ slightly (e.g. a
    retried chunk). NaN for any month with zero matching timesteps.
    \"\"\"
    epoch = datetime(1970, 1, 1)
    dates = np.array([epoch + timedelta(seconds=int(s)) for s in t])
    ym = np.array([f"{d.year}-{d.month:02d}" for d in dates])
    return [float(q[ym == mo, main_idx].mean()) if (ym == mo).any() else float("nan")
            for mo in months]


def log_normalize_shared(rows):
    \"\"\"Map every basin's discharge onto ONE log-scaled 0..1 range.

    Unlike a per-basin min-max (where a small river's dot can look as big as
    a huge one at their respective peaks), this keeps dot size meaningful
    across basins: the Amazon's dot is genuinely, visibly bigger than the
    Danube's. log10() compresses a >10x discharge gap into something that
    still fits on one map; MAX_SIZE below caps the biggest dot so it can
    never dominate the plot.
    \"\"\"
    all_q = np.concatenate([np.asarray(r["monthly_q"], dtype=float) for r in rows])
    all_q = all_q[~np.isnan(all_q) & (all_q > 0)]
    log_q = np.log10(all_q)
    lo, hi = log_q.min(), log_q.max()
    for r in rows:
        q = np.asarray(r["monthly_q"], dtype=float)
        with np.errstate(divide="ignore"):
            lq = np.where(q > 0, np.log10(np.where(q > 0, q, 1)), lo)
        r["norm"] = np.clip((lq - lo) / (hi - lo), 0, 1) if hi > lo else np.full_like(lq, 0.5)


# --- Resolve each showcase basin's main-stem reach, outlet coords, monthly series ---
globe_rows = []
for basin in BASINS_TO_RUN:
    key = next(((b, lsm) for (b, lsm) in series if b == basin), None)
    if key is None:
        print(f"  Skipping {basin}: no completed stream in `series`.")
        continue
    t, q, rivid = series[key]
    main_idx = int(np.argmax(q.mean(axis=0)))        # same pick as Step 9
    lon, lat = basin_outlet_lonlat(basin, rivid, main_idx)
    if lon is None:
        continue
    monthly_q = monthly_mean_discharge(t, q, main_idx, MONTHS)
    globe_rows.append({"basin": basin, "label": BASIN_LABELS.get(basin, basin),
                       "lon": lon, "lat": lat, "monthly_q": monthly_q})

print(f"Resolved {len(globe_rows)}/{len(BASINS_TO_RUN)} showcase basin(s) for the map.")

if globe_rows:
    MIN_SIZE, MAX_SIZE = 60, 500      # scatter marker *area* (points^2), matplotlib's s=
    log_normalize_shared(globe_rows)
    for row in globe_rows:
        row["size"] = MIN_SIZE + row["norm"] * (MAX_SIZE - MIN_SIZE)

    MAP_DIR = OUTPUT_DIR / "map_frames"
    MAP_DIR.mkdir(exist_ok=True)
    cmap = plt.get_cmap("Blues")

    frame_paths = []
    for i, month in enumerate(MONTHS):
        fig = plt.figure(figsize=(10, 6))
        ax = plt.axes(projection=ccrs.PlateCarree())
        ax.set_global()
        ax.add_feature(cfeature.LAND, facecolor="#ebebeb")
        ax.add_feature(cfeature.OCEAN, facecolor="#e6f5ff")
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5, edgecolor="#555555")
        ax.add_feature(cfeature.BORDERS, linewidth=0.3, edgecolor="#888888")
        ax.gridlines(draw_labels=False, linewidth=0.2, color="#cccccc")

        lons = [r["lon"] for r in globe_rows]
        lats = [r["lat"] for r in globe_rows]
        sizes = [r["size"][i] for r in globe_rows]
        colors = [r["norm"][i] for r in globe_rows]
        ax.scatter(lons, lats, s=sizes, c=colors, cmap=cmap, vmin=0, vmax=1,
                   edgecolors="darkslategray", linewidths=0.8,
                   transform=ccrs.PlateCarree(), zorder=5)

        for r in globe_rows:
            ax.text(r["lon"], r["lat"] + 4, f"{r['label']}: {r['monthly_q'][i]:.0f} m3/s",
                     ha="center", fontsize=8, transform=ccrs.PlateCarree(), zorder=6)

        ax.set_title(f"Monthly discharge — {CAMPAIGN} ({month})", fontsize=12)
        png_path = MAP_DIR / f"frame_{i:02d}.png"
        fig.savefig(str(png_path), dpi=120, bbox_inches="tight")
        plt.close(fig)
        frame_paths.append(png_path)

    gif_path = OUTPUT_DIR / f"basin_map_{CAMPAIGN}.gif"
    images = [imageio.imread(p) for p in frame_paths]
    imageio.mimsave(str(gif_path), images, duration=1000, loop=0)   # ms per frame
    print(f"Saved {len(frame_paths)}-frame GIF: {gif_path} "
          f"({gif_path.stat().st_size / 1e6:.2f} MB)")
else:
    print("No showcase basins resolved — nothing to animate.")"""))

# ---------------------------------------------------------------------------
cells.append(md("""\
## Disk usage summary

How much disk would this run need to archive for reproducibility? We report the real size of
every file RAPID2's own code (`cpllsm`, `zeroqinit`, `rapid2` routing) consumed or produced —
`staged_input_paths` (built up by `stage_chunk_inputs` in Step 5) for input, plus the downloaded
`Qout`/`Qfinal` files for output. This deliberately **excludes notebook-only scratch**: the
monthly `Qext` pieces superseded the instant `chunk_qext` concatenates them, the raw GLDAS
monthly downloads (already permanently public at NASA GES DISC), and the Zenodo zip bundles
(already permanently archived at Zenodo record 20672740) — none of those are needed a second
time to reproduce a result, so counting them would overstate what an archive actually needs to
store.

This measurement needs `Qout` to already be downloaded, so it must run **after** Step 9's
`collect_stream_series`, not right after Step 7's `download_qfinals` — an earlier version of this
cell measured `output/` too early and only saw the small `Qfinal` checkpoints, undercounting real
output by ~400x.

If this deployment populates `job.directory_size` (not all do), we also show that figure — the
DPS worker's own working-directory size, a close but not identical cross-check (it reflects only
what one job's worker touched, not whether `crd`/`cpl` were part of that specific job's inputs)."""))

cells.append(code("""\
def human_bytes(n):
    \"\"\"Render a byte count as e.g. '4.2 GB'.\"\"\"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024


def minimum_footprint_input_bytes(paths):
    \"\"\"Total bytes of every file RAPID2's own code actually consumed, per
    staged_input_paths (Step 5) -- not a folder walk, so notebook-only scratch
    (superseded monthly Qext pieces, raw GLDAS downloads, Zenodo zips) is
    never counted.\"\"\"
    return sum(p.stat().st_size for p in paths if p.exists())


def output_bytes_for(records):
    \"\"\"Total bytes of every downloaded Qout/Qfinal file for succeeded chunks.
    Must run after collect_stream_series has downloaded Qout (Step 9) --
    before that, output/ only has the small Qfinal checkpoints.\"\"\"
    total = 0
    for r in records:
        if r["status"] != "Succeeded":
            continue
        d = OUTPUT_DIR / r["basin"] / r["lsm"]
        for pattern in (f"Qout_chunk_{r['chunk']:03d}.nc4",
                       f"Qfinal_chunk_{r['chunk']:03d}.nc4"):
            p = d / pattern
            if p.exists():
                total += p.stat().st_size
    return total


print("=" * 60)
print("  DISK USAGE (minimum reproducibility footprint)")
print("=" * 60)

min_input_bytes = minimum_footprint_input_bytes(staged_input_paths)
min_output_bytes = output_bytes_for(records)
min_total_bytes = min_input_bytes + min_output_bytes
print(f"  Input  (Qext, Q00/Qinit, con/kpr/xpr/bas/crd/cpl parquet): "
      f"{human_bytes(min_input_bytes)}")
print(f"  Output (Qout, Qfinal)                                    : "
      f"{human_bytes(min_output_bytes)}")
print(f"  Total                                                    : "
      f"{human_bytes(min_total_bytes)}")
if min_total_bytes:
    print(f"  Input:output ratio: {min_input_bytes / max(min_output_bytes, 1):.2f} : 1")

# Projection to a 30-year, all-streams run (same per-chunk rate assumption as
# the cost/timing projections above).
n_chunks_done = sum(1 for r in records if r["status"] == "Succeeded")
if n_chunks_done:
    mean_per_chunk = min_total_bytes / n_chunks_done
    proj_chunks = len(STREAMS) * (360 // CHUNK_MONTHS)
    print(f"\\n  30-year projection ({len(STREAMS)} stream(s) x "
          f"{360 // CHUNK_MONTHS} chunks): ~{human_bytes(mean_per_chunk * proj_chunks)}")

# Cross-check against the DPS platform's own working-directory size, if
# this deployment populates it (some don't -- see Step 8's diagnostic).
disk_by_record = {i: m["directory_size"] for i, m in job_metrics_by_record.items()
                  if m["directory_size"]}
if disk_by_record:
    platform_total = sum(disk_by_record.values())
    print(f"\\n  Cross-check -- job.directory_size (DPS worker's own working dir, "
          f"{len(disk_by_record)}/{n_ok} jobs): {human_bytes(platform_total)}")
    print("  (May differ slightly: this is per-worker footprint, not the")
    print("   deduplicated minimum-footprint figure above.)")
else:
    print("\\n  (job.directory_size not populated on this deployment -- no")
    print("   platform-side cross-check available; see Step 8's diagnostic.)")"""))

nb = {"cells": cells,
      "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python",
                                  "name": "python3"},
                   "language_info": {"name": "python"}},
      "nbformat": 4, "nbformat_minor": 5}

out = sys.argv[1] if len(sys.argv) > 1 else "rapid2_tutorial_maap.ipynb"
with open(out, "w") as f:
    json.dump(nb, f, indent=1)
print(f"wrote {out} ({len(cells)} cells)")
