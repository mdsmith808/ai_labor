*******************************************************
* 00_master.do  — one-command rebuild (Week 1)
* Stata 15-safe. Run me from project root or Do-file Editor.
*******************************************************
version 15.0
clear all
set more off

cd ~/Research/ai_labor
pwd

* Project globals
global PROJ "`c(pwd)'"          // or hardcode your path
global DAT  "$PROJ/data"
global RES  "$PROJ/results"
global DOCS "$PROJ/docs"

cap mkdir "$RES"
cap mkdir "$RES/figures"
cap mkdir "$RES/logs"

* Log
local ts = subinstr("`c(current_date)'", " ", "_", .) + "_" + subinstr("`c(current_time)'", ":", "", .)
log using "$RES/logs/00_master_`ts'.smcl", replace

* 1) setup (ado install, options)
do "src/stata/utils/00_setup.do"

* 2) CPS ingest (slim with weights)
do "src/stata/01_cps_ingest.do"

* 3) OCC -> SOC mapping (weighted dominant SOC)
do "src/stata/02_occ_to_soc.do"

* 4) Join O*NET & build Exposure v0.1
do "src/stata/03_onet_join_exposure.do"

* 5) JOLTS import + merge (context)
do "src/stata/04_jolts_import_merge.do"

* 6) LEHD/LODES import + merge (context)
do "src/stata/05_lehd_import_merge.do"

* 7) Snapshots & quick figures (for dashboard)
do "src/stata/06_snapshot_and_figs.do"

log close
display as result "✅ Done. See: $RES and $RES/figures"
