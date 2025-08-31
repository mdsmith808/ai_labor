*******************************************************
* 06_snapshot_and_figs.do — quick figures & snapshots
*******************************************************
version 15.0
clear all
set more off

* ---- Exposure snapshot (SOC-level) ----
capture confirm file "$res/exposure_snapshot.csv"
if _rc {
    di as error "X missing $res/exposure_snapshot.csv — run 03_onet_join_exposure.do first."
    exit 601
}
import delimited using "$res/exposure_snapshot.csv", clear varnames(1) case(lower)

capture confirm variable exposure_v01
if _rc {
    di as error "X exposure_v01 missing in snapshot."
    exit 198
}

* Histogram (unweighted SOC-level)
hist exposure_v01, width(0.02) freq ///
    title("exposure v0.1 (SOC-level)") name(h_exposure, replace)
graph export "$res/figures/exposure_hist.png", replace width(1400) as(png)

* Top/bottom deciles table
egen decile = xtile(exposure_v01), n(10)
preserve
keep soc exposure_v01 decile
keep if inlist(decile,1,10)
gsort decile -exposure_v01
export delimited using "$res/exposure_top_bottom_deciles.csv", replace
restore

* ---- (Optional) Person-weighted distribution from CPS merge ----
capture confirm file "$res/cps_onet_merged.dta"
if !_rc {
    preserve
    use "$res/cps_onet_merged.dta", clear
    rename asecwt wgt
    capture confirm numeric variable wgt
    if _rc destring wgt, replace force
    capture confirm variable exposure_v01
    if !_rc {
        * weighted quantiles
        xtile exp_p = exposure_v01 [pw=wgt], n(100)
        collapse (count) n = exposure_v01 (sum) wsum = wgt, by(exp_p)
        export delimited using "$res/exposure_weighted_percentiles.csv", replace
    }
    restore
}

di as result "saved: $res/figures/exposure_hist.png, $res/exposure_top_bottom_deciles.csv (and weighted percentiles if CPS merge present)"
