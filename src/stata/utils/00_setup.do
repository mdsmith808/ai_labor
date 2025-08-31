*******************************************************
* utils/00_setup.do — globals, ado installs, defaults
*******************************************************
version 15.0
set more off
set scheme s1color

cap mkdir "$res"
cap mkdir "$res/figures"

* Stata 15-safe ado installs
capture which reghdfe
if _rc ssc install reghdfe, replace
capture which boottest
if _rc ssc install boottest, replace
capture which bacondecomp
if _rc ssc install bacondecomp, replace
capture which eventstudyinteract
if _rc ssc install eventstudyinteract, replace
capture which drdid
if _rc ssc install drdid, replace

display as text "setup ok. project: $proj"
