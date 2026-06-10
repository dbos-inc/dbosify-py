# Conformance tests

From Phase 1 onward (see DESIGN.md §9), this directory holds the harness that
clones [temporalio/samples-python](https://github.com/temporalio/samples-python),
mechanically rewrites `temporalio` imports to `temporal_dbos`, runs each
sample's worker + starter against a fresh Postgres database, and asserts
outputs. The resulting pass-rate table is the product's headline number.
