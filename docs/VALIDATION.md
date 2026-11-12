# Release checks

Checks performed when preparing this source release:

- All eleven shell scripts under `scripts/epoch_benchmark/` pass `bash -n`.
- Every path, setting and script named in `README.md` and in these documents exists in the tree.
- No reference to the removed `dynamic-net` module, to the deleted helper scripts, or to unpublished files remains in the source, the scripts or the documents.
- The DBR target-epoch formula, the admission budget, the per-target quota, the CRDT version order, and the PA session fields described in [implementation details](IMPLEMENTATION.md) were read back from the code, not from an earlier design note.
- The release excludes private host configuration, credentials, generated binaries, dependencies, database snapshots, caches and experiment logs.

A fresh native build and a three-host TPC-C run were **not** performed as part of this packaging. The build requires the matching openGauss binarylibs and a ZeroMQ prefix (see [dependency preparation](DEPENDENCIES.md)); the run requires three hosts and BenchmarkSQL. Earlier deployment results are not presented as validation of this tree.

No historical performance numbers are asserted by this code-only release.
