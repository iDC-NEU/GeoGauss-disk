# Dependency preparation

Use Linux. The supported base platforms are the openGauss 6.0.0 ones: CentOS 7.6 (x86_64) and openEuler 20.03 LTS (aarch64). The historical deployment ran CentOS 7 with GCC 10.3 taken from the `binarylibs` bundle; a fresh build on a current distribution has not been validated by this packaging task.

Install the upstream build dependencies from the distribution media or repository:

| Package | Version used upstream |
| --- | --- |
| libaio-devel | 0.3.109-13 |
| flex | 2.5.31 or later |
| bison | 2.7-4 |
| ncurses-devel | 5.9-13.20130511 |
| glibc-devel | 2.17-111 |
| patch | 2.7.1-10 |
| lsb_release | 4.1 |

CMake is required for the `--cmake` build path; the compiler is GCC 10.3 from `binarylibs` (`buildtools/gcc10.3`), which the build and the harness scripts put on `PATH` and `LD_LIBRARY_PATH`.

The source base must be openGauss 6.0.0 together with the matching `openGauss-third_party_binarylibs`. The bundle can be compiled from the `openGauss-third_party` repository or downloaded prebuilt, but the version must match the source tree; the object layout and the generated headers differ between releases.

The epoch module needs ZeroMQ 4.x. Only the runtime library is required; the C++ binding header ships as `src/include/zmq.hpp`.

```text
<zmq-prefix>/
  lib/libzmq.so
  lib/libzmq.so.5          # SONAME used at run time
```

The prefix is the CMake cache variable `EPOCH_ZMQ_ROOT`, whose default is `/data/geogauss/zmqroot`. Pass it explicitly when configuring, for example `cmake -DEPOCH_ZMQ_ROOT=/opt/zmq`. The configure step prints `epoch: enable ZMQ from ...` when the library is found. Without it the module still compiles, but `EPOCH_USE_ZMQ` stays undefined, every publish call returns false, and cluster mode cannot run.

TPC-C is driven by BenchmarkSQL 5.0, in a locally modified `benchmarksql-5.0-epoch` build. It is not bundled. Install a JDK, then point `EPOCH_BENCHMARKSQL` in `scripts/epoch_benchmark/env.sh` at its `run` directory.

YCSB uses the bundled JDBC client. It needs a JDK and a JDBC jar pointed to by `EPOCH_JDBC_JAR`; the driver class defaults to `org.postgresql.Driver` and can be changed with `EPOCH_JDBC_DRIVER`. No YCSB installation is required, and `run_ycsb.sh load` creates the ten-column table.

Network shaping uses `iproute2` (`tc`, netem, tbf) and needs root or passwordless sudo on the shaped host. Cluster preparation needs key-based SSH between the driver host and the others.

Build from the repository root:

```bash
sh build.sh -m release -3rd /path/to/openGauss-third_party_binarylibs --cmake
```

`-m` selects `release`, `debug`, or `memcheck`; the experiments used `release`. `-3rd` must be an absolute path. With `--cmake` the binary is `tmp_build/src/gausskernel/gaussdb`; without it the build takes the autoconf path and installs under `dest/`.

Use identical binaries and library versions on all hosts.
