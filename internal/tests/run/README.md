# Scenario test runner

Runs tests scenarios using Deterministic Simulation Testing (DST) with user defined random seeds which control the Go runtime. It requires single-threaded scheduling and some tweaks to the Go runtime. For this reason DST only works "deterministic" inside WASM.

Read more about DST at https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go


### Test runner

```sh
go run -buildvcs=true ./internal/tests/run/ -v -seed 0x123456789ABCDEF0 -logs ./retain-logs-here


Arguments
  -arch string
      test with arch wasm/native (default "native")
  -count int
      number of iterations using different random seeds (default 1)
  -cpu int
      number of CPU cores to use for running tests (default all)
  -logs string
      output path for test failure logs
  -max-error-rate float
      stops the test runner when the rate of errors observed per second is greater than N (inclusive) (default 10)
  -max-errors int
      stop the test runner after N total observed errors (default 1)
  -race
      enable race detector
  -run string
      regex to select workload to run
  -seed string
      comma separated list of random seeds (uint64, each min 16 char long)
  -timeout duration
      test run timeout (will abort and trace test run) (default 1m0s)
  -v  enable test log streaming
```

Example
```sh
go run -buildvcs=true ./internal/tests/run/ -v
```
