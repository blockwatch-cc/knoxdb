# Scenario test runner

Arguments
```
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
