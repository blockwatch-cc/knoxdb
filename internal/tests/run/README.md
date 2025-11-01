# Scenario test runner

Arguments
```
  -arch string
        test with arch wasm/native (default "native")
  -count int
        number of iterations using different random seeds (default 1)
  -max-error-rate float
        stops the test runner when the rate of errors observed per second is greater than N (inclusive) (default 10)
  -max-errors int
        stop the test runner after N total observed errors (default 1)
  -race
        enable race detector
  -seed string
        comma separated list of random seeds
  -v    enable test log streaming
```

Example
```sh
go run -buildvcs=true ./internal/tests/run/ -v
```
