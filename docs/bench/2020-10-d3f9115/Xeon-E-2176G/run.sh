#!/bin/bash
#
# git checkout d3f9115cb3f3678b528a0b9a263168a0866c805d
#
# To run on a remote host
# GOOS=linux GOARCH=amd64 go test -c ./vec -o vectest
# scp vectest run.sh host:~/
# scp host:~/*.txt .
# benchstat original.txt unorderd.txt perm.txt perm-loop.txt perm-loop-unroll.txt avx512.txt

# in the relevant branch the repo was at an old name
ARGS="-test.cpu 1 -test.count 10 -test.bench "

./vectest $ARGS Uint64EqualAVX2\$ > original.txt

./vectest $ARGS Uint64EqualAVX2Unopt > unorderd.txt
sed -i 's/Unopt//g' unorderd.txt

./vectest $ARGS Uint64EqualAVX2New\$ > perm.txt
sed -i 's/New//g' perm.txt

./vectest $ARGS Uint64EqualAVX2Newer > perm-loop.txt
sed -i 's/Newer//g' perm-loop.txt

./vectest $ARGS Uint64EqualAVX2Unroll > perm-loop-unroll.txt
sed -i 's/Unroll//g' perm-loop-unroll.txt

# ./vectest $ARGS Uint64EqualAVX512 > avx512.txt
# sed -i 's/AVX512/AVX2/g' avx512.txt


