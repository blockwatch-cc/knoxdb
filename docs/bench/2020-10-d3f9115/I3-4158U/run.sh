#!/bin/bash
#
# git checkout d3f9115cb3f3678b528a0b9a263168a0866c805d
#
# Note: sed on OSX requires an empty argument for in-place backup extension, Linux doesnt
#
# benchstat original.txt unorderd.txt perm.txt perm-loop.txt perm-loop-unroll.txt
#

# in the relevant branch the repo was at an old name
PACKAGE=blockwatch.cc/packdb-pro/vec
ARGS="-test.cpu 1 -test.count 10 -test.bench "

go test $PACKAGE $ARGS Uint64EqualAVX2\$ > original.txt

go test $PACKAGE $ARGS Uint64EqualAVX2Unopt > unorderd.txt
sed -i "" 's/Unopt//g' unorderd.txt

go test $PACKAGE $ARGS Uint64EqualAVX2New\$ > perm.txt
sed -i "" 's/New//g' perm.txt

go test $PACKAGE $ARGS Uint64EqualAVX2Newer > perm-loop.txt
sed -i "" 's/Newer//g' perm-loop.txt

go test $PACKAGE $ARGS Uint64EqualAVX2Unroll > perm-loop-unroll.txt
sed -i "" 's/Unroll//g' perm-loop-unroll.txt

# go test ./vec/ $ARGS Uint64EqualAVX512 > avx512.txt

