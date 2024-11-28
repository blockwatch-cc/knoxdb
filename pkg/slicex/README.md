# Package SliceX

This package contains extended features over the standard Go slices package which helps perform operations on unique and sorted slices.

### SliceX (this package)

```go
import "blockwatch.cc/knoxdb/pkg/slicex"

func NewOrderedNumbers(x S) *OrderedNumbers
func (o *OrderedNumbers) SetNonZero() *OrderedNumbers
func (o *OrderedNumbers) SetUnique() *OrderedNumbers
func (o *OrderedNumbers) Insert(v ...E) *OrderedNumbers
func (o *OrderedNumbers) Remove(v ...E) *OrderedNumbers
func (o OrderedNumbers) Index(v E) int
func (o OrderedNumbers) IndexStart(val T, start int) (int, bool) {
func (o OrderedNumbers) Contains(v E) bool
func (o OrderedNumbers) ContainsAny(v ...E) bool
func (o OrderedNumbers) ContainsAll(v ...E) bool
func (o OrderedNumbers) ContainsRange(a, b T) bool
func (o OrderedNumbers) MinMax() (min T, max T) {
func (o OrderedNumbers) Intersect(v *OrderedNumbers) *OrderedNumbers

func NewOrderedStrings(x S) *OrderedStrings
...

func NewOrderedBytes(x S) *OrderedBytes
...
```

### Go slices (official)

```go
import "slices"

func BinarySearch(x S, target E) (int, bool)
func BinarySearchFunc(x S, target T, cmp func(E, T) int) (int, bool)
func Clip(s S) S
func Clone(s S) S
func Compact(s S) S
func CompactFunc(s S, eq func(E, E) bool) S
func Compare(s1, s2 S) int
func CompareFunc(s1 S1, s2 S2, cmp func(E1, E2) int) int
func Contains(s S, v E) bool
func ContainsFunc(s S, f func(E) bool) bool
func Delete(s S, i, j int) S
func DeleteFunc(s S, del func(E) bool) S
func Equal(s1, s2 S) bool
func EqualFunc(s1 S1, s2 S2, eq func(E1, E2) bool) bool
func Grow(s S, n int) S
func Index(s S, v E) int
func IndexFunc(s S, f func(E) bool) int
func Insert(s S, i int, v ...E) S
func IsSorted(x S) bool
func IsSortedFunc(x S, cmp func(a, b E) int) bool
func Max(x S) E
func MaxFunc(x S, cmp func(a, b E) int) E
func Min(x S) E
func MinFunc(x S, cmp func(a, b E) int) E
func Replace(s S, i, j int, v ...E) S
func Reverse(s S)
func Sort(x S)
func SortFunc(x S, cmp func(a, b E) int)
func SortStableFunc(x S, cmp func(a, b E) int)
```

## Algorithms

Vector algorithms for sorted slices based on binary search from Go's sort package. All ordered/comparable types `constraints.Ordered` (signed, unsigned, float, string, arrays) are supported by generic functions and special types via `OrderedBytes` and `OrderedStrings`.

Algorithms are available for
- `removeZeros[T](s []T) []T`
- `removeDuplicates(s []T) []T`
- `contains[T](s []T, e T, optimzed bool) bool`
- `index[T](s []T, e T, last int, optimzed bool) (int, bool)`
- `containsRange[T](s []T from, to T) bool`
- `intersect[T](x, y, out []T) []T`
- `merge[T](s [T], unique bool, v ...T) []T`

## Range coverage algorithm

Checks if a sparse sorted slice contains any value(s) in the closed interval `[from, to]`. This is used when deciding whether a pack contains any of the values from an IN condition based on the packs min/max range.
```
val slice ->       |- - - - - - - - - -|
                   .                   .
Range A      [--]  .                   .
Range B.1       [--]                   .
Range B.2      [-------]               .
Range B.3          [--]                .
Range C.1          .       [--]        .            // some values in range
Range C.2          .       [--]        .            // no values in range
Range D.1          .                [--]
Range D.2          .               [-------]
Range D.3          .                   [--]
Range E            .                   .  [--]
Range F     [-----------------------------------]
```