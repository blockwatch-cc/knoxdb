// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"unsafe"
)

// A Go slice header up until at least Go 1.26 assuming
// Go's internal representation remains unchanged.
//
// This is currently the only way to cast an unsafe.Pointer
// to a struct field offset back to the slice at this offset
// because reflect.SliceAt requires data pointer and length.
type sliceType struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

var ptrSize = unsafe.Sizeof(uintptr(0))

// iface is a type similar to Go's internal interface representation
type iface struct {
	itab unsafe.Pointer // Pointer to interface table (type + method pointers)
	data unsafe.Pointer // Pointer to the concrete value
}

// getMarshalerType extracts a type's interface table from any.
// although this is a Go runtime internal it is a faster way to
// call interface methods on a reflect slice. The alternative
// would be:
//
//	for i := range rslice.Len() {
//		 rslice.Index(i).Interface().(schema.Marshaler).MarshalSchema(...)
//	}
// func getMarshalerType(val any) (unsafe.Pointer, bool) {
// 	mi, ok := val.(schema.Marshaler)
// 	if !ok {
// 		return nil, false
// 	}
// 	// Cast the interface to our internal struct to access its itab
// 	i := (*iface)(unsafe.Pointer(&mi))
// 	return i.itab, ok
// }

// getSliceElemMarshalerType returns the slice element's type
// interface table. Use as
//
//	if itab, ok := getSliceElemMarshalerType(rslice); ok {
//		err := e.marshalSlice(
//			itab,
//			rslice.UnsafePointer(),
//			rslice.Type().Elem().Size(),
//			rslice.Len(),
//			buf,
//		)
//		if err != nil {
//			return nil, err
//		}
//		return buf.Bytes(), nil
//	}
// func getSliceElemMarshalerType(rslice reflect.Value) (unsafe.Pointer, bool) {
// 	if rslice.Len() == 0 {
// 		return nil, false
// 	}
// 	return getMarshalerType(rslice.Index(0).Interface())
// }
