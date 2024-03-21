/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tlssupport

// extensionID
var extensionID = getPrefixedExtensionID([]int{1, 1})

// extensionPrefix
var extensionPrefix = []int{1, 3, 6, 1, 4, 1, 53594}

// getPrefixedExtensionID
func getPrefixedExtensionID(suffix []int) []int {
	return append(extensionPrefix, suffix...)
}

// extensionIDEqual
func extensionIDEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
