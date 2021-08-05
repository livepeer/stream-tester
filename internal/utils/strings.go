package utils

func StringsArrayContains(strs []string, val string) bool {
	for _, v := range strs {
		if v == val {
			return true
		}
	}
	return false
}

func StringsArraysEq(ar1, ar2 []string) bool {
	if len(ar1) != len(ar2) {
		return false
	}
	for _, v1 := range ar1 {
		if !StringsArrayContains(ar2, v1) {
			return false
		}
	}
	return true
}
