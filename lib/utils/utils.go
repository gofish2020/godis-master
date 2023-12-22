package utils

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// ToCmdLine2 convert commandName and string-type argument to [][]byte
func ToCmdLine2(commandName string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = []byte(s)
	}
	return result
}

// ToCmdLine3 convert commandName and []byte-type argument to CmdLine
func ToCmdLine3(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = s
	}
	return result
}

// Equals check whether the given value is equal
func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

// ConvertRange converts redis index to go slice index
// -1 => size-1
// both inclusive [0, 10] => left inclusive right exclusive [0, 9)
// out of bound to max inbound [size, size+1] => [-1, -1]
func ConvertRange(start int64, end int64, size int64) (int, int) {
	if start < -size { // 负数从 -1开始一直到 -size
		return -1, -1
	} else if start < 0 { // -1 表示 尾部的 最后一个元素 size-1
		start = size + start
	} else if start >= size { // 正数从0 开始一直到 size-1
		return -1, -1
	}

	if end < -size { // 负数从 -1开始一直到 -size [-size,-1]
		return -1, -1
	} else if end < 0 { // end为负数，转成整数并且+1， 因为要包括end这个位置，所以+1是为了包括 end
		end = size + end + 1
	} else if end < size { // end 没有越界，end+1
		end = end + 1
	} else { // end 如果越界，直接用size
		end = size
	}
	if start > end { // 最后的结果 start <= end
		return -1, -1
	}
	// 这里的end不会被使用 返回的边界范围为：[start,end）
	return int(start), int(end)
}
