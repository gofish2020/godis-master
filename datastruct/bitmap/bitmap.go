package bitmap

type BitMap []byte

func New() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

func toByteSize(bitSize int64) int64 {
	if bitSize%8 == 0 {
		return bitSize / 8
	}
	return bitSize/8 + 1
}

func (b *BitMap) grow(bitSize int64) {
	byteSize := toByteSize(bitSize)
	gap := byteSize - int64(len(*b))
	if gap <= 0 {
		return
	}
	*b = append(*b, make([]byte, gap)...)
}

func (b *BitMap) BitSize() int {
	return len(*b) * 8
}

func FromBytes(bytes []byte) *BitMap {
	bm := BitMap(bytes)
	return &bm
}

func (b *BitMap) ToBytes() []byte {
	return *b
}

func (b *BitMap) SetBit(offset int64, val byte) {
	byteIndex := offset / 8
	bitOffset := offset % 8
	mask := byte(1 << bitOffset)
	b.grow(offset + 1)
	if val > 0 {
		// set bit
		(*b)[byteIndex] |= mask
	} else {
		// clear bit
		(*b)[byteIndex] &^= mask // a &^ = mask   a = a & (a ^ mask) = (a & a) ^ (a & mask) = a ^ ( 如果a=1 -> 1 如果 a=0 -> 0) = 1 ^1 or 0 ^ 0 = 结果都是 0
	}
}

// 这里存在一个认知误区： bitOffset的方向是：单个字节（从右到左）
// byteIndex 的方向：（从左到右）
// 这里的第几都是从0开始的第几
func (b *BitMap) GetBit(offset int64) byte {
	byteIndex := offset / 8 // 表示第几个字节
	bitOffset := offset % 8 // 表示 某个字节的第几个bit
	if byteIndex >= int64(len(*b)) {
		return 0
	}
	// (*b)[byteIndex] 表示字节,  bit位的低位在字节的右边（非左边）
	return ((*b)[byteIndex] >> bitOffset) & 0x01
}

type Callback func(offset int64, val byte) bool

func (b *BitMap) ForEachBit(begin int64, end int64, cb Callback) {
	offset := begin
	byteIndex := offset / 8
	bitOffset := offset % 8
	for byteIndex < int64(len(*b)) {
		b := (*b)[byteIndex]

		// 本字节的bit偏移量（从右向左）
		for bitOffset < 8 {

			bit := byte(b >> bitOffset & 0x01)

			// 该bit和 预期值相同
			if !cb(offset, bit) {
				return
			}
			// 继续本字节的下一个bit位
			bitOffset++
			// 这个是记录当前bit的实际偏移位置，用作结果返回
			offset++

			// 如果超过end
			if offset >= end && end != 0 {
				break // 这里应该直接return即可
			}
		}

		// 下一个字节
		byteIndex++
		// 下一个字节 的bit偏移位置
		bitOffset = 0
		// 
		if end > 0 && offset >= end {
			break
		}
	}
}

func (b *BitMap) ForEachByte(begin int, end int, cb Callback) {
	if end == 0 {
		end = len(*b)
	} else if end > len(*b) {
		end = len(*b)
	}
	for i := begin; i < end; i++ {
		if !cb(int64(i), (*b)[i]) {
			return
		}
	}
}
