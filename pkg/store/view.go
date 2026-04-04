package store

type ByteView []byte

func (v ByteView) Len() int {
	return len(v)
}

func (v ByteView) String() string {
	return string(v)
}

func (v ByteView) Slice() ByteView {
	return cloneByteView(v)
}

func cloneByteView(v ByteView) ByteView {
	clone := make(ByteView, len(v))
	copy(clone, v)
	return clone
}
