// store/byteview_test.go
package store

import (
	"fmt"
	"testing"
)

func TestByteViewBasics(t *testing.T) {
	// 创建一个 ByteView
	original := ByteView("hello world")

	// 测试 Len
	if original.Len() != 11 {
		t.Errorf("expected Len=11, got %d", original.Len())
	}

	// 测试 String
	if original.String() != "hello world" {
		t.Errorf("expected String='hello world', got '%s'", original.String())
	}
}

func TestByteViewClone(t *testing.T) {
	original := ByteView("clone me")

	// 获取副本
	clone := original.Slice()

	// 修改副本
	clone[0] = 'C'

	// original 不受影响
	if original.String() != "clone me" {
		t.Error("original ByteView should not be affected by modifying clone -- ", original)
	}
}

func TestByteViewEmpty(t *testing.T) {
	empty := ByteView("")
	if empty.Len() != 0 {
		t.Errorf("expected Len=0, got %d", empty.Len())
	}
	if empty.String() != "" {
		t.Errorf("expected empty string, got '%s'", empty.String())
	}
}

func ExampleByteView() {
	v := ByteView("test data")
	fmt.Println(v.Len())    // 输出: 9
	fmt.Println(v.String()) // 输出: test data

	clone := v.Slice()
	clone[0] = 'T'
	fmt.Println(v.String())    // 输出: test data (v 不受影响)
	fmt.Println(string(clone)) // 输出: Test data

	// Output:
	// 9
	// test data
	// test data
	// Test data
}
