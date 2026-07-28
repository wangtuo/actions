package math

import "testing"

func TestAdd(t *testing.T) {
	if Add(1, 2) != 3 {
		t.Fatal("Add(1,2) != 3")
	}
}

func TestSub(t *testing.T) {
	if Sub(5, 2) != 3 {
		t.Fatal("Sub(5,2) != 3")
	}
}

func TestMul(t *testing.T) {
	if Mul(3, 4) != 12 {
		t.Fatal("Mul(3,4) != 12")
	}
}

func FuzzAdd(f *testing.F) {
	f.Add(1, 2)
	f.Fuzz(func(t *testing.T, a, b int) {
		if Add(a, b) != Add(b, a) {
			t.Fatalf("Add 不满足交换律: a=%d b=%d", a, b)
		}
	})
}

func BenchmarkAdd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Add(i, i+1)
	}
}
