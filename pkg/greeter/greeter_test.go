package greeter

import "testing"

func TestHello(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "Hello, world!"},
		{"Ark", "Hello, Ark!"},
		{"世界", "Hello, 世界!"},
	}
	for _, c := range cases {
		if got := Hello(c.in); got != c.want {
			t.Errorf("Hello(%q)=%q, want %q", c.in, got, c.want)
		}
	}
}
