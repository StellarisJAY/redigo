package pattern

import (
	"testing"
)

func TestPattern_Matches1(t *testing.T) {
	p1 := "h?llo"
	p2 := "h[^e]llo"
	p3 := "h[abc.]llo"
	p4 := "h[a-e]llo"
	p5 := "h[ae]ll[^o]"

	pattern1 := ParsePattern(p1)
	pattern2 := ParsePattern(p2)
	pattern3 := ParsePattern(p3)
	pattern4 := ParsePattern(p4)
	pattern5 := ParsePattern(p5)

	if pattern1.Matches("hello") && pattern1.Matches("hallo") && !pattern1.Matches("hllo") {
		t.Log("Pattern1 Passed")
	} else {
		t.Fail()
	}

	if !pattern2.Matches("hello") && pattern2.Matches("hallo") && !pattern2.Matches("hllo") {
		t.Log("Pattern2 Passed")
	} else {
		t.Fail()
	}

	if !pattern3.Matches("hello") && pattern3.Matches("hallo") && pattern3.Matches("h.llo") && !pattern3.Matches("hllo") {
		t.Log("Pattern3 Passed")
	} else {
		t.Fail()
	}

	if pattern4.Matches("hello") && pattern4.Matches("hallo") && !pattern4.Matches("h.llo") && !pattern4.Matches("hllo") {
		t.Log("Pattern4 Passed")
	} else {
		t.Fail()
	}

	if !pattern5.Matches("hallo") && !pattern5.Matches("hello") && pattern5.Matches("helli") {
		t.Log("Pattern5 Passed")
	} else {
		t.Fail()
	}
}

func TestPattern_Matches2(t *testing.T) {
	p1 := "*"
	p2 := "he*o"
	p3 := "h*ll*"

	pattern1 := ParsePattern(p1)
	pattern2 := ParsePattern(p2)
	pattern3 := ParsePattern(p3)

	if pattern1.Matches("hello") && pattern1.Matches("k") && pattern1.Matches("") {
		t.Log("Pattern * Passed")
	} else {
		t.Fail()
	}

	if pattern2.Matches("hello") && pattern2.Matches("heo") && pattern2.Matches("helo") {
		t.Log("Pattern he*o Passed")
	} else {
		t.Fail()
	}

	if pattern3.Matches("hello") && pattern3.Matches("heeellooo") && pattern3.Matches("hll") {
		t.Log("Pattern h*ll* Passed")
	} else {
		t.Fail()
	}
}
