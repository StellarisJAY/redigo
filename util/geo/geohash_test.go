package geo

import (
	"math"
	"testing"
)

func TestEncode(t *testing.T) {
	testCases := []struct {
		name     string
		lat      float64
		lng      float64
		expected string
	}{
		{"case-1", 31.1932993, 121.4396019, "wtw37q"},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if res := string(Encode(tc.lat, tc.lng, 15)); res != tc.expected {
				t.Logf("result: %s, expected: %s", res, tc.expected)
				t.Fail()
			}
		})
	}
}

func TestDistance(t *testing.T) {
	testCases := []struct {
		name     string
		lat1     float64
		lng1     float64
		lat2     float64
		lng2     float64
		expected float64
	}{
		{"equator-position-1", 0, 0, 0, 90, math.Round(math.Pi * EarthRadius / 2)},
		{"equator-position-2", 0, 45, 0, -45, math.Round(math.Pi * EarthRadius / 2)},
		{"non-equator-1", 45, 90, -45, 90, math.Round(math.Pi * EarthRadius / 2)},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if dis := Distance(tc.lat1, tc.lng1, tc.lat2, tc.lng2); math.Round(dis) != tc.expected {
				t.Logf("expected: %f, result: %f", tc.expected, dis)
				t.Fail()
			}
		})
	}
}
