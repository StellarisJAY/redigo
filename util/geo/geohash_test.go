package geo

import (
	"fmt"
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
		{"case-2", -31.191911, -122.345122, "362yxc"},
		{"case-3", -0.1132445, 10.01234456, "kpzp7g"},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if res := ToString(Encode(tc.lat, tc.lng, 15)); res != tc.expected {
				t.Logf("result: %s, expected: %s", res, tc.expected)
				t.Fail()
			}
		})
	}
}

func TestDecode(t *testing.T) {
	testCases := []struct {
		name      string
		latitude  float64
		longitude float64
	}{
		{"case-1", 31.1932993, 121.4396019},
		{"case-2", -31.191911, -122.345122},
		{"case-3", -0.1132445, 10.01234456},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			encode := Encode(tc.latitude, tc.longitude, 20)
			lat, lng, err := Decode(encode)
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
			if fmt.Sprintf("%.2f", lat) != fmt.Sprintf("%.2f", tc.latitude) {
				t.Logf("wrong latitude, result: %f, expected: %f", lat, tc.latitude)
			}
			if fmt.Sprintf("%.2f", lng) != fmt.Sprintf("%.2f", tc.longitude) {
				t.Logf("wrong latitude, result: %f, expected: %f", lng, tc.longitude)
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

func TestFormatUint64(t *testing.T) {
	testCases := []struct {
		name      string
		expected  uint64
		latitude  float64
		longitude float64
	}{
		{"case-1", 2024680306809116940, 31.1932993, 121.4396019},
		{"case-2", 217863960333589777, -31.191911, -122.345122},
		{"case-3", 1302981842366826010, -0.1132445, 10.01234456},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if num := FormatUint64(Encode(tc.latitude, tc.longitude, MaxPrecision)); num != tc.expected {
				t.Logf("wrong uint64, expected: %d, got: %d", tc.expected, num)
				t.Fail()
			}
		})
	}
}

func TestFromUint64(t *testing.T) {
	testCases := []struct {
		name      string
		number    uint64
		latitude  float64
		longitude float64
	}{
		{"case-1", 2024680306809116940, 31.1932993, 121.4396019},
		{"case-2", 217863960333589777, -31.191911, -122.345122},
		{"case-3", 1302981842366826010, -0.1132445, 10.01234456},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if lat, lng, err := Decode(FromUint64(tc.number)); err != nil {
				t.Error(err)
				t.FailNow()
			} else if fmt.Sprintf("%.3f", lat) != fmt.Sprintf("%.3f", tc.latitude) ||
				fmt.Sprintf("%.3f", lng) != fmt.Sprintf("%.3f", tc.longitude) {
				t.Logf("wrong coordinates, expect: {lat=%f, lng=%f}, got: {lat=%f, lng=%f}", tc.latitude, tc.longitude, lat, lng)
				t.FailNow()
			}
		})
	}
}
