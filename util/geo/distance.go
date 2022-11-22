package geo

import (
	"math"
)

const EarthRadius float64 = 6378.137

func Distance(lat1, lng1 float64, lat2, lng2 float64) float64 {
	// 角度转换成弧度
	lat1, lat2, lng1, lng2 = math.Pi*lat1/180, math.Pi*lat2/180, math.Pi*lng1/180, math.Pi*lng2/180
	// 计算单位球上直角坐标
	x1 := math.Cos(lat1) * math.Cos(lng1)
	y1 := math.Cos(lat1) * math.Sin(lng1)
	z1 := math.Sin(lat1)

	x2 := math.Cos(lat2) * math.Cos(lng2)
	y2 := math.Cos(lat2) * math.Sin(lng2)
	z2 := math.Sin(lat2)

	// 坐标点直线距离
	lineDis := math.Sqrt((x1-x2)*(x1-x2) + (y1-y2)*(y1-y2) + (z1-z2)*(z1-z2))
	// 球心夹角
	angle := math.Asin(0.5*lineDis) * 2
	// 弧长 = 弧度 * R
	distance := angle * EarthRadius
	return distance
}
