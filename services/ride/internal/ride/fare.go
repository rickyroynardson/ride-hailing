package ride

import "math"

const (
	// Currency is the one currency rides are priced in. Amounts are whole rupiah.
	Currency = "IDR"

	baseFare = 8_000
	perKm    = 3_000
	minFare  = 10_000
	// CancellationFee is what a rider pays for cancelling after a driver accepted.
	CancellationFee = 5_000

	// roadFactor turns the straight-line distance into a typical road distance.
	roadFactor  = 1.3
	earthRadius = 6371.0 // km
)

// Money is an amount in a currency's smallest unit used here.
type Money struct {
	Amount   int64
	Currency string
}

// priceFare is the upfront fare between two points, rounded up to a whole rupiah.
// ponytail: estimate and booking price independently, so a tariff change between them shows a different fare; sign
// the estimate into a quote token and honour it at CreateRide if tariffs ever change at runtime
func priceFare(pickup, dropoff Location) Money {
	km := distanceKm(pickup, dropoff) * roadFactor
	amount := int64(math.Ceil(baseFare + km*perKm))
	return Money{Amount: max(amount, minFare), Currency: Currency}
}

// distanceKm is the great-circle distance (haversine).
func distanceKm(a, b Location) float64 {
	toRad := func(deg float64) float64 { return deg * math.Pi / 180 }
	dLat, dLng := toRad(b.Lat-a.Lat), toRad(b.Lng-a.Lng)
	h := math.Pow(math.Sin(dLat/2), 2) + math.Cos(toRad(a.Lat))*math.Cos(toRad(b.Lat))*math.Pow(math.Sin(dLng/2), 2)
	return 2 * earthRadius * math.Asin(math.Sqrt(h))
}
