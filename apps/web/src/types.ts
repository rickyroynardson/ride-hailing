// The public gateway's protobuf JSON shape. int64 amounts are strings, not floating-point numbers.
export interface Point { lat?: number; lng?: number }
export interface Money { amount: string; currency: string }
export interface User { id: string; name: string; email: string; phone?: string }
export interface Tokens { accessToken: string; refreshToken: string }
export interface AuthResult { user: User; tokens: Tokens }
export interface DriverProfile {
  licenseNumber: string
  vehiclePlate: string
  vehicleModel: string
  status: 'pending' | 'approved' | 'suspended'
}
export interface Profile { user: User; driverProfile?: DriverProfile | null }
export type RideStatus = 'RIDE_STATUS_REQUESTED' | 'RIDE_STATUS_ACCEPTED' | 'RIDE_STATUS_IN_PROGRESS' | 'RIDE_STATUS_COMPLETED' | 'RIDE_STATUS_CANCELLED'
export interface Ride {
  rideId: string
  status: RideStatus
  pickup: Point
  dropoff: Point
  driverId?: string
  driverLocation?: Point | null
  cancelReason?: string
  fare?: Money | null
}
export interface Offer { offerId: string; rideId: string; pickup: Point; expiresAt: string }
export interface Charge {
  chargeId: string
  rideId: string
  status: 'CHARGE_STATUS_PENDING' | 'CHARGE_STATUS_CAPTURED' | 'CHARGE_STATUS_FAILED'
  kind: string
  amount: Money
  declineReason?: string
  refundedAmount?: string
}
export function terminal(ride: Ride | null): boolean {
  return ride?.status === 'RIDE_STATUS_COMPLETED' || ride?.status === 'RIDE_STATUS_CANCELLED'
}
export function money(value?: Money | null): string {
  if (!value) return '—'
  try {
    return new Intl.NumberFormat('id-ID', { style: 'currency', currency: value.currency, maximumFractionDigits: 0 }).format(BigInt(value.amount || '0'))
  } catch { return `${value.currency} ${value.amount}` }
}
export function coords(p: Point): string { return `${(p.lat ?? 0).toFixed(5)}, ${(p.lng ?? 0).toFixed(5)}` }
export const CENTER: Point = { lat: -6.2, lng: 106.8 }
