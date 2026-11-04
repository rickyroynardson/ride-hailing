import type { Point } from './types'

export interface BookingIntent { requestId: string; pickup: Point; dropoff: Point }

// Tab-local intent receipts contain no credentials. Keep the exact key AND payload across lost replies/reloads;
// allocating a new key on each attempt defeats idempotency. Memory is a fallback when browser storage is disabled.
const memory = new Map<string, string>()
function read(key: string): string | null {
  try { return sessionStorage.getItem(key) ?? memory.get(key) ?? null } catch { return memory.get(key) ?? null }
}
function write(key: string, value: string | null) {
  if (value === null) memory.delete(key); else memory.set(key, value)
  try { if (value === null) sessionStorage.removeItem(key); else sessionStorage.setItem(key, value) } catch { /* memory fallback */ }
}
const bookingKey = (user: string) => `jalan:booking:${user}`
export function pendingBooking(user: string): BookingIntent | null {
  try { return JSON.parse(read(bookingKey(user)) || 'null') } catch { return null }
}
export function bookingIntent(user: string, pickup: Point, dropoff: Point): BookingIntent {
  const pending = pendingBooking(user)
  if (pending) return pending
  const intent = { requestId: crypto.randomUUID(), pickup: { ...pickup }, dropoff: { ...dropoff } }
  write(bookingKey(user), JSON.stringify(intent))
  return intent
}
export function acknowledgeBooking(user: string) { write(bookingKey(user), null) }
export function paymentRetryIntent(user: string, ride: string): string {
  const key = `jalan:retry:${user}:${ride}`
  const requestId = read(key) || crypto.randomUUID()
  write(key, requestId)
  return requestId
}
export function acknowledgePaymentRetry(user: string, ride: string) { write(`jalan:retry:${user}:${ride}`, null) }
