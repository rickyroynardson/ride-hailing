import { useEffect, useRef, useState } from 'react'
import { Api, ApiError, pause } from './api'
import { terminal, type Charge, type Offer, type Point, type Ride } from './types'

export type Connection = 'connecting' | 'live' | 'reconnecting' | 'closed'
const message = (e: unknown) => e instanceof Error ? e.message : String(e)

export function useRide(api: Api, rideId: string) {
  const [ride, setRide] = useState<Ride | null>(null)
  const [connection, setConnection] = useState<Connection>('closed')
  const [error, setError] = useState('')
  useEffect(() => {
    setRide(null)
    setError('')
    if (!rideId) { setConnection('closed'); return }
    const controller = new AbortController()
    const { signal } = controller
    let finished = false
    void (async () => {
      let attempt = 0
      while (!signal.aborted && !finished) {
        setConnection(attempt ? 'reconnecting' : 'connecting')
        try {
          await api.watch<{ ride: Ride }>(`/v1/rides/${encodeURIComponent(rideId)}:watch`, signal, ({ ride }) => {
            setRide(ride)
            setError('')
            setConnection('live')
            finished = terminal(ride)
            return !finished
          })
        } catch (e) {
          if (signal.aborted) return
          setError(message(e))
          if (e instanceof ApiError && [401, 403, 404].includes(e.status)) break
        }
        if (!finished) {
          setConnection('reconnecting')
          await pause(Math.min(1000 * 2 ** attempt++, 10_000), signal).catch(() => {})
        }
      }
      if (!signal.aborted) setConnection('closed')
    })()
    return () => controller.abort()
  }, [api, rideId])
  return { ride: ride?.rideId === rideId ? ride : null, connection, error }
}

export function useDriver(api: Api, online: boolean, point: Point) {
  const position = useRef(point)
  position.current = point
  const [offer, setOffer] = useState<Offer | null>(null)
  const [connection, setConnection] = useState<Connection>('closed')
  const [error, setError] = useState('')
  const [lastSent, setLastSent] = useState<number | null>(null)
  useEffect(() => {
    setOffer(null)
    setLastSent(null)
    setError('')
    if (!online) { setConnection('closed'); return }
    const controller = new AbortController()
    const { signal } = controller
    // One request at a time; a slow response mustn't trigger a burst of overlapping heartbeats.
    void (async () => {
      while (!signal.aborted) {
        try {
          const res = await api.request<{ outcome: string }>('/v1/driver/location', 'PUT', {
            location: position.current, accuracyMeters: 5, recordedAt: new Date().toISOString(),
          }, signal)
          if (signal.aborted) return
          if (res.outcome === 'UPDATE_OUTCOME_STORED') { setLastSent(Date.now()); setError('') }
          else setError(`Position not stored: ${res.outcome.replace('UPDATE_OUTCOME_', '').toLowerCase()}`)
        } catch (e) {
          if (signal.aborted) return
          setError(`Location: ${message(e)}`)
        }
        await pause(4000, signal).catch(() => {})
      }
    })()
    void (async () => {
      let attempt = 0
      while (!signal.aborted) {
        setConnection(attempt ? 'reconnecting' : 'connecting')
        try {
          await api.watch<{ offer?: Offer | null }>('/v1/driver/offer:watch', signal, ({ offer }) => {
            setOffer(offer ?? null)
            setConnection('live')
            attempt = 0
          })
        } catch (e) {
          if (signal.aborted) return
          setError(`Offers: ${message(e)}`)
          if (e instanceof ApiError && [401, 403].includes(e.status)) { setConnection('closed'); setOffer(null); return }
        }
        if (signal.aborted) return
        setOffer(null) // an old offer isn't actionable during a disconnect
        setConnection('reconnecting')
        await pause(Math.min(1000 * 2 ** attempt++, 10_000), signal).catch(() => {})
      }
    })()
    return () => controller.abort()
  }, [api, online])
  return { offer, connection, error, lastSent }
}

export function useCharge(api: Api, ride: Ride | null) {
  const [charge, setCharge] = useState<Charge | null>(null)
  const [error, setError] = useState('')
  const [revision, setRevision] = useState(0)
  const rideId = terminal(ride) ? ride?.rideId : undefined
  // A cancellation before assignment / when there was no driver has no charge to wait for.
  const free = ride?.status === 'RIDE_STATUS_CANCELLED' && !ride.driverId
  useEffect(() => {
    setCharge(null)
    setError('')
    if (!rideId || free) return
    const controller = new AbortController()
    const { signal } = controller
    void (async () => {
      for (let i = 0; i < 30 && !signal.aborted; i++) {
        try {
          const res = await api.request<{ charge: Charge }>(`/v1/rides/${encodeURIComponent(rideId)}/charge`, 'GET', undefined, signal)
          if (signal.aborted) return
          setCharge(res.charge)
          setError('')
          if (res.charge.status !== 'CHARGE_STATUS_PENDING') return
        } catch (e) {
          if (signal.aborted) return
          if (!(e instanceof ApiError && e.status === 404)) { setError(message(e)); return }
        }
        await pause(2000, signal).catch(() => {})
      }
      if (!signal.aborted) setError('Payment is still being processed. You can check again shortly.')
    })()
    return () => controller.abort()
  }, [api, rideId, free, revision])
  return { charge, error, free, reload: () => setRevision(n => n + 1) }
}
