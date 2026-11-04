import { afterEach, expect, it, vi } from 'vitest'
import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { Api } from './api'
import { useDriver, useRide } from './hooks'

afterEach(cleanup)

it('hides stale offers immediately on EOF and stops heartbeat and streaming work on unmount', async () => {
  const api = new Api()
  const calls: AbortSignal[] = []
  vi.spyOn(api, 'request').mockImplementation(async (_path, _method, _body, signal) => {
    calls.push(signal!)
    return { outcome: 'UPDATE_OUTCOME_STORED' }
  })
  let end!: () => void
  vi.spyOn(api, 'watch').mockImplementation(async (_path, _signal, update) => {
    update({ offer: { offerId: 'o1', rideId: 'r1', expiresAt: '2099-01-01T00:00:00Z', pickup: {} } })
    await new Promise<void>(resolve => { end = resolve })
  })
  const { result, unmount } = renderHook(() => useDriver(api, true, { lat: -6.2, lng: 106.8 }))
  await waitFor(() => expect(result.current.offer?.offerId).toBe('o1'))
  await act(async () => { end() })
  expect(result.current.offer).toBeNull()
  expect(result.current.connection).toBe('reconnecting')
  unmount()
  expect(calls.every(s => s.aborted)).toBe(true)
})

it('never displays a previous ride as the snapshot of a newly booked one', async () => {
  const api = new Api()
  const watchers: Record<string, (value: unknown) => void> = {}
  vi.spyOn(api, 'watch').mockImplementation(async (path, signal, update) => {
    watchers[path] = update
    await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }))
  })
  const { result, rerender } = renderHook(({ id }) => useRide(api, id), { initialProps: { id: 'old' } })
  await act(async () => { watchers['/v1/rides/old:watch']({ ride: { rideId: 'old', status: 'RIDE_STATUS_ACCEPTED' } }) })
  expect(result.current.ride?.rideId).toBe('old')
  rerender({ id: 'new' })
  expect(result.current.ride).toBeNull()
})
