import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import App from './App'

vi.mock('./Map', () => ({ default: () => <div aria-label="Trip map" /> }))

const jwt = () => `header.${btoa(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600 }))}.sig`
const tokens = () => ({ accessToken: jwt(), refreshToken: 'refresh' })
const json = (body: unknown) => new Response(JSON.stringify(body))
const price = { amount: '38575', currency: 'IDR' }
const profile = { user: { id: 'u1', name: 'Ricky', email: 'r@example.com' }, driverProfile: null as unknown }
let streams: Record<string, ReadableStreamDefaultController<Uint8Array>>
function send(path: string, value: unknown) { streams[path].enqueue(new TextEncoder().encode(JSON.stringify({ result: value }) + '\n')) }
function streaming(path: string, initial: unknown) {
  return new Response(new ReadableStream<Uint8Array>({ start(c) { streams[path] = c; c.enqueue(new TextEncoder().encode(JSON.stringify({ result: initial }) + '\n')) } }))
}
const ride = (status: string) => ({ rideId: 'r1', status, pickup: { lat: -6.2, lng: 106.8 }, dropoff: { lat: -6.23, lng: 106.835 }, fare: price, driverId: 'd1' })

beforeEach(() => { sessionStorage.clear(); streams = {}; profile.driverProfile = null })
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

async function signIn() {
  fireEvent.change(screen.getByLabelText('Email address'), { target: { value: 'r@example.com' } })
  fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'password123' } })
  fireEvent.click(screen.getByRole('button', { name: 'Let’s get going ↗' }))
  await screen.findByRole('heading', { name: /Hi, Ricky/ })
}

it('takes a rider through estimate, booking, the live trip and captured payment without any internal calls', async () => {
  const fetcher = vi.fn(async (path: string, init?: RequestInit) => {
    switch (path) {
      case '/v1/auth/login': return json({ ...profile, tokens: tokens() })
      case '/v1/me': return json(profile)
      case '/v1/fares:estimate': return json({ fare: price })
      case '/v1/rides': return json({ rideId: 'r1', fare: price })
      case '/v1/rides/r1:watch': return streaming(path, { ride: ride('RIDE_STATUS_REQUESTED') })
      case '/v1/rides/r1/charge': return json({ charge: { status: 'CHARGE_STATUS_CAPTURED', amount: price, refundedAmount: '0' } })
      default: throw new Error(`Unexpected request: ${init?.method} ${path}`)
    }
  })
  vi.stubGlobal('fetch', fetcher)
  render(<App />)
  await signIn()
  fireEvent.click(screen.getByRole('button', { name: 'Estimate' }))
  await screen.findByText('Rp 38.575')
  fireEvent.click(screen.getByRole('button', { name: 'Request ride ↗' }))
  await screen.findByRole('heading', { name: 'Finding your match.' })
  expect(sessionStorage.getItem('jalan:ride:u1')).toBe('r1')
  await act(async () => { send('/v1/rides/r1:watch', { ride: ride('RIDE_STATUS_ACCEPTED') }) })
  await screen.findByRole('heading', { name: 'You’re on your way.' })
  await act(async () => { send('/v1/rides/r1:watch', { ride: ride('RIDE_STATUS_COMPLETED') }) })
  await screen.findByText('Paid · Rp 38.575')
  expect(fetcher.mock.calls.every(([path]) => path.startsWith('/v1/'))).toBe(true)
  expect(sessionStorage.getItem('accessToken')).toBeNull()
})

it('uses the driver offer stream, accepts, starts and completes a ride', async () => {
  profile.driverProfile = { status: 'approved', vehicleModel: 'Avanza', vehiclePlate: 'B1', licenseNumber: 'L1' }
  const fetcher = vi.fn(async (path: string) => {
    switch (path) {
      case '/v1/auth/login': return json({ ...profile, tokens: tokens() })
      case '/v1/auth/refresh': return json({ ...profile, tokens: tokens() })
      case '/v1/me': return json(profile)
      case '/v1/driver/earnings': return json({ balances: [] })
      case '/v1/driver/location': return json({ outcome: 'UPDATE_OUTCOME_STORED' })
      case '/v1/driver/offer:watch': return streaming(path, { offer: { offerId: 'o1', rideId: 'r1', pickup: { lat: -6.2, lng: 106.8 }, expiresAt: '2099-01-01T00:00:00Z' } })
      case '/v1/offers/o1:accept': return json({ rideId: 'r1' })
      case '/v1/rides/r1:watch': return streaming(path, { ride: { ...ride('RIDE_STATUS_ACCEPTED'), driverId: 'u1' } })
      case '/v1/rides/r1:start': send('/v1/rides/r1:watch', { ride: { ...ride('RIDE_STATUS_IN_PROGRESS'), driverId: 'u1' } }); return json({})
      case '/v1/rides/r1:complete': send('/v1/rides/r1:watch', { ride: { ...ride('RIDE_STATUS_COMPLETED'), driverId: 'u1' } }); return json({})
      case '/v1/rides/r1/charge': return json({ charge: { status: 'CHARGE_STATUS_CAPTURED', amount: price } })
      default: throw new Error(`Unexpected request ${path}`)
    }
  })
  vi.stubGlobal('fetch', fetcher)
  render(<App />)
  await signIn()
  fireEvent.click(screen.getByRole('button', { name: 'I’m driving' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Go online ↗' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Accept ride ↗' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Rider picked up · Start trip' }))
  const complete = await screen.findByRole('button', { name: 'Rider dropped off · Complete trip' })
  await waitFor(() => expect(complete.hasAttribute('disabled')).toBe(false))
  fireEvent.click(complete)
  await screen.findByRole('heading', { name: 'You have arrived.' })
  expect(fetcher.mock.calls.some(([path]) => path === '/v1/driver/offer')).toBe(false) // no offer polling
})

it('keeps driver approval outside the app', async () => {
  profile.driverProfile = { status: 'pending' }
  vi.stubGlobal('fetch', vi.fn(async (path: string) => path === '/v1/auth/login' ? json({ ...profile, tokens: tokens() }) : json(profile)))
  render(<App />)
  await signIn()
  fireEvent.click(screen.getByRole('button', { name: 'I’m driving' }))
  expect(await screen.findByText(/administrator must approve/)).toBeTruthy()
  expect(screen.queryByRole('button', { name: 'Go online ↗' })).toBeNull()
})

it('keeps an unpaid ride available until the rider changes the card and retries payment', async () => {
  sessionStorage.setItem('jalan:ride:u1', 'r1')
  let paid = false
  const fetcher = vi.fn(async (path: string) => {
    switch (path) {
      case '/v1/auth/login': return json({ ...profile, tokens: tokens() })
      case '/v1/me': return json(profile)
      case '/v1/rides/r1:watch': return streaming(path, { ride: ride('RIDE_STATUS_COMPLETED') })
      case '/v1/rides/r1/charge': return json({ charge: { status: paid ? 'CHARGE_STATUS_CAPTURED' : 'CHARGE_STATUS_FAILED', declineReason: paid ? '' : 'card_declined', amount: price } })
      case '/v1/me/payment-method': return json({})
      case '/v1/rides/r1/charge:retry': paid = true; return json({})
      default: throw new Error(`Unexpected request ${path}`)
    }
  })
  vi.stubGlobal('fetch', fetcher)
  render(<App />)
  await signIn()
  await screen.findByText('Not paid · card_declined')
  expect(screen.getByRole('button', { name: 'Done · Back to planning' }).hasAttribute('disabled')).toBe(true)
  fireEvent.click(screen.getByText('Payment method'))
  fireEvent.click(screen.getByRole('button', { name: 'Save payment method' }))
  await screen.findByText('Payment method updated.')
  fireEvent.click(screen.getByRole('button', { name: 'Retry payment' }))
  await screen.findByText('Paid · Rp 38.575')
  const done = screen.getByRole('button', { name: 'Done · Back to planning' })
  await waitFor(() => expect(done.hasAttribute('disabled')).toBe(false))
  fireEvent.click(done)
  expect(sessionStorage.getItem('jalan:ride:u1')).toBeNull()
})
