import { afterEach, describe, expect, it, vi } from 'vitest'
import { Api, ApiError, readStream } from './api'

const jwt = (seconds: number) => `header.${btoa(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + seconds }))}.sig`
const json = (value: unknown, status = 200, headers?: HeadersInit) => new Response(JSON.stringify(value), { status, headers })
afterEach(() => vi.unstubAllGlobals())

describe('NDJSON streams', () => {
  it('handles UTF-8 and frames split over arbitrary chunks, including a last line without a newline', async () => {
    const bytes = new TextEncoder().encode('{"result":{"name":"Jalan ↗"}}\n\n{"result":{"offer":null}}')
    const stream = new ReadableStream<Uint8Array>({ start(c) { for (const b of bytes) c.enqueue(Uint8Array.of(b)); c.close() } })
    const messages: unknown[] = []
    await readStream(stream, new AbortController().signal, message => { messages.push(message) })
    expect(messages).toEqual([{ name: 'Jalan ↗' }, { offer: null }])
  })

  it('delivers the first frame without waiting for EOF and cancels the stream on early completion', async () => {
    const cancel = vi.fn()
    const stream = new ReadableStream<Uint8Array>({ start(c) { c.enqueue(new TextEncoder().encode('{"result":{"status":"completed"}}\n')) }, cancel })
    const got = vi.fn(() => false)
    await readStream(stream, new AbortController().signal, got)
    expect(got).toHaveBeenCalledWith({ status: 'completed' })
    expect(cancel).toHaveBeenCalledOnce()
    expect(stream.locked).toBe(false)
  })

  it('stops a quiet pending read on cancellation', async () => {
    const cancel = vi.fn()
    const stream = new ReadableStream<Uint8Array>({ cancel })
    const controller = new AbortController()
    const task = readStream(stream, controller.signal, () => {})
    controller.abort()
    await expect(task).rejects.toMatchObject({ name: 'AbortError' })
    expect(cancel).toHaveBeenCalled()
    expect(stream.locked).toBe(false)
  })

  it('recognizes an in-band error even with HTTP 200', async () => {
    const stream = new Response('{"error":{"code":16,"message":"token expired"}}\n').body!
    await expect(readStream(stream, new AbortController().signal, () => {})).rejects.toMatchObject({ code: 16, message: 'token expired' })
  })
})

describe('gateway client', () => {
  it('serializes refreshes so simultaneous requests rotate a single-use refresh token only once', async () => {
    const fresh = jwt(3600)
    let finish!: () => void
    const barrier = new Promise<void>(resolve => { finish = resolve })
    const fetcher = vi.fn(async (path: string) => {
      if (path === '/v1/auth/refresh') {
        await barrier
        return json({ tokens: { accessToken: fresh, refreshToken: 'rotated' } })
      }
      return json({ ok: true })
    })
    vi.stubGlobal('fetch', fetcher)
    const api = new Api()
    api.setSession({ accessToken: jwt(-10), refreshToken: 'single-use' })
    const a = api.request('/v1/me')
    const b = api.request('/v1/driver/earnings')
    finish()
    await Promise.all([a, b])
    expect(fetcher.mock.calls.filter(([p]) => p === '/v1/auth/refresh')).toHaveLength(1)
    const sent = fetcher.mock.calls as unknown as [string, RequestInit][]
    expect(sent.find(([p]) => p === '/v1/me')?.[1].headers).toMatchObject({ Authorization: `Bearer ${fresh}` })
  })

  it('does not resurrect a session if a refresh finishes after sign-out', async () => {
    let finish!: (r: Response) => void
    const fetcher = vi.fn(() => new Promise<Response>(resolve => { finish = resolve }))
    vi.stubGlobal('fetch', fetcher)
    const api = new Api()
    api.setSession({ accessToken: jwt(-1), refreshToken: 'old' })
    const pending = api.request('/v1/me')
    api.setSession(null)
    finish(json({ tokens: { accessToken: jwt(3600), refreshToken: 'new' } }))
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' })
    await expect(api.request('/v1/me')).rejects.toMatchObject({ status: 401 })
    expect(fetcher).toHaveBeenCalledOnce()
  })

  it('forwards 429 and Retry-After rather than hiding or retrying it', async () => {
    const fetcher = vi.fn(async () => json({ code: 8, message: 'Slow down.' }, 429, { 'Retry-After': '12' }))
    vi.stubGlobal('fetch', fetcher)
    const api = new Api()
    await expect(api.request('/v1/auth/login', 'POST', {}, undefined, false)).rejects.toMatchObject({ status: 429, code: 8, retryAfter: '12', message: 'Slow down. Try again in 12s.' })
    expect(fetcher).toHaveBeenCalledOnce()
  })

  it('never repeats a booking after an unknown network outcome', async () => {
    const fetcher = vi.fn(async () => { throw new TypeError('Network lost') })
    vi.stubGlobal('fetch', fetcher)
    const api = new Api()
    api.setSession({ accessToken: jwt(3600), refreshToken: 'r' })
    await expect(api.request('/v1/rides', 'POST', {})).rejects.toThrow('Network lost')
    expect(fetcher).toHaveBeenCalledOnce()
  })

  it('refreshes once after a 401; a 403 is never retried', async () => {
    const fetcher = vi.fn(async () => json({}, 401))
      .mockResolvedValueOnce(json({ code: 16, message: 'Expired' }, 401))
      .mockResolvedValueOnce(json({ tokens: { accessToken: jwt(7200), refreshToken: 'rotated' } }))
      .mockResolvedValueOnce(json({ code: 7, message: 'Driver only' }, 403))
    vi.stubGlobal('fetch', fetcher)
    const api = new Api()
    api.setSession({ accessToken: jwt(3600), refreshToken: 'r' })
    await expect(api.request('/v1/driver/offer')).rejects.toBeInstanceOf(ApiError)
    expect(fetcher).toHaveBeenCalledTimes(3)
  })
})
