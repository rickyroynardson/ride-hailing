import type { AuthResult, Tokens } from './types'

export class ApiError extends Error {
  constructor(public status: number, public code: number, message: string, public retryAfter?: string | null) { super(message) }
}

async function errorResponse(response: Response): Promise<ApiError> {
  const body = await response.json().catch(() => ({}))
  const retry = response.headers.get('Retry-After')
  const suffix = response.status === 429 && retry ? ` Try again in ${retry}s.` : ''
  return new ApiError(response.status, body.code ?? 2, (body.message || `Gateway returned HTTP ${response.status}.`) + suffix, retry)
}

function expiry(token: string): number {
  // This only schedules refreshes. The services, not the browser, verify tokens.
  try { return JSON.parse(atob(token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/'))).exp * 1000 }
  catch { return 0 }
}

export class Api {
  private tokens: Tokens | null = null
  private generation = 0
  private refreshing: Promise<void> | null = null
  onExpired?: () => void

  // Credentials live only in memory. Keep one instance per mounted app/tab: refresh tokens are single-use.
  setSession(tokens: Tokens | null) {
    this.generation++
    this.tokens = tokens
    this.refreshing = null
  }

  async refresh(force = false): Promise<void> {
    if (!this.tokens) throw new ApiError(401, 16, 'Please sign in again.')
    if (!force && expiry(this.tokens.accessToken) > Date.now() + 60_000) return
    if (this.refreshing) return this.refreshing
    const generation = this.generation
    const refreshToken = this.tokens.refreshToken
    const task = (async () => {
      const response = await fetch('/v1/auth/refresh', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refreshToken }),
        signal: AbortSignal.timeout(15_000),
      })
      if (generation !== this.generation) throw new DOMException('Session changed', 'AbortError')
      if (!response.ok) {
        const error = await errorResponse(response)
        if (error.status === 401 || error.status === 403) {
          this.setSession(null)
          this.onExpired?.()
        }
        throw error
      }
      const result: AuthResult = await response.json()
      if (generation !== this.generation) throw new DOMException('Session changed', 'AbortError')
      this.tokens = result.tokens
    })()
    this.refreshing = task
    try { await task }
    finally { if (this.refreshing === task) this.refreshing = null }
  }

  private async send(path: string, init: RequestInit = {}, authenticated = true): Promise<Response> {
    const generation = this.generation
    if (authenticated) await this.refresh()
    if (authenticated && generation !== this.generation) throw new DOMException('Session changed', 'AbortError')
    init.signal?.throwIfAborted()
    const token = this.tokens?.accessToken
    const request = () => fetch(path, {
      ...init,
      headers: { 'Content-Type': 'application/json', ...(authenticated ? { Authorization: `Bearer ${this.tokens?.accessToken}` } : {}), ...init.headers },
    })
    let response = await request()
    if (authenticated && generation !== this.generation) {
      await response.body?.cancel()
      throw new DOMException('Session changed', 'AbortError')
    }
    if (authenticated && response.status === 401) {
      await response.body?.cancel()
      // Another concurrent call may already have rotated the token.
      if (!this.tokens) throw new ApiError(401, 16, 'Please sign in again.')
      if (this.tokens.accessToken === token) await this.refresh(true)
      init.signal?.throwIfAborted()
      response = await request()
    }
    if (!response.ok) throw await errorResponse(response)
    return response
  }

  async request<T>(path: string, method = 'GET', body?: unknown, signal?: AbortSignal, authenticated = true): Promise<T> {
    const bounded = AbortSignal.timeout(15_000)
    const response = await this.send(path, { method, body: body === undefined ? undefined : JSON.stringify(body), signal: signal ? AbortSignal.any([signal, bounded]) : bounded }, authenticated)
    return response.json()
  }

  async watch<T>(path: string, signal: AbortSignal, onMessage: (message: T) => boolean | void): Promise<void> {
    const response = await this.send(path, { signal })
    if (!response.body) throw new Error('This browser cannot read streaming responses.')
    await readStream(response.body, signal, onMessage)
  }

  async logout(): Promise<void> {
    const refreshToken = this.tokens?.refreshToken
    this.setSession(null) // a late refresh or HTTP response must never sign the user back in
    if (refreshToken) await this.request('/v1/auth/logout', 'POST', { refreshToken }, undefined, false)
  }
}

// Fetch streams, not EventSource: the gateway speaks NDJSON, and bearer headers must be sent. Frames and even UTF-8
// characters may be split across network chunks. Always release/cancel the reader on errors or early completion.
export async function readStream<T>(body: ReadableStream<Uint8Array>, signal: AbortSignal, onMessage: (message: T) => boolean | void): Promise<void> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let pending = ''
  const abort = () => { void reader.cancel().catch(() => {}) }
  signal.addEventListener('abort', abort, { once: true })
  function deliver(line: string): boolean {
    if (!line.trim()) return true
    if (line.length > 1_048_576) throw new Error('Gateway stream frame is too large.')
    const frame = JSON.parse(line)
    if (frame.error) throw new ApiError(200, frame.error.code, frame.error.message || 'Stream ended with an error.')
    return onMessage(frame.result as T) !== false
  }
  try {
    signal.throwIfAborted()
    while (true) {
      const { value, done } = await reader.read()
      signal.throwIfAborted()
      pending += decoder.decode(value, { stream: !done })
      let newline: number
      while ((newline = pending.indexOf('\n')) !== -1) {
        const line = pending.slice(0, newline)
        pending = pending.slice(newline + 1)
        if (!deliver(line)) return
      }
      if (pending.length > 1_048_576) throw new Error('Gateway stream frame is too large.')
      if (done) {
        if (pending.trim()) deliver(pending)
        return
      }
    }
  } finally {
    signal.removeEventListener('abort', abort)
    await reader.cancel().catch(() => {})
    reader.releaseLock()
  }
}

export function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) { reject(signal.reason); return }
    const abort = () => { clearTimeout(timer); reject(signal.reason) }
    const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve() }, ms)
    signal.addEventListener('abort', abort, { once: true })
  })
}
