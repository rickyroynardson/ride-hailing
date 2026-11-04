import { useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { Api, ApiError } from './api'
import { useCharge, useDriver, useRide } from './hooks'
import { acknowledgeBooking, acknowledgePaymentRetry, bookingIntent, pendingBooking, paymentRetryIntent } from './intents'
import Map from './Map'
import { CENTER, coords, money, terminal, type AuthResult, type Money, type Point, type Profile, type Ride } from './types'

const humanError = (e: unknown) => e instanceof Error ? e.message : String(e)
const route = (id: string) => `/v1/rides/${encodeURIComponent(id)}`
const storedRide = (userId: string) => { try { return sessionStorage.getItem(`jalan:ride:${userId}`) || '' } catch { return '' } }
const storedMode = (userId: string): 'rider' | 'driver' => { try { return sessionStorage.getItem(`jalan:mode:${userId}`) === 'driver' ? 'driver' : 'rider' } catch { return 'rider' } }
function saveRide(userId: string, id: string) { try { if (id) sessionStorage.setItem(`jalan:ride:${userId}`, id); else sessionStorage.removeItem(`jalan:ride:${userId}`) } catch { /* private browser storage may be disabled */ } }

export default function App() {
  const api = useMemo(() => new Api(), [])
  const [profile, setProfile] = useState<Profile | null>(null)
  const [expired, setExpired] = useState(false)
  useEffect(() => { api.onExpired = () => { setProfile(null); setExpired(true) }; return () => { api.onExpired = undefined } }, [api])
  return <div className="app-shell">
    <header className="topbar"><a className="brand" href="/" aria-label="Jalan home"><span className="brand-symbol">↗</span>jalan<span className="brand-period">.</span></a>
      <div className="tagline">A little closer to where you want to be.</div><span className="environment">LOCAL PREVIEW</span>
    </header>
    {profile ? <Workspace key={profile.user.id} api={api} profile={profile} setProfile={setProfile} /> : <Auth api={api} expired={expired} onSignedIn={setProfile} />}
    <footer>Built for the journey. <span>Gateway-only client · Test payments, no real money</span></footer>
  </div>
}

function Auth({ api, onSignedIn, expired }: { api: Api; onSignedIn: (p: Profile) => void; expired: boolean }) {
  const [register, setRegister] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(expired ? 'Your session expired. Please sign in again.' : '')
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setBusy(true); setError('')
    const form = new FormData(e.currentTarget)
    try {
      const result = await api.request<AuthResult>(`/v1/auth/${register ? 'register' : 'login'}`, 'POST', Object.fromEntries(form), undefined, false)
      api.setSession(result.tokens)
      onSignedIn(await api.request<Profile>('/v1/me'))
    } catch (e) { setError(humanError(e)) }
    finally { setBusy(false) }
  }
  return <main className="auth-layout">
    <section className="intro"><span className="eyebrow">GOOD PLACES ARE A RIDE AWAY</span><h1>Life moves.<br /><em>Go with it.</em></h1><p>A ride across town. A fresh start to your day.<br />Your next destination starts right here.</p>
      <div className="journey-art" aria-hidden="true"><div className="road road-one" /><div className="road road-two" /><span className="art-pin art-a">A</span><span className="art-pin art-b">B</span><span className="art-car">↗</span><span className="art-label">JAKARTA, INDONESIA</span></div>
      <div className="intro-caption"><span>01 /</span> Less waiting. More living.</div>
    </section>
    <section className="auth-card"><span className="eyebrow">WELCOME TO JALAN</span><h2>{register ? 'Your next chapter.' : 'Good to see you.'}</h2><p>{register ? 'One account. Ride when you need to, drive when you’re ready.' : 'Sign in to find your ride or get behind the wheel.'}</p>
      <div className="segmented"><button type="button" disabled={busy} className={!register ? 'selected' : ''} onClick={() => { setRegister(false); setError('') }}>Sign in</button><button type="button" disabled={busy} className={register ? 'selected' : ''} onClick={() => { setRegister(true); setError('') }}>Create account</button></div>
      <form onSubmit={submit}>{register && <label>Full name<input name="name" autoComplete="name" required maxLength={100} placeholder="What should we call you?" /></label>}
        <label>Email address<input name="email" type="email" autoComplete="email" required placeholder="you@example.com" /></label>
        {register && <label>Phone number<input name="phone" type="tel" autoComplete="tel" placeholder="+62…" /></label>}
        <label>Password<input name="password" type="password" autoComplete={register ? 'new-password' : 'current-password'} minLength={8} maxLength={72} required placeholder="At least 8 characters" /></label>
        <ErrorNote>{error}</ErrorNote><button className="primary wide" disabled={busy}>{busy ? 'One moment…' : register ? 'Create my account ↗' : 'Let’s get going ↗'}</button>
      </form><small className="muted">Credentials stay in this tab’s memory. Reloading signs you out.</small>
    </section>
  </main>
}

function Workspace({ api, profile, setProfile }: { api: Api; profile: Profile; setProfile: (p: Profile | null) => void }) {
  const [mode, setMode] = useState<'rider' | 'driver'>(() => storedMode(profile.user.id))
  const [rideId, setRideId] = useState(() => storedRide(profile.user.id))
  const [pending, setPending] = useState(() => pendingBooking(profile.user.id))
  const [pickup, setPickup] = useState<Point>(() => pending?.pickup || CENTER)
  const [dropoff, setDropoff] = useState<Point>(() => pending?.dropoff || { lat: -6.23, lng: 106.835 })
  const [position, setPosition] = useState<Point>(CENTER)
  const [selecting, setSelecting] = useState<'pickup' | 'dropoff'>('pickup')
  const [fare, setFare] = useState<Money | null>(null)
  const [online, setOnline] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [now, setNow] = useState(Date.now())
  const [recovering, setRecovering] = useState(true)
  const [recoveryError, setRecoveryError] = useState('')
  const [recoveryRevision, setRecoveryRevision] = useState(0)
  const recoveryEpoch = useRef(0)
  const watched = useRide(api, rideId)
  const driver = useDriver(api, online, position)
  const active = !!rideId && !terminal(watched.ride)
  const approved = profile.driverProfile?.status === 'approved'
  const blocked = busy || recovering || !!recoveryError
  useEffect(() => { const t = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(t) }, [])
  useEffect(() => {
    if (watched.ride?.driverId === profile.user.id) setMode('driver')
  }, [watched.ride?.driverId, profile.user.id])
  useEffect(() => { try { sessionStorage.setItem(`jalan:mode:${profile.user.id}`, mode) } catch { /* optional tab state */ } }, [mode, profile.user.id])

  function remember(id: string) { recoveryEpoch.current++; setRecovering(false); setRideId(id); saveRide(profile.user.id, id) }
  useEffect(() => {
    let request: AbortController | undefined
    async function recover() {
      request?.abort()
      const controller = new AbortController()
      request = controller
      const epoch = recoveryEpoch.current
      setRecovering(true)
      try {
        const result = await api.request<{ ride?: Ride | null }>('/v1/rides/current', 'GET', undefined, controller.signal)
        if (controller.signal.aborted || epoch !== recoveryEpoch.current) return
        setRecoveryError('')
        if (result.ride) {
          setRideId(result.ride.rideId)
          saveRide(profile.user.id, result.ride.rideId)
          setMode(result.ride.driverId === profile.user.id ? 'driver' : 'rider')
          if (pendingBooking(profile.user.id)?.requestId === result.ride.requestId) {
            acknowledgeBooking(profile.user.id)
            setPending(null)
          }
        }
      } catch (e) {
        if (!controller.signal.aborted && epoch === recoveryEpoch.current) setRecoveryError(humanError(e))
      } finally {
        if (!controller.signal.aborted && epoch === recoveryEpoch.current) setRecovering(false)
      }
    }
    const onFocus = () => { void recover() }
    void recover()
    window.addEventListener('online', onFocus)
    window.addEventListener('focus', onFocus)
    return () => { request?.abort(); window.removeEventListener('online', onFocus); window.removeEventListener('focus', onFocus) }
  }, [api, profile.user.id, recoveryRevision])
  async function perform(fn: () => Promise<void>) {
    setBusy(true); setError(''); setNotice('')
    try { await fn() } catch (e) { setError(humanError(e)) } finally { setBusy(false) }
  }
  function choose(p: Point) {
    if (mode === 'driver') setPosition(p)
    else if (!active && !pending) { setFare(null); if (selecting === 'pickup') setPickup(p); else setDropoff(p) }
  }
  async function offline() { setOnline(false); await api.request('/v1/driver/location', 'DELETE') }
  async function signOut() {
    await perform(async () => {
      try { if (online) await offline() } finally { try { await api.logout() } finally { setProfile(null) } }
    })
  }
  const offer = driver.connection === 'live' && driver.offer && Date.parse(driver.offer.expiresAt) > now ? driver.offer : null
  const seconds = offer ? Math.max(0, Math.ceil((Date.parse(offer.expiresAt) - now) / 1000)) : 0

  return <main className="workspace">
    <div className="workspace-heading"><div><span className="eyebrow">YOUR EVERYDAY, IN MOTION</span><h1>Hi, {profile.user.name.split(' ')[0] || 'there'}<span className="wave">✳</span></h1></div>
      <div className="account"><span>{profile.user.email}</span><button className="text-button" onClick={signOut} disabled={busy}>Sign out</button></div>
    </div>
    <div className="workspace-grid"><aside className="control-panel">
      <div className="segmented mode-switch"><button className={mode === 'rider' ? 'selected' : ''} disabled={busy || active || online} onClick={() => setMode('rider')}>I need a ride</button><button className={mode === 'driver' ? 'selected' : ''} disabled={busy || active} onClick={() => setMode('driver')}>I’m driving</button></div>
      <ErrorNote>{error}</ErrorNote>{notice && <p className="notice" role="status">{notice}</p>}
      {recovering && <p className="muted" role="status">Checking for your current ride…</p>}
      <ErrorNote>{recoveryError && `Could not check your current ride: ${recoveryError}`}</ErrorNote>
      {recoveryError && <button className="secondary wide" onClick={() => setRecoveryRevision(n => n + 1)}>Retry trip recovery</button>}
      {mode === 'rider' ? <>
        {!active && <section><span className="eyebrow">WHERE TO?</span><h2>A new destination.</h2><p className="muted">Choose points on the map, or enter coordinates below.</p>
          <PointInput label="Pickup" point={pickup} onChange={p => { setPickup(p); setFare(null) }} disabled={!!pending} selected={selecting === 'pickup'} onSelect={() => setSelecting('pickup')} />
          <PointInput label="Dropoff" point={dropoff} onChange={p => { setDropoff(p); setFare(null) }} disabled={!!pending} selected={selecting === 'dropoff'} onSelect={() => setSelecting('dropoff')} />
          <div className="fare-strip"><span>Estimated fare</span><strong>{money(fare)}</strong></div>
          <div className="button-row"><button className="secondary" disabled={busy} onClick={() => perform(async () => { const res = await api.request<{ fare: Money }>('/v1/fares:estimate', 'POST', { pickup, dropoff }); setFare(res.fare) })}>Estimate</button>
            <button className="primary" disabled={blocked || (!fare && !pending) || !!rideId} onClick={() => perform(async () => {
              const intent = bookingIntent(profile.user.id, pickup, dropoff)
              setPending(intent)
              const res = await api.request<{ rideId: string; fare: Money }>('/v1/rides', 'POST', intent)
              remember(res.rideId)
              acknowledgeBooking(profile.user.id)
              setPending(null)
            })}>{busy ? 'Please wait…' : pending ? 'Retry original booking ↗' : 'Request ride ↗'}</button></div>
          <small className="muted">{pending ? 'The previous booking has not been acknowledged. Retrying keeps the same request ID and coordinates; it cannot create a second ride.' : 'Estimate first. The fare returned at booking is the final upfront price. A lost response can be safely retried.'}</small>
        </section>}
      </> : <>
        {!approved ? <DriverApplication api={api} profile={profile} onUpdate={setProfile} /> : <section>
          <div className="section-heading"><h2>Your driver seat.</h2><span className={`badge ${online ? 'green' : ''}`}>{online ? 'Online' : 'Offline'}</span></div>
          <p className="muted">Manual location mode: click the map to move your reported position. This preview does not use GPS or navigate.</p>
          <PointInput label="My position" point={position} onChange={setPosition} selected />
          <button className={online ? 'secondary wide' : 'primary wide'} disabled={online ? busy : blocked} onClick={() => perform(async () => {
            if (online) await offline()
            else { await api.refresh(true); setOnline(true) }
          })}>{online ? 'Go offline' : 'Go online ↗'}</button>
          <div className="driver-meta"><span className={`live-dot ${driver.connection === 'live' ? '' : 'quiet'}`} /> Offers: {online ? driver.connection : 'offline'}<span>{driver.lastSent ? `Position ${Math.max(0, Math.floor((now - driver.lastSent) / 1000))}s ago` : 'Position not sent'}</span></div>
          <ErrorNote>{driver.error}</ErrorNote>
          {offer && !active ? <div className="offer-card"><span className="eyebrow">A RIDE NEAR YOU · {seconds}s</span><h3>Someone’s ready to go.</h3><p>Pickup: {coords(offer.pickup)}</p><div className="button-row">
            <button className="secondary" disabled={busy} onClick={() => perform(async () => { await api.request(`/v1/offers/${encodeURIComponent(offer.offerId)}:decline`, 'POST', {}) })}>Decline</button>
            <button className="primary" disabled={blocked} onClick={() => perform(async () => {
              // Remember the candidate first: a timeout may mean the assignment succeeded but its reply was lost.
              saveRide(profile.user.id, offer.rideId)
              try {
                const res = await api.request<{ rideId: string }>(`/v1/offers/${encodeURIComponent(offer.offerId)}:accept`, 'POST', {})
                remember(res.rideId)
              } catch (e) {
                if (!(e instanceof ApiError) || [503, 504].includes(e.status)) remember(offer.rideId)
                else saveRide(profile.user.id, rideId)
                throw e
              }
            })}>Accept ride ↗</button>
          </div></div> : online && !active && <div className="waiting"><span>◎</span> Ready when they are.<small>New offers appear here automatically.</small></div>}
          <Earnings api={api} revision={watched.ride?.status} />
        </section>}
      </>}
      {rideId && <section className="ride-section"><RideCard api={api} userId={profile.user.id} id={rideId} ride={watched.ride} connection={watched.connection} error={watched.error} mode={mode} busy={busy} perform={perform} onDone={() => remember('')} /></section>}
      {!active && <details className="resume"><summary>Resume a ride by ID</summary><form onSubmit={e => { e.preventDefault(); const id = String(new FormData(e.currentTarget).get('rideId')).trim(); if (id) remember(id) }}><label>Ride ID<input name="rideId" required placeholder="Paste your ride ID" /></label><button className="secondary" disabled={busy}>Resume</button></form></details>}
      {active && !watched.ride && watched.error && <button className="text-button" onClick={() => remember('')}>Dismiss this ride ID</button>}
      {mode === 'rider' && <PaymentMethod api={api} />}
    </aside>
    <div className="map-panel"><Map pickup={active ? watched.ride?.pickup : mode === 'rider' ? pickup : offer?.pickup} dropoff={active ? watched.ride?.dropoff : mode === 'rider' ? dropoff : null} driver={mode === 'driver' ? position : watched.ride?.driverLocation} onPick={choose} selecting={mode === 'driver' ? 'Click to update your driver position' : active ? '' : `Click to set your ${selecting}`} />
      <div className="map-bottom"><div><span className="eyebrow">A BETTER WAY THROUGH THE CITY</span><h3>The city is yours.</h3></div><span className="city-code">JKT <span>↗</span></span></div>
    </div></div>
  </main>
}

function RideCard({ api, userId, id, ride, connection, error, mode, busy, perform, onDone }: { api: Api; userId: string; id: string; ride: Ride | null; connection: string; error: string; mode: string; busy: boolean; perform: (fn: () => Promise<void>) => Promise<void>; onDone: () => void }) {
  const payment = useCharge(api, ride)
  const labels: Record<string, [string, string]> = {
    RIDE_STATUS_REQUESTED: ['Finding your match.', 'We’re looking for an available driver nearby.'],
    RIDE_STATUS_ACCEPTED: ['You’re on your way.', 'Your driver is heading to the pickup.'],
    RIDE_STATUS_IN_PROGRESS: ['Enjoy the journey.', 'Your trip is in progress.'],
    RIDE_STATUS_COMPLETED: ['You have arrived.', 'Thanks for taking the journey with Jalan.'],
    RIDE_STATUS_CANCELLED: ['Plans change.', ride?.cancelReason === 'CANCEL_REASON_NO_DRIVER' ? 'No driver was available. Please try again later.' : 'This ride was cancelled.'],
  }
  const [title, detail] = ride ? labels[ride.status] || ['Updating…', 'Waiting for the latest state.'] : ['Connecting to your ride…', 'Getting the latest trip details.']
  const connected = connection === 'live'
  return <><div className="section-heading"><span className="eyebrow">YOUR TRIP</span><span className="badge">{connection}</span></div>
    <h2>{title}</h2><p className="muted">{detail}</p><ErrorNote>{error}</ErrorNote>
    <div className="ride-id"><span>Ride ID · keep this to resume</span><code>{id}</code></div>
    {ride && <><div className="trip-points"><p><b>A</b>{coords(ride.pickup)}</p><p><b>B</b>{coords(ride.dropoff)}</p></div><div className="fare-strip"><span>Upfront fare</span><strong>{money(ride.fare)}</strong></div>
      {mode === 'rider' && ['RIDE_STATUS_REQUESTED', 'RIDE_STATUS_ACCEPTED'].includes(ride.status) && <><button className="secondary wide" disabled={busy || !connected} onClick={() => perform(async () => { await api.request(`${route(id)}:cancel`, 'POST', {}) })}>Cancel ride</button><small className="muted">Cancelling after a driver accepts costs Rp 5.000.</small></>}
      {mode === 'driver' && ['RIDE_STATUS_ACCEPTED', 'RIDE_STATUS_IN_PROGRESS'].includes(ride.status) && <button className="primary wide" disabled={busy || !connected} onClick={() => perform(async () => { await api.request(`${route(id)}:${ride.status === 'RIDE_STATUS_ACCEPTED' ? 'start' : 'complete'}`, 'POST', {}) })}>{ride.status === 'RIDE_STATUS_ACCEPTED' ? 'Rider picked up · Start trip' : 'Rider dropped off · Complete trip'}</button>}
      {terminal(ride) && <div className="payment-summary"><span className="eyebrow">PAYMENT</span><p>{payment.free ? 'No charge for this cancellation.' : payment.charge ? payment.charge.status === 'CHARGE_STATUS_CAPTURED' ? `Paid · ${money(payment.charge.amount)}` : payment.charge.status === 'CHARGE_STATUS_FAILED' ? `Not paid · ${payment.charge.declineReason || 'Card declined'}` : 'Processing your payment…' : 'Waiting for the payment service…'}</p>
        {payment.charge?.refundedAmount && payment.charge.refundedAmount !== '0' && <p>Refunded · {money({ ...payment.charge.amount, amount: payment.charge.refundedAmount })}</p>}
        <ErrorNote>{payment.error}</ErrorNote>
        {mode === 'rider' && payment.charge?.status === 'CHARGE_STATUS_FAILED' && <button className="primary wide" disabled={busy} onClick={() => perform(async () => {
          await api.request(`${route(id)}/charge:retry`, 'POST', { requestId: paymentRetryIntent(userId, id) })
          acknowledgePaymentRetry(userId, id)
          payment.reload()
        })}>Retry payment</button>}
        {!payment.free && <button className="text-button" onClick={payment.reload}>Check payment</button>}
        <button className="secondary wide" onClick={onDone} disabled={busy || (mode === 'rider' && !payment.free && payment.charge?.status !== 'CHARGE_STATUS_CAPTURED')}>Done · Back to planning</button>
      </div>}
    </>}
  </>
}

function DriverApplication({ api, profile, onUpdate }: { api: Api; profile: Profile; onUpdate: (p: Profile) => void }) {
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  async function update(body?: unknown) {
    setBusy(true); setError('')
    try {
      if (body) await api.request('/v1/me/driver', 'POST', body)
      else await api.refresh(true) // driver claim changes only when a fresh token is issued
      onUpdate(await api.request<Profile>('/v1/me'))
    } catch (e) { setError(humanError(e)) } finally { setBusy(false) }
  }
  return <section><span className="eyebrow">DRIVE WITH JALAN</span><h2>{profile.driverProfile ? 'One more step.' : 'Take the wheel.'}</h2>
    {profile.driverProfile ? <><p>Your application is <strong>{profile.driverProfile.status}</strong>.</p><p className="muted">An administrator must approve your profile. This app cannot approve or reinstate drivers.</p><button className="secondary wide" disabled={busy} onClick={() => update()}>Refresh approval status</button></> : <form onSubmit={e => { e.preventDefault(); void update(Object.fromEntries(new FormData(e.currentTarget))) }}><p className="muted">Apply below. You can go online once your application is approved.</p><label>Driving license<input name="licenseNumber" required /></label><label>Vehicle plate<input name="vehiclePlate" required /></label><label>Vehicle model<input name="vehicleModel" placeholder="Toyota Avanza" required /></label><button className="primary wide" disabled={busy}>Apply to drive ↗</button></form>}
    <ErrorNote>{error}</ErrorNote></section>
}

function PaymentMethod({ api }: { api: Api }) {
  const [error, setError] = useState('')
  const [saved, setSaved] = useState(false)
  const [busy, setBusy] = useState(false)
  return <details className="payment-method"><summary>Payment method <span>Test provider</span></summary><p className="muted">No real card details. Choose a test token; a new account uses the successful default card.</p><form onSubmit={async e => {
    e.preventDefault(); setBusy(true); setError(''); setSaved(false)
    const token = new FormData(e.currentTarget).get('token')
    try { await api.request('/v1/me/payment-method', 'PUT', { token }); setSaved(true) } catch (e) { setError(humanError(e)) } finally { setBusy(false) }
  }}><label>Test card<select name="token"><option value="tok_ok">Successful payment</option><option value="tok_declined">Declined card</option><option value="tok_lost_response">Lost reply (safe retry)</option></select></label><button className="secondary wide" disabled={busy}>Save payment method</button></form><ErrorNote>{error}</ErrorNote>{saved && <p className="notice" role="status">Payment method updated.</p>}</details>
}

function Earnings({ api, revision }: { api: Api; revision?: string }) {
  const [balances, setBalances] = useState<Money[]>([])
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  async function load(signal?: AbortSignal) {
    setBusy(true)
    try { const res = await api.request<{ balances?: Money[] }>('/v1/driver/earnings', 'GET', undefined, signal); if (!signal?.aborted) { setBalances(res.balances ?? []); setError('') } }
    catch (e) { if (!signal?.aborted) setError(humanError(e)) }
    finally { if (!signal?.aborted) setBusy(false) }
  }
  useEffect(() => { const c = new AbortController(); void load(c.signal); return () => c.abort() }, [api, revision])
  return <div className="earnings"><div><span className="eyebrow">YOUR EARNINGS</span><strong>{balances.length ? balances.map(money).join(' · ') : 'Rp 0'}</strong><small className="muted">Ledger balance, not a payout.</small></div><button className="text-button" onClick={() => load()} disabled={busy}>Refresh</button><ErrorNote>{error}</ErrorNote></div>
}

function PointInput({ label, point, onChange, selected, onSelect, disabled }: { label: string; point: Point; onChange: (p: Point) => void; selected?: boolean; onSelect?: () => void; disabled?: boolean }) {
  return <fieldset disabled={disabled} className={`point-input ${selected ? 'focused' : ''}`}>
    <button type="button" className="point-label" onClick={onSelect}><span>{label === 'Dropoff' ? 'B' : label === 'Pickup' ? 'A' : '↗'}</span>{label}</button>
    <div className="coordinate-row"><label>Latitude<input type="number" step="any" min={-85} max={85} aria-label={`${label} latitude`} value={point.lat ?? 0} onChange={e => { const n = e.currentTarget.valueAsNumber; if (Number.isFinite(n) && n >= -85 && n <= 85) onChange({ ...point, lat: n }) }} /></label><label>Longitude<input type="number" step="any" min={-180} max={180} aria-label={`${label} longitude`} value={point.lng ?? 0} onChange={e => { const n = e.currentTarget.valueAsNumber; if (Number.isFinite(n) && n >= -180 && n <= 180) onChange({ ...point, lng: n }) }} /></label></div>
  </fieldset>
}

function ErrorNote({ children }: { children?: ReactNode }) { return children ? <div className="error-note" role="alert">{children}</div> : null }
