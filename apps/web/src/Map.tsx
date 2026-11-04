import { useEffect, useRef } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { CENTER, type Point } from './types'

interface Props { pickup?: Point | null; dropoff?: Point | null; driver?: Point | null; onPick: (p: Point) => void; selecting: string }
export default function Map({ pickup, dropoff, driver, onPick, selecting }: Props) {
  const host = useRef<HTMLDivElement>(null)
  const map = useRef<L.Map | null>(null)
  const layer = useRef<L.LayerGroup | null>(null)
  const pick = useRef(onPick)
  pick.current = onPick
  useEffect(() => {
    if (!host.current) return
    const m = L.map(host.current, { zoomControl: false }).setView([CENTER.lat!, CENTER.lng!], 13)
    L.control.zoom({ position: 'bottomright' }).addTo(m)
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors', maxZoom: 19,
    }).addTo(m)
    m.on('click', e => pick.current({ lat: e.latlng.lat, lng: e.latlng.lng }))
    layer.current = L.layerGroup().addTo(m)
    map.current = m
    const observer = new ResizeObserver(() => m.invalidateSize())
    observer.observe(host.current)
    return () => { observer.disconnect(); m.remove(); map.current = null }
  }, [])
  useEffect(() => {
    const group = layer.current
    if (!group) return
    group.clearLayers()
    const pin = (p: Point, letter: string, color: string, label: string) => {
      L.marker([p.lat ?? 0, p.lng ?? 0], { icon: L.divIcon({ className: '', html: `<span class="map-pin" style="--pin:${color}">${letter}</span>`, iconSize: [36, 36], iconAnchor: [18, 18] }) }).bindTooltip(label).addTo(group)
    }
    if (pickup) pin(pickup, 'A', '#1d6b56', 'Pickup')
    if (dropoff) pin(dropoff, 'B', '#da8b42', 'Dropoff')
    if (driver) pin(driver, '↗', '#274d83', 'Driver’s last reported position')
    if (pickup && dropoff) L.polyline([[pickup.lat ?? 0, pickup.lng ?? 0], [dropoff.lat ?? 0, dropoff.lng ?? 0]], { color: '#1d6b56', weight: 3, dashArray: '7 9' }).addTo(group)
  }, [pickup, dropoff, driver])
  return <div className="map-wrap">
    <div ref={host} className="map" aria-label="Trip map; click to choose a location" />
    <div className="map-note"><span className="live-dot" /> {selecting || 'Your trip, live'} <small>Dashed line is not a driving route</small></div>
  </div>
}
