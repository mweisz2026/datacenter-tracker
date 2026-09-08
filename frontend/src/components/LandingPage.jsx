import { useState, useEffect } from 'react'
import axios from 'axios'
import { AlertTriangle, AlertCircle, ExternalLink, RefreshCw } from 'lucide-react'

function formatDate(dateStr) {
  if (!dateStr) return ''
  try {
    const d = new Date(dateStr)
    const now = new Date()
    const diffH = Math.floor((now - d) / 3600000)
    if (diffH < 1) return 'just now'
    if (diffH < 24) return `${diffH}h ago`
    const diffD = Math.floor(diffH / 24)
    if (diffD < 7) return `${diffD}d ago`
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  } catch {
    return dateStr.slice(0, 10)
  }
}

function ratingColor(r) {
  if (!r) return 'text-muted'
  const s = r.toUpperCase()
  if (s.includes('BBB') || s.includes('AAA') || s.includes('AA') || s.includes('A+')) return 'text-green'
  if (s.includes('BB') || s.includes('BA')) return 'text-yellow'
  return 'text-red'
}

const ALERT_STYLE = {
  CRITICAL: { bg: 'bg-red/10 border border-red/30',    text: 'text-red',    badge: 'bg-red/20 text-red',       Icon: AlertTriangle },
  HIGH:     { bg: 'bg-yellow/10 border border-yellow/30', text: 'text-yellow', badge: 'bg-yellow/20 text-yellow', Icon: AlertCircle },
}

export default function LandingPage({ bonds, onSelectBond }) {
  const [weather, setWeather]         = useState({})
  const [alerts, setAlerts]           = useState([])
  const [alertsLoading, setLoading]   = useState(true)
  const [asOf, setAsOf]               = useState(null)

  function loadAlerts() {
    setLoading(true)
    axios.get('/api/alerts')
      .then(r => { setAlerts(r.data.alerts || []); setAsOf(r.data.as_of) })
      .catch(() => setAlerts([]))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    axios.get('/api/weather_all').then(r => setWeather(r.data || {})).catch(() => {})
    loadAlerts()
  }, [])

  return (
    <div className="p-4 space-y-5 max-w-screen-2xl mx-auto">

      {/* ── Portfolio Table ──────────────────────────────────────────────── */}
      <div className="card overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <span className="section-header">Portfolio Overview</span>
          <span className="text-xs text-muted font-mono">{bonds.length} bonds tracked</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted uppercase tracking-wide" style={{ fontSize: '10px' }}>
                <th className="text-left px-3 py-2 font-medium">Bond</th>
                <th className="text-left px-3 py-2 font-medium hidden md:table-cell">Operator</th>
                <th className="text-left px-3 py-2 font-medium">Tenant</th>
                <th className="text-left px-3 py-2 font-medium hidden lg:table-cell">Location</th>
                <th className="text-center px-3 py-2 font-medium">Rtg</th>
                <th className="text-right px-3 py-2 font-medium">Cpn</th>
                <th className="text-right px-3 py-2 font-medium hidden sm:table-cell">Maturity</th>
                <th className="text-right px-3 py-2 font-medium">Price</th>
                <th className="text-right px-3 py-2 font-medium">STW</th>
                <th className="text-right px-3 py-2 font-medium">YTW</th>
                <th className="text-left px-3 py-2 font-medium hidden xl:table-cell">Weather</th>
              </tr>
            </thead>
            <tbody>
              {bonds.map(bond => {
                const wx   = weather[bond.id]
                const temp = wx?.current?.temperature
                const fcst = wx?.current?.short_forecast || ''
                const wxStr = temp !== undefined
                  ? `${Math.round(temp)}°F · ${fcst.split(' ').slice(0, 3).join(' ')}`
                  : null

                return (
                  <tr
                    key={bond.id}
                    className="border-b border-border/40 hover:bg-white/5 cursor-pointer transition-colors group"
                    onClick={() => onSelectBond(bond.id)}
                  >
                    {/* Bond name + size */}
                    <td className="px-3 py-2.5">
                      <div className="font-semibold text-primary group-hover:text-blue transition-colors">
                        {bond.name}
                      </div>
                      <div className="text-muted" style={{ fontSize: '10px' }}>
                        ${((bond.tranche_size_mm || 0) / 1000).toFixed(1)}B
                      </div>
                    </td>

                    {/* Operator */}
                    <td className="px-3 py-2.5 text-muted hidden md:table-cell">
                      <div className="truncate max-w-[130px]">{bond.guarantor || '—'}</div>
                    </td>

                    {/* Tenant */}
                    <td className="px-3 py-2.5">
                      <span className="text-blue font-medium">{bond.lease_counterparty || '—'}</span>
                    </td>

                    {/* Location */}
                    <td className="px-3 py-2.5 text-muted hidden lg:table-cell">
                      <div className="truncate max-w-[150px]">{bond.location_display || '—'}</div>
                    </td>

                    {/* Rating */}
                    <td className="px-3 py-2.5 text-center">
                      <span className={`font-mono font-bold ${ratingColor(bond.ratings)}`}>
                        {bond.ratings || 'NR'}
                      </span>
                    </td>

                    {/* Coupon */}
                    <td className="px-3 py-2.5 text-right font-mono text-primary">
                      {bond.coupon ? `${bond.coupon}%` : '—'}
                    </td>

                    {/* Maturity */}
                    <td className="px-3 py-2.5 text-right font-mono text-muted hidden sm:table-cell">
                      {bond.maturity
                        ? new Date(bond.maturity).toLocaleDateString('en-US', { month: 'short', year: '2-digit' })
                        : '—'}
                    </td>

                    {/* Price */}
                    <td className="px-3 py-2.5 text-right font-mono">
                      {bond.price != null ? (
                        <span className={
                          bond.price >= 100 ? 'text-green' :
                          bond.price < 90  ? 'text-red' : 'text-yellow'
                        }>
                          {Number(bond.price).toFixed(2)}
                        </span>
                      ) : <span className="text-muted/50">—</span>}
                    </td>

                    {/* STW */}
                    <td className="px-3 py-2.5 text-right font-mono">
                      {bond.stw_bps != null ? (
                        <span className={
                          bond.stw_bps > 600 ? 'text-red' :
                          bond.stw_bps > 350 ? 'text-yellow' : 'text-green'
                        }>
                          +{Math.round(bond.stw_bps)}
                        </span>
                      ) : <span className="text-muted/50">—</span>}
                    </td>

                    {/* YTW */}
                    <td className="px-3 py-2.5 text-right font-mono text-primary">
                      {bond.ytw != null ? `${Number(bond.ytw).toFixed(2)}%` : <span className="text-muted/50">—</span>}
                    </td>

                    {/* Weather */}
                    <td className="px-3 py-2.5 hidden xl:table-cell">
                      {wxStr
                        ? <span className="text-muted whitespace-nowrap">{wxStr}</span>
                        : <span className="text-muted/30">—</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Cross-bond Material Alerts ───────────────────────────────────── */}
      <div className="card p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <AlertTriangle size={13} className="text-red" />
            <span className="section-header">Material Alerts — All Bonds</span>
            {alerts.length > 0 && (
              <span className="text-xs font-mono text-muted">({alerts.length})</span>
            )}
          </div>
          <div className="flex items-center gap-3">
            {asOf && !alertsLoading && (
              <span className="text-xs text-muted font-mono">
                as of {new Date(asOf * 1000).toLocaleTimeString()}
              </span>
            )}
            <button
              onClick={loadAlerts}
              disabled={alertsLoading}
              className="text-muted hover:text-primary transition-colors disabled:opacity-40"
              title="Refresh alerts"
            >
              <RefreshCw size={12} className={alertsLoading ? 'animate-spin' : ''} />
            </button>
          </div>
        </div>

        {alertsLoading ? (
          <div className="flex flex-col items-center gap-3 py-10 text-muted">
            <div className="spinner" />
            <span className="text-xs">Scanning all {bonds.length} bonds for material alerts…</span>
            <span className="text-xs text-muted/50">First load takes ~15s — results cache for 25 min</span>
          </div>
        ) : alerts.length === 0 ? (
          <div className="text-muted text-xs text-center py-8">
            No material alerts across portfolio
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-2">
            {alerts.map((alert, i) => {
              const cat   = alert.importance_category || 'HIGH'
              const style = ALERT_STYLE[cat] || ALERT_STYLE.HIGH
              const Icon  = style.Icon
              return (
                <a
                  key={i}
                  href={alert.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={`block p-3 rounded cursor-pointer transition-opacity hover:opacity-90 ${style.bg}`}
                >
                  <div className="flex items-start gap-2">
                    <Icon size={13} className={`flex-shrink-0 mt-0.5 ${style.text}`} />
                    <div className="flex-1 min-w-0">
                      {/* Bond + severity badges */}
                      <div className="flex items-center gap-1.5 flex-wrap mb-1">
                        <button
                          onClick={e => { e.preventDefault(); onSelectBond(alert.bond_id) }}
                          className="text-xs px-1.5 py-0.5 rounded font-mono font-bold bg-white/10 text-primary hover:bg-white/20 transition-colors"
                        >
                          {alert.bond_name}
                        </button>
                        <span className={`text-xs px-1.5 py-0.5 rounded font-mono font-bold ${style.badge}`}>
                          {cat}
                        </span>
                        {typeof alert.importance_score === 'number' && (
                          <span className="text-xs px-1.5 py-0.5 rounded font-mono font-bold bg-white/10 text-primary" title="Relevance score (0-10)">
                            {alert.importance_score}/10
                          </span>
                        )}
                      </div>
                      <div className={`text-xs font-semibold leading-snug line-clamp-2 ${style.text}`}>
                        {alert.title}
                      </div>
                      {(alert.importance_blurb || alert.importance_reason) && (
                        <div className="text-xs text-muted mt-0.5 italic line-clamp-3">
                          {alert.importance_blurb || alert.importance_reason}
                        </div>
                      )}
                      <div className="flex items-center gap-2 mt-1.5">
                        <span className="text-xs text-muted truncate">{alert.source}</span>
                        <span className="text-xs text-muted ml-auto flex-shrink-0">{formatDate(alert.published)}</span>
                        <ExternalLink size={9} className="text-muted flex-shrink-0" />
                      </div>
                    </div>
                  </div>
                </a>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
