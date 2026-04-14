import { useEffect, useState } from 'react'
import axios from 'axios'
import { MapPin, Building2, Calendar, Zap } from 'lucide-react'
import KeyMetrics from './KeyMetrics'
import BondDetails from './BondDetails'
import WeatherWidget from './WeatherWidget'
import NewsFeed from './NewsFeed'

const RATING_BADGE = (r) => {
  if (!r) return 'badge-nr'
  const s = r.toUpperCase()
  if (s.startsWith('AAA') || s.startsWith('AA') || s.startsWith('A+') || s.startsWith('BBB')) return 'badge-ig'
  if (s.includes('BB') || s.includes('BA1') || s.includes('BA2')) return 'badge-hy'
  if (s.includes('B3') || s.includes('B+') || s.includes('/B/') || s.startsWith('/B')) return 'badge-ccc'
  return 'badge-nr'
}

const PRODUCT_COLOR = (p) => {
  if (!p) return 'text-muted'
  return p.includes('Turnkey') ? 'text-green' : 'text-purple'
}

export default function BondPage({ bond, activeSubId }) {
  const [weather, setWeather] = useState(null)
  const [news, setNews] = useState(null)
  const [weatherLoading, setWeatherLoading] = useState(true)
  const [newsLoading, setNewsLoading] = useState(true)

  const displayBond = activeSubId
    ? bond.sub_tranches?.find(s => s.id === activeSubId) || bond
    : bond

  useEffect(() => {
    setWeatherLoading(true)
    setNewsLoading(true)
    setWeather(null)
    setNews(null)

    axios.get(`/api/weather/${bond.id}`)
      .then(r => setWeather(r.data))
      .catch(() => setWeather({ error: 'Failed to load weather' }))
      .finally(() => setWeatherLoading(false))

    axios.get(`/api/news/${bond.id}`)
      .then(r => setNews(r.data))
      .catch(() => setNews({ news: [], tweets: [], bloomberg: [] }))
      .finally(() => setNewsLoading(false))
  }, [bond.id])

  // Days to completion
  const daysToCompletion = () => {
    if (!bond.completion_date) return null
    const diff = new Date(bond.completion_date) - new Date()
    const days = Math.ceil(diff / (1000 * 60 * 60 * 24))
    return days > 0 ? days : null
  }
  const dtc = daysToCompletion()

  return (
    <div className="p-4 space-y-4 max-w-screen-2xl mx-auto">

      {/* Bond header */}
      <div className="card p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <h1 className="text-xl font-bold text-primary">{displayBond.name || bond.name}</h1>
              {displayBond.cusip && (
                <span className="font-mono text-xs text-muted bg-surface px-2 py-0.5 rounded border border-border">
                  {displayBond.cusip.trim()}
                </span>
              )}
              <span className={`badge ${RATING_BADGE(displayBond.ratings)}`}>
                {displayBond.ratings || 'NR'}
              </span>
              {bond.product && (
                <span className={`text-xs font-medium ${PRODUCT_COLOR(bond.product)}`}>
                  {bond.product}
                </span>
              )}
            </div>
            <div className="flex items-center gap-4 mt-1.5 flex-wrap">
              <div className="flex items-center gap-1 text-muted text-xs">
                <MapPin size={11} />
                <span>{bond.location_display}</span>
              </div>
              <div className="flex items-center gap-1 text-muted text-xs">
                <Building2 size={11} />
                <span>Tenant: <span className="text-primary font-medium">{bond.lease_counterparty || '—'}</span></span>
              </div>
              {bond.total_campus_mw && (
                <div className="flex items-center gap-1 text-muted text-xs">
                  <Zap size={11} />
                  <span><span className="text-primary font-medium">{bond.total_campus_mw} MW</span> campus</span>
                </div>
              )}
              {bond.completion_date && (
                <div className="flex items-center gap-1 text-muted text-xs">
                  <Calendar size={11} />
                  <span>Target: <span className={`font-medium ${dtc && dtc < 180 ? 'text-yellow' : 'text-primary'}`}>
                    {new Date(bond.completion_date).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}
                    {dtc && ` (${dtc.toLocaleString()} days)`}
                  </span></span>
                </div>
              )}
            </div>
          </div>
          <div className="text-right">
            <div className="text-xs text-muted">Tranche Size</div>
            <div className="font-mono text-lg font-bold text-gold">
              ${((displayBond.tranche_size_mm || bond.tranche_size_mm) / 1000).toFixed(1)}B
            </div>
          </div>
        </div>
      </div>

      {/* Key metrics strip */}
      <KeyMetrics bond={displayBond.id === bond.id ? bond : { ...bond, ...displayBond }} />

      {/* Three-column main layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left — Bond Details */}
        <div className="lg:col-span-1 space-y-4">
          <BondDetails bond={displayBond.id === bond.id ? bond : { ...bond, ...displayBond }} />
        </div>

        {/* Center — Weather */}
        <div className="lg:col-span-1">
          <WeatherWidget
            weather={weather}
            loading={weatherLoading}
            locationDisplay={bond.location_display}
          />
        </div>

        {/* Right — News */}
        <div className="lg:col-span-1">
          <NewsFeed news={news} loading={newsLoading} />
        </div>
      </div>
    </div>
  )
}
