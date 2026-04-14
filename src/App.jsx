import { useState, useEffect } from 'react'
import axios from 'axios'
import BondNav from './components/BondNav'
import BondPage from './components/BondPage'
import LandingPage from './components/LandingPage'

const PRICES_REFRESH_MS = 2 * 60 * 1000  // 2 minutes — matches Bloomberg push cadence

export default function App() {
  const [bonds, setBonds] = useState([])
  const [livePrices, setLivePrices] = useState({})
  const [priceError, setPriceError] = useState(null)
  const [activeBondId, setActiveBondId] = useState(null)   // null = landing page
  const [activeSubId, setActiveSubId] = useState(null)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [loading, setLoading] = useState(true)

  // Load static bond data once
  useEffect(() => {
    axios.get('/api/bonds')
      .then(({ data }) => {
        setBonds(data.bonds)
        // Start on landing page (activeBondId stays null)
      })
      .catch(e => console.error('Failed to fetch bonds', e))
      .finally(() => setLoading(false))
  }, [])

  // Poll live prices from Excel every 2 minutes
  useEffect(() => {
    fetchPrices()
    const interval = setInterval(fetchPrices, PRICES_REFRESH_MS)
    return () => clearInterval(interval)
  }, [])

  async function fetchPrices() {
    try {
      const { data } = await axios.get('/api/prices')
      setLivePrices(data.prices || {})
      setPriceError(data.error || null)
      setLastUpdated(new Date())
    } catch (e) {
      console.error('Failed to fetch prices', e)
    }
  }

  // Merge live prices over static bond data
  const mergedBonds = bonds.map(bond => {
    const live = livePrices[bond.id]
    if (!live) return bond
    return { ...bond, ...live }
  })

  const activeBond = mergedBonds.find(b => b.id === activeBondId)

  return (
    <div className="flex flex-col h-screen overflow-hidden" style={{ background: '#0d1117' }}>
      {/* Top header bar */}
      <header className="flex-none flex items-center justify-between px-4 py-2 border-b border-border" style={{ background: '#0d1117' }}>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-green live-dot" />
            <span className="font-mono text-xs text-muted uppercase tracking-widest">Live</span>
          </div>
          <span className="text-gold font-bold text-lg tracking-tight">DC SENTINEL</span>
          <span className="text-muted text-xs hidden sm:block">· Datacenter Bond Intelligence</span>
        </div>
        <div className="flex items-center gap-4 text-xs text-muted font-mono">
          {priceError && (
            <span className="text-yellow text-xs truncate max-w-xs" title={priceError}>
              ⚠ {priceError.slice(0, 60)}
            </span>
          )}
          {lastUpdated && (
            <span>Prices: {lastUpdated.toLocaleTimeString()}</span>
          )}
          <span className="badge badge-hy">{bonds.length} BONDS</span>
        </div>
      </header>

      {/* Bond navigation tabs */}
      {!loading && (
        <BondNav
          bonds={mergedBonds}
          activeBondId={activeBondId}
          activeSubId={activeSubId}
          onSelect={(bondId, subId) => {
            setActiveBondId(bondId)
            setActiveSubId(subId || null)
          }}
          onHome={() => { setActiveBondId(null); setActiveSubId(null) }}
        />
      )}

      {/* Main content */}
      <main className="flex-1 overflow-y-auto" style={{ background: '#0d1117' }}>
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <div className="spinner" />
          </div>
        ) : activeBondId === null ? (
          <LandingPage
            bonds={mergedBonds}
            onSelectBond={(bondId) => { setActiveBondId(bondId); setActiveSubId(null) }}
          />
        ) : activeBond ? (
          <BondPage bond={activeBond} activeSubId={activeSubId} />
        ) : (
          <div className="flex items-center justify-center h-64 text-muted">
            No bonds loaded
          </div>
        )}
      </main>
    </div>
  )
}
