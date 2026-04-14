import { useState } from 'react'
import { ChevronDown, LayoutDashboard } from 'lucide-react'

const RATING_COLOR = (r) => {
  if (!r) return 'text-muted'
  const s = r.toUpperCase()
  if (s.includes('AAA') || s.includes('AA') || s.includes('A+') || s.includes('BBB')) return 'text-green'
  if (s.includes('BB') || s.includes('BA')) return 'text-yellow'
  if (s.includes('B') || s.includes('CCC')) return 'text-red'
  return 'text-muted'
}

export default function BondNav({ bonds, activeBondId, activeSubId, onSelect, onHome }) {
  const [subOpen, setSubOpen] = useState(false)
  const relatedBx = bonds.find(b => b.id === 'related_bx')

  return (
    <div
      className="flex-none border-b border-border overflow-x-auto"
      style={{ background: '#161b22', scrollbarWidth: 'thin' }}
    >
      <div className="flex min-w-max">
        {/* Overview / landing page tab */}
        <button
          onClick={onHome}
          className={`flex items-center gap-1.5 px-3 py-2.5 text-xs font-medium border-b-2 border-r border-border/50 transition-colors whitespace-nowrap ${
            activeBondId === null
              ? 'border-blue text-blue bg-blue/10'
              : 'border-transparent text-muted hover:text-primary hover:bg-white/5'
          }`}
        >
          <LayoutDashboard size={11} />
          <span>Overview</span>
        </button>

        {bonds.map(bond => {
          const isRelated = bond.id === 'related_bx'
          const isActive = bond.id === activeBondId
          const hasSubTranches = isRelated && bond.sub_tranches?.length > 0

          return (
            <div key={bond.id} className="relative flex-none">
              <button
                onClick={() => {
                  if (isRelated) setSubOpen(v => !v)
                  onSelect(bond.id, null)
                }}
                className={`flex items-center gap-1.5 px-3 py-2.5 text-xs font-medium border-b-2 transition-colors whitespace-nowrap ${
                  isActive
                    ? 'border-blue text-blue bg-blue/10'
                    : 'border-transparent text-muted hover:text-primary hover:bg-white/5'
                }`}
              >
                <span className={RATING_COLOR(bond.ratings)}>●</span>
                <span>{bond.name}</span>
                {hasSubTranches && <ChevronDown size={10} className={subOpen && isActive ? 'rotate-180 transition-transform' : 'transition-transform'} />}
              </button>

              {/* Sub-tranche dropdown for Related/BX */}
              {isRelated && subOpen && isActive && hasSubTranches && (
                <div
                  className="absolute top-full left-0 z-50 w-56 card shadow-xl py-1"
                  style={{ background: '#1c2128' }}
                >
                  <button
                    onClick={() => { onSelect('related_bx', null); setSubOpen(false) }}
                    className={`w-full text-left px-3 py-2 text-xs hover:bg-white/5 ${!activeSubId ? 'text-blue' : 'text-primary'}`}
                  >
                    Main Tranche (7.500% 2042)
                  </button>
                  {bond.sub_tranches.map(sub => (
                    <button
                      key={sub.id}
                      onClick={() => { onSelect('related_bx', sub.id); setSubOpen(false) }}
                      className={`w-full text-left px-3 py-2 text-xs hover:bg-white/5 ${activeSubId === sub.id ? 'text-blue' : 'text-primary'}`}
                    >
                      {sub.name.split('—')[1]?.trim() || sub.name}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
