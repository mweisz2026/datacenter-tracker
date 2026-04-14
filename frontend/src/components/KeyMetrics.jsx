const fmt = (v, digits = 3) => v != null ? Number(v).toFixed(digits) : '—'
const fmtBps = (v) => v != null ? `+${Math.round(v)}` : '—'

function Metric({ label, value, unit = '', color = 'text-primary', sub = null }) {
  return (
    <div className="card p-3 flex flex-col gap-0.5">
      <div className="label">{label}</div>
      <div className={`font-mono text-base font-bold ${color}`}>
        {value}<span className="text-xs font-normal text-muted ml-0.5">{unit}</span>
      </div>
      {sub && <div className="text-xs text-muted font-mono">{sub}</div>}
    </div>
  )
}

export default function KeyMetrics({ bond }) {
  const couponDisplay = typeof bond.coupon === 'number'
    ? `${(bond.coupon * 100).toFixed(3)}%`
    : bond.coupon || '—'

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-2">
      <Metric
        label="Price"
        value={bond.price != null ? fmt(bond.price, 3) : '—'}
        color="text-primary"
        sub={bond.price != null ? (bond.price > 100 ? 'premium' : bond.price < 100 ? 'discount' : 'par') : null}
      />
      <Metric
        label="YTW"
        value={bond.ytw_pct != null ? fmt(bond.ytw_pct, 3) : '—'}
        unit="%"
        color="text-blue"
      />
      <Metric
        label="STW"
        value={fmtBps(bond.stw_bps)}
        unit=" bps"
        color="text-blue"
      />
      <Metric
        label="YTC"
        value={bond.ytc_pct != null ? fmt(bond.ytc_pct, 3) : '—'}
        unit="%"
        color="text-purple"
      />
      <Metric
        label="STC"
        value={fmtBps(bond.stc_bps)}
        unit=" bps"
        color="text-purple"
      />
      <Metric
        label="Coupon"
        value={couponDisplay}
        color="text-gold"
      />
      <Metric
        label="Debt Yield"
        value={bond.implied_debt_yield_pct != null ? fmt(bond.implied_debt_yield_pct, 2) : '—'}
        unit="%"
        color={bond.implied_debt_yield_pct > 12 ? 'text-green' : 'text-primary'}
        sub="Implied"
      />
      <Metric
        label="NOI Margin"
        value={bond.noi_margin_pct != null ? `${bond.noi_margin_pct}` : '—'}
        unit="%"
        color={bond.noi_margin_pct >= 90 ? 'text-green' : bond.noi_margin_pct >= 70 ? 'text-yellow' : 'text-red'}
      />
    </div>
  )
}
