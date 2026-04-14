import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'

function Row({ label, value, highlight = false, mono = true }) {
  if (value == null || value === '' || value === 'null') return null
  return (
    <div className="flex justify-between items-start py-1 border-b border-border/40 gap-2">
      <span className="text-muted text-xs flex-shrink-0 w-40">{label}</span>
      <span className={`text-xs text-right flex-1 ${mono ? 'font-mono' : ''} ${highlight ? 'text-gold font-semibold' : 'text-primary'}`}>
        {String(value)}
      </span>
    </div>
  )
}

function Section({ title, children, defaultOpen = true }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="card p-3 mb-2">
      <button
        onClick={() => setOpen(v => !v)}
        className="flex items-center gap-1.5 w-full text-left"
      >
        {open ? <ChevronDown size={12} className="text-blue" /> : <ChevronRight size={12} className="text-blue" />}
        <span className="section-header">{title}</span>
      </button>
      {open && <div className="mt-2">{children}</div>}
    </div>
  )
}

const pct = (v) => v != null ? `${(v * 100).toFixed(1)}%` : null
const mm = (v) => v != null ? `$${Number(v).toLocaleString()}mm` : null
const bps = (v) => v != null ? `${Math.round(v)} bps` : null

export default function BondDetails({ bond }) {
  return (
    <div>
      <Section title="Security & Pricing">
        <Row label="CUSIP" value={bond.cusip?.trim()} />
        <Row label="Underlying CUSIP" value={bond.underlying_tenant_cusip?.trim()} />
        <Row label="Price" value={bond.price != null ? bond.price.toFixed(3) : null} highlight />
        <Row label="YTW" value={bond.ytw_pct != null ? `${bond.ytw_pct.toFixed(3)}%` : null} />
        <Row label="STW" value={bps(bond.stw_bps)} />
        <Row label="YTC" value={bond.ytc_pct != null ? `${bond.ytc_pct.toFixed(3)}%` : null} />
        <Row label="STC" value={bps(bond.stc_bps)} />
        <Row label="Coupon" value={bond.coupon} highlight />
        <Row label="Maturity" value={bond.maturity} />
        <Row label="WAL" value={bond.wal_yr != null ? `${bond.wal_yr} yr` : null} />
        <Row label="Ratings" value={bond.ratings} />
        <Row label="Call Protection" value={bond.call_protection != null ? `${bond.call_protection}${typeof bond.call_protection === 'number' ? ' yr NC' : ''}` : null} />
        <Row label="Call Price" value={bond.call_price} />
        <Row label="Market Cap" value={bond.market_cap} mono={false} />
        <Row label="Spread to Underlying" value={bps(bond.spread_to_underlying_bps)} />
        <Row label="CP Debt Yield" value={bond.counterparty_debt_yield_pct != null ? `${Number(bond.counterparty_debt_yield_pct).toFixed(2)}%` : null} />
        <Row label="Issuer Spread to CP" value={bps(bond.issuer_spread_to_cp_bps)} />
      </Section>

      <Section title="Lease & Counterparty">
        <Row label="Lease Tenant" value={bond.lease_counterparty} mono={false} highlight />
        <Row label="Tenant Rating" value={bond.counterparty_rating} />
        <Row label="DC Lease Tenor" value={bond.lease_tenor_yr != null ? `${bond.lease_tenor_yr} yr` : null} />
        <Row label="Ultimate Tenant" value={bond.ultimate_tenant} mono={false} />
        <Row label="Ultimate Tenor" value={bond.ultimate_tenant_tenor_yr != null ? `${bond.ultimate_tenant_tenor_yr} yr` : null} />
        <Row label="Guarantor" value={bond.guarantor} mono={false} />
        <Row label="Gty Trigger" value={bond.guarantee_trigger} mono={false} />
      </Section>

      <Section title="Campus & Technical">
        <Row label="Product Type" value={bond.product} mono={false} />
        <Row label="Total Campus MW" value={bond.total_campus_mw != null ? `${bond.total_campus_mw} MW` : null} />
        <Row label="Critical IT Load" value={bond.critical_it_mw != null ? `${bond.critical_it_mw} MW` : null} highlight />
        <Row label="IT Load %" value={bond.critical_it_pct != null ? `${bond.critical_it_pct}%` : null} />
        <Row label="PUE" value={bond.pue} />
        <Row label="Target Completion" value={bond.completion_date} highlight />
      </Section>

      <Section title="Economics">
        <Row label="Tranche Size" value={mm(bond.tranche_size_mm)} highlight />
        <Row label="Lease Agreement" value={mm(bond.lease_agreement_mm)} />
        <Row label="Contract Tenure" value={bond.contract_tenure_yr != null ? `${bond.contract_tenure_yr} yr` : null} />
        <Row label="$/MW/yr" value={bond.initial_per_mw_yr_mm != null ? `$${bond.initial_per_mw_yr_mm}mm` : null} />
        <Row label="Construction Cost" value={mm(bond.construction_cost_mm)} />
        <Row label="$/MW (Critical IT)" value={bond.per_mw_critical_mm != null ? `$${bond.per_mw_critical_mm}mm` : null} />
        <Row label="At-Cost / Debt" value={bond.at_cost_to_debt} highlight />
        <Row label="Yield on Cost" value={bond.yield_on_cost_pct != null ? `${bond.yield_on_cost_pct.toFixed(2)}%` : null} />
        <Row label="Annual Revenue" value={mm(bond.annual_revenue_mm)} />
        <Row label="NOI" value={mm(bond.noi_mm)} highlight />
        <Row label="NOI Margin" value={bond.noi_margin_pct != null ? `${bond.noi_margin_pct}%` : null} />
        <Row label="Implied Debt Yield" value={bond.implied_debt_yield_pct != null ? `${bond.implied_debt_yield_pct.toFixed(3)}%` : null} highlight />
      </Section>

      <Section title="Amortization & Cash Flow" defaultOpen={false}>
        <Row label="% Principal @ Mat." value={bond.pct_principal_at_maturity} />
        <Row label="Amort Schedule" value={bond.amort_schedule} mono={false} />
        <Row label="Amort % Tranche" value={bond.amort_pct_tranche} />
        <Row label="Amort Yr 1" value={bond.amort_yr1} />
        <Row label="Amort / NOI" value={bond.amort_as_pct_noi} />
        <Row label="ECF Sweep" value={bond.ecf_sweep} mono={false} />
        <Row label="ECF Annual" value={bond.ecf_sweep_annual} mono={false} />
        <Row label="Debt / NOI (Yr 1)" value={bond.debt_noi_yr1} highlight />
        <Row label="Debt / NOI (Mat.)" value={bond.debt_noi_maturity} />
      </Section>

      <Section title="Construction" defaultOpen={false}>
        <Row label="EPC" value={bond.epc} mono={false} />
        <Row label="EPC Market Cap" value={bond.epc_market_cap} mono={false} />
        <Row label="Subcontractors" value={bond.subcontractors} mono={false} />
        <Row label="Surety" value={bond.surety} mono={false} />
        <Row label="Power" value={bond.power_contingency} mono={false} />
        <Row label="Cost Overrun" value={bond.cost_overrun} mono={false} />
        <Row label="Construction Delay" value={bond.construction_delay != null ? String(bond.construction_delay) : null} mono={false} />
      </Section>
    </div>
  )
}
