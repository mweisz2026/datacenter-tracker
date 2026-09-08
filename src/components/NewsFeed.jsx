import { useState } from 'react'
import { ExternalLink, MessageSquare, Newspaper, AlertTriangle, AlertCircle } from 'lucide-react'

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

function NewsItem({ item }) {
  const isReddit = item.type === 'reddit'

  return (
    <a
      href={item.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-2 rounded mb-1 cursor-pointer transition-colors ${isReddit ? 'tweet-item' : 'news-item'}`}
    >
      <div className="flex items-start gap-1.5">
        <div className="flex-shrink-0 mt-0.5">
          {isReddit
            ? <MessageSquare size={11} className="text-blue" />
            : <Newspaper size={11} className="text-muted" />
          }
        </div>
        <div className="flex-1 min-w-0">
          <div className="text-xs text-primary leading-snug line-clamp-2 font-medium">
            {item.title}
          </div>
          {item.summary && !isReddit && (
            <div className="text-xs text-muted mt-0.5 line-clamp-2 leading-snug">
              {item.summary}
            </div>
          )}
          <div className="flex items-center gap-2 mt-1">
            <span className="text-xs text-muted truncate">{item.source}</span>
            {item.published && <span className="text-xs text-muted ml-auto flex-shrink-0">{formatDate(item.published)}</span>}
            <ExternalLink size={9} className="text-muted flex-shrink-0" />
          </div>
        </div>
      </div>
    </a>
  )
}

const TABS = ['All', 'News', 'Social', 'Industry']

const ALERT_STYLE = {
  CRITICAL: {
    bg:     'bg-red/10 border border-red/30',
    text:   'text-red',
    badge:  'bg-red/20 text-red',
    Icon:   AlertTriangle,
  },
  HIGH: {
    bg:     'bg-yellow/10 border border-yellow/30',
    text:   'text-yellow',
    badge:  'bg-yellow/20 text-yellow',
    Icon:   AlertCircle,
  },
}

function AlertItem({ item }) {
  const cat   = item.importance_category || 'HIGH'
  const style = ALERT_STYLE[cat] || ALERT_STYLE.HIGH
  const Icon  = style.Icon

  return (
    <a
      href={item.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-2.5 rounded mb-2 cursor-pointer transition-opacity hover:opacity-90 ${style.bg}`}
    >
      <div className="flex items-start gap-2">
        <Icon size={13} className={`flex-shrink-0 mt-0.5 ${style.text}`} />
        <div className="flex-1 min-w-0">
          <div className={`text-xs font-semibold leading-snug line-clamp-2 ${style.text}`}>
            {item.title}
          </div>
          {(item.importance_blurb || item.importance_reason) && (
            <div className="text-xs text-muted mt-0.5 italic line-clamp-3">
              {item.importance_blurb || item.importance_reason}
            </div>
          )}
          <div className="flex items-center gap-2 mt-1">
            <span className={`text-xs px-1.5 py-0.5 rounded font-mono font-bold ${style.badge}`}>
              {cat}
            </span>
            {typeof item.importance_score === 'number' && (
              <span className="text-xs px-1.5 py-0.5 rounded font-mono font-bold bg-white/10 text-primary" title="Relevance score (0-10)">
                {item.importance_score}/10
              </span>
            )}
            <span className="text-xs text-muted truncate">{item.source}</span>
            <span className="text-xs text-muted ml-auto flex-shrink-0">{formatDate(item.published)}</span>
            <ExternalLink size={9} className="text-muted flex-shrink-0" />
          </div>
        </div>
      </div>
    </a>
  )
}

export default function NewsFeed({ news, loading }) {
  const [tab, setTab] = useState('All')

  if (loading) {
    return (
      <div className="card p-4 flex items-center justify-center h-40">
        <div className="spinner" />
      </div>
    )
  }

  if (!news) {
    return <div className="card p-4 text-muted text-xs">No news available</div>
  }

  const alertItems    = news.alerts   || []
  const socialItems   = news.social   || []
  const newsOnlyItems = news.news     || []
  const industryItems = news.industry || []

  const allItems = [...newsOnlyItems, ...socialItems]
    .sort((a, b) => new Date(b.published) - new Date(a.published))

  let displayItems = []
  if (tab === 'All')      displayItems = allItems
  else if (tab === 'News')     displayItems = newsOnlyItems
  else if (tab === 'Social')   displayItems = socialItems
  else if (tab === 'Industry') displayItems = industryItems

  const counts = {
    All:      allItems.length,
    News:     newsOnlyItems.length,
    Social:   socialItems.length,
    Industry: industryItems.length,
  }

  return (
    <div className="card p-3 h-full flex flex-col">
      <div className="section-header mb-2">News & Social Feed</div>

      {/* Alerts — surfaced by Claude relevance scorer */}
      {alertItems.length > 0 && (
        <div className="mb-3">
          <div className="label mb-2 flex items-center gap-1.5">
            <AlertTriangle size={10} className="text-red" />
            Material Alerts ({alertItems.length})
          </div>
          {alertItems.map((item, i) => <AlertItem key={i} item={item} />)}
        </div>
      )}

      {/* Tab bar */}
      <div className="flex gap-1 mb-3 border-b border-border pb-2">
        {TABS.map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`text-xs px-2 py-1 rounded transition-colors ${
              tab === t
                ? 'bg-blue/20 text-blue'
                : 'text-muted hover:text-primary'
            }`}
          >
            {t}
            {counts[t] > 0 && (
              <span className="ml-1 font-mono">{counts[t]}</span>
            )}
          </button>
        ))}
      </div>

      {/* Feed */}
      <div className="flex-1 overflow-y-auto" style={{ maxHeight: '600px' }}>
        {displayItems.length === 0 ? (
          <div className="text-muted text-xs text-center py-6">
            {tab === 'Social'
              ? 'No recent Reddit discussions found for this project'
              : 'No items found for this bond'}
          </div>
        ) : (
          displayItems.map((item, i) => <NewsItem key={i} item={item} />)
        )}
      </div>
    </div>
  )
}
