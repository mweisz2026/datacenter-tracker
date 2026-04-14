import { Cloud, Sun, CloudRain, CloudSnow, Wind, Droplets, Eye, Thermometer, AlertTriangle } from 'lucide-react'

const WEATHER_ICONS = {
  'Sunny': Sun,
  'Clear': Sun,
  'Mostly Sunny': Sun,
  'Partly Cloudy': Cloud,
  'Mostly Cloudy': Cloud,
  'Cloudy': Cloud,
  'Overcast': Cloud,
  'Rain': CloudRain,
  'Showers': CloudRain,
  'Thunderstorm': CloudRain,
  'Snow': CloudSnow,
  'Sleet': CloudSnow,
  'Windy': Wind,
}

function getIcon(desc) {
  if (!desc) return Cloud
  for (const [key, Icon] of Object.entries(WEATHER_ICONS)) {
    if (desc.toLowerCase().includes(key.toLowerCase())) return Icon
  }
  return Cloud
}

const ALERT_CLASS = (severity) => {
  const s = (severity || '').toLowerCase()
  if (s === 'extreme' || s === 'severe') return 'alert-severe'
  if (s === 'moderate') return 'alert-moderate'
  return 'alert-minor'
}

function ForecastDay({ period }) {
  const Icon = getIcon(period.short_forecast)
  const isDaytime = period.is_daytime
  return (
    <div className="flex items-center justify-between py-1 border-b border-border/30 last:border-0">
      <span className="text-xs text-muted w-20 truncate">{period.name}</span>
      <Icon size={13} className="text-muted mx-1 flex-shrink-0" />
      <span className="text-xs text-muted w-14 truncate text-center">{period.short_forecast}</span>
      <span className={`font-mono text-xs font-bold w-12 text-right ${isDaytime ? 'text-yellow' : 'text-blue'}`}>
        {period.temperature}°{period.temperature_unit}
      </span>
      {period.wind_speed && (
        <span className="text-xs text-muted w-14 text-right truncate">{period.wind_speed}</span>
      )}
      {period.precipitation_pct != null && (
        <span className="text-xs text-blue w-10 text-right">{Math.round(period.precipitation_pct)}%</span>
      )}
    </div>
  )
}

export default function WeatherWidget({ weather, loading, locationDisplay }) {
  if (loading) {
    return (
      <div className="card p-4 flex items-center justify-center h-40">
        <div className="spinner" />
      </div>
    )
  }

  if (!weather || weather.error) {
    return (
      <div className="card p-4">
        <div className="section-header">Weather — {locationDisplay}</div>
        <div className="text-muted text-xs mt-2">{weather?.error || 'No data available'}</div>
      </div>
    )
  }

  const { current, forecast, alerts, city, state } = weather
  const CurrentIcon = current ? getIcon(current.description) : Cloud

  return (
    <div className="space-y-2">
      {/* Current conditions */}
      <div className="card p-4">
        <div className="section-header">
          Current Conditions — {city || locationDisplay}{state ? `, ${state}` : ''}
        </div>

        {current ? (
          <div>
            <div className="flex items-end gap-3 mb-3">
              <div className="flex items-center gap-2">
                <CurrentIcon size={32} className="text-blue" />
                <div>
                  <div className="font-mono text-3xl font-bold text-primary">
                    {current.temperature_f != null ? `${current.temperature_f}°F` : '—'}
                  </div>
                  <div className="text-xs text-muted">{current.description || '—'}</div>
                </div>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div className="flex items-center gap-1.5">
                <Wind size={11} className="text-muted" />
                <span className="text-xs text-muted">Wind</span>
                <span className="font-mono text-xs text-primary ml-auto">
                  {current.wind_speed_mph != null ? `${current.wind_speed_mph} mph` : '—'}
                </span>
              </div>
              <div className="flex items-center gap-1.5">
                <Droplets size={11} className="text-muted" />
                <span className="text-xs text-muted">Humidity</span>
                <span className="font-mono text-xs text-primary ml-auto">
                  {current.humidity_pct != null ? `${current.humidity_pct}%` : '—'}
                </span>
              </div>
              <div className="flex items-center gap-1.5">
                <Thermometer size={11} className="text-muted" />
                <span className="text-xs text-muted">Dewpoint</span>
                <span className="font-mono text-xs text-primary ml-auto">
                  {current.dewpoint_f != null ? `${current.dewpoint_f}°F` : '—'}
                </span>
              </div>
              <div className="flex items-center gap-1.5">
                <Eye size={11} className="text-muted" />
                <span className="text-xs text-muted">Visibility</span>
                <span className="font-mono text-xs text-primary ml-auto">
                  {current.visibility_miles != null ? `${current.visibility_miles} mi` : '—'}
                </span>
              </div>
              {current.barometric_pressure_mb != null && (
                <div className="flex items-center gap-1.5 col-span-2">
                  <span className="text-xs text-muted">Pressure</span>
                  <span className="font-mono text-xs text-primary ml-auto">
                    {current.barometric_pressure_mb} mb
                  </span>
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="text-muted text-xs">Current observation unavailable</div>
        )}
      </div>

      {/* Active alerts */}
      {alerts && alerts.length > 0 && (
        <div className="card p-3">
          <div className="section-header text-red">Active Weather Alerts</div>
          <div className="space-y-2 mt-1">
            {alerts.map((a, i) => (
              <div key={i} className={`p-2 rounded text-xs ${ALERT_CLASS(a.severity)}`}>
                <div className="flex items-center gap-1.5 font-semibold text-primary">
                  <AlertTriangle size={11} />
                  {a.event}
                  <span className="ml-auto text-muted">{a.severity}</span>
                </div>
                {a.headline && <div className="text-muted mt-0.5">{a.headline}</div>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 7-day forecast */}
      {forecast && forecast.length > 0 && (
        <div className="card p-3">
          <div className="section-header">7-Day Forecast</div>
          <div className="mt-1">
            {forecast.map((p, i) => (
              <ForecastDay key={i} period={p} />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
