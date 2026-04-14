import httpx
import asyncio

HEADERS = {"User-Agent": "datacenter-tracker/1.0 (contact@diameter.com)"}


def _c_to_f(c):
    return round(c * 9 / 5 + 32, 1) if c is not None else None


def _ms_to_mph(ms):
    return round(ms * 2.237, 1) if ms is not None else None


def _m_to_miles(m):
    return round(m / 1609.344, 1) if m is not None else None


async def get_weather(lat: float, lon: float) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # 1. Resolve grid point
            r = await client.get(f"https://api.weather.gov/points/{lat},{lon}", headers=HEADERS)
            r.raise_for_status()
            props = r.json()["properties"]
            forecast_url = props["forecast"]
            stations_url = props["observationStations"]
            city = props["relativeLocation"]["properties"]["city"]
            state = props["relativeLocation"]["properties"]["state"]

            # 2. Forecast + stations in parallel
            forecast_task = client.get(forecast_url, headers=HEADERS)
            stations_task = client.get(f"{stations_url}?limit=1", headers=HEADERS)
            alerts_task = client.get(
                f"https://api.weather.gov/alerts/active?point={lat},{lon}", headers=HEADERS
            )
            forecast_r, stations_r, alerts_r = await asyncio.gather(
                forecast_task, stations_task, alerts_task
            )

            # 3. Current conditions from nearest station
            current = None
            try:
                station_id = stations_r.json()["features"][0]["properties"]["stationIdentifier"]
                obs_r = await client.get(
                    f"https://api.weather.gov/stations/{station_id}/observations/latest",
                    headers=HEADERS,
                )
                o = obs_r.json()["properties"]
                current = {
                    "temperature_f": _c_to_f(o["temperature"]["value"]),
                    "description": o.get("textDescription", ""),
                    "wind_speed_mph": _ms_to_mph(o["windSpeed"]["value"]),
                    "wind_direction_deg": o["windDirection"]["value"],
                    "humidity_pct": round(o["relativeHumidity"]["value"], 1)
                    if o["relativeHumidity"]["value"] is not None
                    else None,
                    "dewpoint_f": _c_to_f(o["dewpoint"]["value"]),
                    "visibility_miles": _m_to_miles(o["visibility"]["value"]),
                    "barometric_pressure_mb": round(o["barometricPressure"]["value"] / 100, 1)
                    if o["barometricPressure"]["value"] is not None
                    else None,
                    "timestamp": o["timestamp"],
                    "station": station_id,
                }
            except Exception:
                pass

            # 4. 7-day forecast periods
            forecast_periods = []
            try:
                periods = forecast_r.json()["properties"]["periods"][:14]
                for p in periods:
                    forecast_periods.append(
                        {
                            "name": p["name"],
                            "temperature": p["temperature"],
                            "temperature_unit": p["temperatureUnit"],
                            "wind_speed": p["windSpeed"],
                            "wind_direction": p["windDirection"],
                            "short_forecast": p["shortForecast"],
                            "detailed_forecast": p["detailedForecast"],
                            "is_daytime": p["isDaytime"],
                            "icon": p["icon"],
                            "precipitation_pct": p.get("probabilityOfPrecipitation", {}).get("value"),
                        }
                    )
            except Exception:
                pass

            # 5. Active alerts
            alerts = []
            try:
                for feat in alerts_r.json().get("features", [])[:5]:
                    ap = feat["properties"]
                    alerts.append(
                        {
                            "event": ap.get("event", ""),
                            "severity": ap.get("severity", ""),
                            "urgency": ap.get("urgency", ""),
                            "headline": ap.get("headline", ""),
                            "description": (ap.get("description") or "")[:300],
                            "expires": ap.get("expires", ""),
                        }
                    )
            except Exception:
                pass

            return {
                "city": city,
                "state": state,
                "current": current,
                "forecast": forecast_periods,
                "alerts": alerts,
                "error": None,
            }

    except Exception as e:
        return {"current": None, "forecast": [], "alerts": [], "error": str(e)}
