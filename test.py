import azure.functions as func
import logging
import requests
from datetime import datetime, timezone

app = func.FunctionApp()

# ---------- helpers ----------

def fetch_irail(url: str) -> dict:
    headers = {
        "User-Agent": "bel-irail-research/0.1 (contact: you@example.com)",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()

# ---------- functions ----------

@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("OK", status_code=200)


@app.route(route="irail", methods=["GET"])
def irail(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP iRail request received")

    IRAIL_URL = (
        "https://api.irail.be/liveboard/"
        "?station=Brussels-Central&format=json"
    )

    try:
        data = fetch_irail(IRAIL_URL)
        departures = data.get("departures", {}).get("departure", [])

        return func.HttpResponse(
            f"Fetched {len(departures)} departures at "
            f"{datetime.now(timezone.utc).isoformat()}",
            status_code=200
        )

    except Exception:
        logging.exception("iRail fetch failed")
        return func.HttpResponse(
            "Failed to fetch iRail data",
            status_code=500
        )
