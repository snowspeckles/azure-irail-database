import azure.functions as func
import os
import time
import pyodbc
import logging
import requests
import random
from datetime import datetime, timezone

app = func.FunctionApp()

# ---------- helpers ----------

def fetch_irail(url: str) -> dict:
    
    # self-enforced rate limit before the request
    LAST_REQUEST_TIME = 0
    MIN_INTERVAL = 1 / 3  #3 requests per second
    
    now = time.monotonic()
    elapsed = now - LAST_REQUEST_TIME
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
        
    # small jitters before request
    time.sleep(random.uniform(0.01, 0.05))

    headers = {
        "User-Agent": "bel-irail-research/0.1 (https://github.com/intanwardhani; ijuqd02go@mozmail.com)",
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers, timeout=15)
    
    # back off after request, if get 429 error (exceeding request limit)
    if response.status_code == 429:
        # respect retry-after if provided
        retry_after = response.headers.get("Retry-After")
        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 5
        time.sleep(wait_time)
        # try once more (polite retry)
        response = requests.get(url, headers=headers, timeout=15)
        # if retry is not allowed, just cry loudly
    
    response.raise_for_status()
    LAST_REQUEST_TIME = time.monotonic()
    
    return response.json()

# ---------- functions ----------

@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("OK", status_code=200)


@app.route(route="irail", methods=["GET"])
def irail(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP iRail request received")

    LIVEBOARD_URL = (
        "https://api.irail.be/liveboard/"
        "?station=Gent-Sint-Pieters&arrdep=departure&lang=en&format=json&alerts=false"
    )
    
    conn_str = os.environ.get("SQL_CONNECTION_STRING")
    logging.info(f"Conn string present: {bool(conn_str)}")


    try:
        data = fetch_irail(LIVEBOARD_URL)
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
        
# @app.route(route="load-stations-rollback", methods=["POST"])
# def load_stations_rollback(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info("TEST MODE: load-stations (rollback only)")

#     MAX_STATIONS = 10

#     # fetch iRail stations
#     IRAIL_URL = "https://api.irail.be/stations/?format=json&lang=en"
#     try:
#         data = fetch_irail(IRAIL_URL)
#         stations = data.get("station", [])[:MAX_STATIONS]
#     except Exception:
#         logging.exception("Failed to fetch stations from iRail")
#         return func.HttpResponse("Failed to fetch stations", status_code=500)

#     # connect to Azure SQL
#     try:
#         conn = pyodbc.connect(
#             os.environ["SQL_CONNECTION_STRING_TEST"],
#             autocommit=False
#             )
#         conn.timeout = 5
#         cursor = conn.cursor()
#         cursor.execute("BEGIN TRANSACTION")
#     except Exception:
#         logging.exception("Failed to connect to Azure SQL")
#         return func.HttpResponse("Database connection failed", status_code=500)

#     attempted = 0
    
#     # insert test rows
#     try:
#         for s in stations:
#             cursor.execute("""
#                 INSERT INTO dbo.StationsRollback (station_id, name, latitude, longitude)
#                 VALUES (?, ?, ?, ?)
#             """,
#             s["id"],
#             s["name"],
#             s.get("locationY"),
#             s.get("locationX")
#             )
#             attempted += 1

#         # always rollback in test mode
#         conn.rollback()
#         logging.info("Transaction rolled back intentionally")

#     except Exception:
#         logging.exception("Insert failed — rolling back")
#         conn.rollback()
#         return func.HttpResponse("Insert failed (rolled back)", status_code=500)

#     finally:
#         cursor.close()
#         conn.close()

#     return func.HttpResponse(
#         f"TEST SUCCESS: attempted inserts = {attempted}, committed = 0",
#         status_code=200
#     )

@app.route(route="load-stations", methods=["POST"])
def load_stations(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("START INGESTION: load-stations")

    # MAX_STATIONS = None

    # fetch iRail stations
    STA_URL = "https://api.irail.be/stations/?format=json&lang=en"
    try:
        data = fetch_irail(STA_URL)
        stations = data.get("station", [])
        # if MAX_STATIONS:
        #     stations = stations[:MAX_STATIONS]
    except Exception:
        logging.exception("Failed to fetch stations from iRail")
        return func.HttpResponse("Failed to fetch stations", status_code=500)

    # connect to DB
    try:
        conn = pyodbc.connect(
            os.environ["SQL_CONNECTION_STRING"],
            autocommit=False
        )
        conn.timeout = 10
        cursor = conn.cursor()
        
        ### debug ###
        cursor.execute("SELECT DB_NAME() AS db_name, @@SERVERNAME AS server_name")
        row = cursor.fetchone()
        logging.info(f"Connected to DB: {row.db_name}, Server: {row.server_name}") # pyright: ignore[reportOptionalMemberAccess]
        ### end debug ###
        
        cursor.execute("BEGIN TRANSACTION")
        
    except Exception:
        logging.exception("Failed to connect to Azure SQL")
        return func.HttpResponse("Database connection failed", status_code=500)

    upserted = 0
    try:
        for s in stations:
            cursor.execute("""
                MERGE [dbo].[Stations] AS target
                USING (VALUES (?, ?, ?, ?)) AS source (
                    station_id,
                    name,
                    longitude,
                    latitude
                )
                ON target.station_id = source.station_id
                WHEN MATCHED THEN
                    UPDATE SET
                        name = source.name,
                        longitude = source.longitude,
                        latitude = source.latitude,
                        collected_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (station_id, name, longitude, latitude)
                    VALUES (
                        source.station_id,
                        source.name,
                        source.longitude,
                        source.latitude
                    );
            """,
            s["id"],
            s["name"],
            s.get("locationX"),
            s.get("locationY")
            )
            upserted += 1

        conn.commit()
        logging.info(f"Stations upserted: {upserted}")

    except Exception:
        logging.exception("Station ingestion failed — rolling back")
        conn.rollback()
        return func.HttpResponse("Station ingestion failed", status_code=500)

    finally:
        cursor.close()
        conn.close()

    return func.HttpResponse(
        f"SUCCESS: stations upserted = {upserted}",
        status_code=200
    )
    
# @app.route(route="load-departures", methods=["POST"])
# def load_departures(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info("START INGESTION: load-departures")

#     # fetch iRail stations
#     DEP_URL = "https://api.irail.be/liveboard/?format=json&lang=en"
#     try:
#         data = fetch_irail(DEP_URL)
#         departures_block = data.get("departures", {})
#         departures = departures_block.get("departure", [])
#     except Exception:
#         logging.exception("Failed to fetch departures from iRail")
#         return func.HttpResponse("Failed to fetch departures", status_code=500)

#     # connect to DB
#     try:
#         conn = pyodbc.connect(
#             os.environ["SQL_CONNECTION_STRING"],
#             autocommit=False
#         )
#         conn.timeout = 10
#         cursor = conn.cursor()
        
#         ### debug ###
#         cursor.execute("SELECT DB_NAME() AS db_name, @@SERVERNAME AS server_name")
#         row = cursor.fetchone()
#         logging.info(f"Connected to DB: {row.db_name}, Server: {row.server_name}") # pyright: ignore[reportOptionalMemberAccess]
#         ### end debug ###
        
#         cursor.execute("BEGIN TRANSACTION")
        
#     except Exception:
#         logging.exception("Failed to connect to Azure SQL")
#         return func.HttpResponse("Database connection failed", status_code=500)

#     upserted = 0
#     try:
#         for d in departures:
#             cursor.execute("""
#                 MERGE dbo.TrainDepartures AS target
#                 USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
#                     departure_id,
#                     scheduled_time,
#                     delay_seconds,
#                     is_cancelled,
#                     origin_id,
#                     destination_id,
#                     platform_planned,
#                     platform_actual,
#                     vehicle_id,
#                     collected_at
#                 )
#                 ON target.departure_id = source.departure_id
#                 WHEN MATCHED THEN
#                     UPDATE SET
#                         scheduled_time = source.scheduled_time,
#                         delay_seconds = source.delay_seconds,
#                         is_cancelled = source.is_cancelled,
#                         origin_id = source.origin_id,
#                         destination_id = source.destination_id,
#                         platform_planned = source.platform_planned,
#                         platform_actual = source.platform_actual,
#                         vehicle_id = source.vehicle_id,
#                         collected_at = SYSUTCDATETIME()
#                 WHEN NOT MATCHED THEN
#                     INSERT (station_id, name, longitude, latitude)
#                     VALUES (
#                         source.departure_id,
#                         source.scheduled_time,
#                         source.delay_seconds,
#                         source.is_cancelled,
#                         source.origin_id,
#                         source.destation_id,
#                         source.platform_planned,
#                         source.platform_actual,
#                         source.vehicle_id
#                     );
#             """,
#             d["id"],
#             s["time"],
#             s.get("locationX"),
#             s.get("locationY")
#             )
#             upserted += 1

#         conn.commit()
#         logging.info(f"Stations upserted: {upserted}")

#     except Exception:
#         logging.exception("Station ingestion failed — rolling back")
#         conn.rollback()
#         return func.HttpResponse("Station ingestion failed", status_code=500)

#     finally:
#         cursor.close()
#         conn.close()

#     return func.HttpResponse(
#         f"SUCCESS: stations upserted = {upserted}",
#         status_code=200
#     )
