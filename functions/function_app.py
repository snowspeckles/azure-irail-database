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
        
@app.route(route="load-stations-rollback", methods=["POST"])
def load_stations_rollback(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("TEST MODE: load-stations (rollback only)")

    MAX_STATIONS = 10

    # fetch iRail stations
    IRAIL_URL = "https://api.irail.be/stations/?format=json&lang=en"
    try:
        data = fetch_irail(IRAIL_URL)
        stations = data.get("station", [])[:MAX_STATIONS]
    except Exception:
        logging.exception("Failed to fetch stations from iRail")
        return func.HttpResponse("Failed to fetch stations", status_code=500)

    # connect to Azure SQL
    try:
        conn = pyodbc.connect(
            os.environ["SQL_CONNECTION_STRING_TEST"],
            autocommit=False
            )
        conn.timeout = 5
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
    except Exception:
        logging.exception("Failed to connect to Azure SQL")
        return func.HttpResponse("Database connection failed", status_code=500)

    attempted = 0
    
    # insert test rows
    try:
        for s in stations:
            cursor.execute("""
                INSERT INTO [dbo].[StationsRollback] (station_id, name, latitude, longitude)
                VALUES (?, ?, ?, ?)
            """,
            s["id"],
            s["name"],
            s.get("locationY"),
            s.get("locationX")
            )
            attempted += 1
        
        # verify before rollback
        cursor.execute("SELECT COUNT(*) FROM [dbo].[StationsRollback]")
        total_in_db = cursor.fetchone()[0] # pyright: ignore[reportOptionalSubscript]
        logging.info(f"Stations processed: {attempted}, Total in DB: {total_in_db}")

        # always rollback in test mode
        conn.rollback()
        logging.info("Transaction rolled back intentionally")

    except Exception:
        logging.exception("Insert failed — rolling back")
        conn.rollback()
        return func.HttpResponse("Insert failed (rolled back)", status_code=500)

    finally:
        cursor.close()
        conn.close()

    return func.HttpResponse(
        f"TEST SUCCESS: attempted inserts = {attempted}, committed = 0",
        status_code=200
    )

@app.route(route="load-stations", methods=["POST"])
def load_stations(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("START INGESTION: load-stations")

    STA_URL = "https://api.irail.be/stations/?format=json&lang=en"
    try:
        data = fetch_irail(STA_URL)
        stations = data.get("station", [])
        logging.info(f"Fetched {len(stations)} stations from iRail API")     
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
        
        # cursor.execute("BEGIN TRANSACTION")
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
        
        # verify before commit
        cursor.execute("SELECT COUNT(*) FROM [dbo].[Stations]")
        total_in_db = cursor.fetchone()[0] # pyright: ignore[reportOptionalSubscript]
        logging.info(f"Stations processed: {upserted}, Total in DB: {total_in_db}")

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
    
@app.route(route="load-departures", methods=["POST"])
def load_departures(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("START INGESTION: load-departures")

    # connect to DB
    try:
        conn = pyodbc.connect(
            os.environ["SQL_CONNECTION_STRING"],
            autocommit=False
        )
        cursor = conn.cursor()
    except Exception:
        logging.exception("Failed to connect to Azure SQL")
        return func.HttpResponse("Database connection failed", status_code=500)
    
    # station id must be provided for the url
    try:
        cursor.execute("""
            SELECT station_id
            FROM Stations
        """)
        stations = [row[0] for row in cursor.fetchall()]
        logging.info(f"Fetched {len(stations)} stations from DB")
    except Exception:
        logging.exception("Failed to fetch stations from DB")
        return func.HttpResponse("Failed to fetch stations", status_code=500)

    upserted = 0
    vehicle_cache: dict[str, str | None] = {}
    
    try:
        for station_id in stations:
            
            DEP_URL = (
                "https://api.irail.be/liveboard/"
                f"?id={station_id}&arrdep=departure&format=json&lang=en"
                )
            
            try:
                data = fetch_irail(DEP_URL)
                departure = (data.get("departures", {}).get("departure", []))
                logging.info(f"{station_id}: {len(departure)} departures")
            except Exception:
                logging.exception("Failed to fetch departures from iRail")
                return func.HttpResponse("Failed to fetch departures", status_code=500)
            
            
            for d in departure:
                # --- parse time safely ---
                scheduled_epoch = int(d["time"])
                scheduled_time_utc = datetime.fromtimestamp(
                    scheduled_epoch,
                    tz=timezone.utc
                    )
            
                # --- enrich destination via vehicle API ---
                vehicle_id = d.get("vehicle")
                destination_id = None
                if vehicle_id:
                    if vehicle_id in vehicle_cache:
                        destination_id = vehicle_cache[vehicle_id]
                    else:
                        try:
                            VEH_URL = (
                                "https://api.irail.be/vehicle/"
                                f"?id={vehicle_id}&format=json&lang=en"
                                )
                            vehicle_data = fetch_irail(VEH_URL)

                            stops = (vehicle_data.get("stops", {}).get("stop", []))

                            if stops:
                                final_station_id = stops[-1].get("station")
                                
                                # make sure that station coalescence is ingested correctly
                                if final_station_id:
                                    cursor.execute(
                                        "SELECT 1 FROM Stations WHERE station_id = ?",
                                        final_station_id
                                        )
                                    if cursor.fetchone():
                                        destination_id = final_station_id
                                    else:
                                        # log missing stations
                                        cursor.execute("""
                                            MERGE [dbo].[MissingStations] AS target
                                            USING (VALUES (?)) AS source (station_id)
                                            ON target.station_id = source.station_id
                                            WHEN MATCHED THEN
                                                UPDATE SET
                                                    last_seen = SYSUTCDATETIME(),
                                                    seen_count = target.seen_count + 1
                                            WHEN NOT MATCHED THEN
                                                INSERT (station_id)
                                                VALUES (source.station_id);
                                        """, final_station_id)
                                        destination_id = None
                            vehicle_cache[vehicle_id] = destination_id
                        except Exception:
                            logging.warning(
                                f"Vehicle enrichment failed for {vehicle_id}",
                                exc_info=True)
                            vehicle_cache[vehicle_id] = None       

                # --- MERGE departure ---
                cursor.execute("""
                    MERGE [dbo].[TrainDepartures] AS target
                    USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
                        departure_external_id,
                        scheduled_time,
                        delay_seconds,
                        is_cancelled,
                        origin_id,
                        destination_id,
                        platform_planned,
                        platform_actual,
                        vehicle_id,
                        collected_at
                    )
                    ON  target.departure_external_id = source.departure_external_id
                    AND target.scheduled_time = source.scheduled_time
                    AND target.origin_id = source.origin_id

                    WHEN MATCHED THEN
                        UPDATE SET
                            delay_seconds = source.delay_seconds,
                            is_cancelled = source.is_cancelled,
                            destination_id = COALESCE(target.destination_id,source.destination_id),
                            platform_planned = source.platform_planned,
                            platform_actual = source.platform_actual,
                            vehicle_id = source.vehicle_id,
                            collected_at = source.collected_at

                    WHEN NOT MATCHED THEN
                        INSERT (
                            departure_external_id,
                            scheduled_time,
                            delay_seconds,
                            is_cancelled,
                            origin_id,
                            destination_id,
                            platform_planned,
                            platform_actual,
                            vehicle_id,
                            collected_at
                        )
                        VALUES (
                            source.departure_external_id,
                            source.scheduled_time,
                            source.delay_seconds,
                            source.is_cancelled,
                            source.origin_id,
                            source.destination_id,
                            source.platform_planned,
                            source.platform_actual,
                            source.vehicle_id,
                            source.collected_at
                        );
                """,
                d["id"],
                scheduled_time_utc,
                int(d.get("delay", 0)),
                1 if d.get("canceled") == "1" else 0,
                station_id,
                destination_id,
                d.get("platform", {}),
                d.get("platforminfo", {}).get("name"),
                vehicle_id,
                datetime.now(timezone.utc)
                )

                upserted += 1
        
        # verify before commit
        cursor.execute("SELECT COUNT(*) FROM [dbo].[TrainDepartures]")
        total_in_db = cursor.fetchone()[0] # pyright: ignore[reportOptionalSubscript]
        logging.info(f"Departures processed: {upserted}, Total in DB: {total_in_db}")

        conn.commit()
        logging.info(f"Departures upserted: {upserted}")

    except Exception:
        logging.exception("Departure ingestion failed — rolling back")
        conn.rollback()
        return func.HttpResponse("Departure ingestion failed", status_code=500)
    
    finally:
        cursor.close()
        conn.close()

    return func.HttpResponse(
        f"SUCCESS: departures upserted = {upserted}",
        status_code=200
    )

