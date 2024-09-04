

# -*- coding: UTF-8 -*-

import re
import csv
import sys
import json
import time
import random
import logging
import os
from datetime import datetime, timezone, timedelta
import pytz
import requests
from pyfiglet import Figlet





# Basic logger configuration
logging.basicConfig(level=logging.DEBUG, format='<%(asctime)s %(levelname)s> %(message)s')
logging.addLevelName(logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
logging.addLevelName(logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
logging.info("=====> START %s <=====", datetime.now())
HEADLINE_FONT = 'drpepper'


DATA_PATH = 'public/data/'
BASE_URL = 'https://www.umweltbundesamt.de/api/air_data/v3/airquality/'

def big_debug_text(text):
    """ Write some fancy big text into log-output """
    custom_fig = Figlet(font=HEADLINE_FONT, width=120)
    logging.info("\n\n%s", custom_fig.renderText(text))


def readUrlWithCache(url):
    """ repeated runs of this script will use the same cache file for 1 month """

    filename = 'cache/{}'.format(re.sub("[^0-9a-zA-Z]+", "_", url.replace(BASE_URL, "")))

    currentTS = time.time()
    cacheMaxAge = 60 * 60 * 24 * 30
    generateCacheFile = True
    filecontent = "{}"
    if os.path.isfile(filename):
        fileModTime = os.path.getmtime(filename)
        timeDiff = currentTS - fileModTime
        if timeDiff > cacheMaxAge:
            logging.debug("# CACHE file age %s too old: %s", timeDiff, filename)
        else:
            generateCacheFile = False
            logging.debug("(using cached file instead of url get)")
            with open(filename) as myfile:
                filecontent = "".join(line.rstrip() for line in myfile)

    if generateCacheFile:
        logging.debug("# URL HTTP GET %s ", filename)
        req = requests.get(url, timeout=120)
        if req.status_code > 399:
            logging.warning('  - Request result: HTTP %s - %s', req.status_code, url)

        open(filename, 'wb').write(req.content)
        filecontent = req.text
        # lets wat a bit, dont kill a public server
        time.sleep(1)

    jsn = json.loads(filecontent)
    if jsn.get('status') == 404:
        logging.warning('  - missing url: %s', url)
        return json.loads("{}")
    return jsn


def write_json_file(data, outfile_name):
    """ Create and write output format: GeoJSON """

    big_debug_text("..write GeoJSON")

    features = []
    for entry in data:

        features.append(
          {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": entry[0]
            },
            "properties": entry[1]
          }
        )

    logging.info('Got %s Stationen', len(features))

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    logging.info( "Writing file '%s'", outfile_name)
    with open(outfile_name, "w", encoding="utf-8") as outfile:
        json.dump(geojson, outfile, ensure_ascii=True, indent=2)



descriptions_en = {
    0: 'Air quality index',
    1: 'Particulate matter (PM₁₀) Daily average (hourly floating) in µg/m³',
    3: 'Ozone (O₃) One hour average in µg/m³',
    5: 'Nitrogen dioxide (NO₂) One hour average in µg/m³',
    9: 'Particulate matter (PM₂,₅) Daily average (hourly floating) in µg/m³'
}

descriptions_de = {
    1: 'Feinstaub (PM₁₀)',
    3: 'Ozon (O₃)',
    5: 'Stickstoffdioxid (NO₂)',
    9: 'Feinstaub (PM₂,₅)'
}

stations = {
    #       LAT,             LON,                 ID          NAME                        LAGE
    1303: [51.9532732608, 7.61937876070, 'DENW260', 'Münster Weseler Straße', 'städtisch Verkehr'],
    1140: [51.9365217168, 7.61158680053, 'DENW095', 'Münster-Geist', 'städtisch Hintergrund']
}

TZ = pytz.timezone('Europe/Berlin')
TODAY = datetime.now(TZ)
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
logging.info(" TIMEZONE INFO: %s -- %s ", TODAY, LOCAL_TIMEZONE)
big_debug_text('Luftdaten Script')

def create_geojson():
    """fetch latest 10 measures and generate a geojson from it"""

    # get data -8 hours back, because sometimes the sensors dont send data for a few hours
    date_now = datetime.now() + timedelta(hours=3, minutes=0)
    timespan_end = date_now.strftime("%Y-%m-%d")
    timespan_end_h = int(date_now.strftime("%H"))
    date_start = datetime.now() - timedelta(hours=8, minutes=0)
    timespan_start = date_start.strftime("%Y-%m-%d")
    timespan_start_h = int(date_start.strftime("%H"))
    timespan_end_h = timespan_end_h if timespan_end_h else 1
    timespan_start_h = timespan_start_h if timespan_start_h else 1

    geodata = []
    for station_id, station_desc in stations.items():
        url = (
            BASE_URL + 'json'
            f'?date_from={timespan_start}&date_to={timespan_end}&time_from={timespan_start_h}'
            f'&time_to={timespan_end_h}&station={station_id}'
        )
        logging.debug("%s %s : %s", station_desc[2], station_desc[3], url)

        latest_values = {}
        station_data = {}
        uba_json_response = readUrlWithCache(url)
        for date, entry in uba_json_response['data'][f'{station_id}'].items():
            utc_date = entry[0]
            air_quality = entry[1]
            is_incomplete = entry[2]
            logging.debug("%s - %s - %s: %s", date, air_quality, is_incomplete, entry[3:])
            measures_in = {}
            for measure in entry[3:]:
                measures_in[str(measure[0])] = measure

            # Allgemeine Stationsdaten setzen
            measures_out = {
                'Station': station_desc[3],
                'Station-ID': station_desc[2],
                'Zeitpunkt': utc_date,
                'Luftqualitätsindex': air_quality
            }

            # Messwerte aus der API Response auslesen
            for m_id, m_name in descriptions_de.items():
                if str(m_id) in measures_in:
                    measures_out[m_name] = measures_in[str(m_id)][1]
                else:
                    measures_out[m_name] = '-'

            if is_incomplete:
                measures_out['Fehlender Wert'] = 'Ja'

            # Speichere für jeden Messwert den neusten in {latest_values}
            for m_id, m_name in descriptions_de.items():
                if str(m_id) in latest_values:
                    if measures_out[m_name] != '-':
                        current_value = latest_values[str(m_id)][0]
                        current_time = latest_values[str(m_id)][1]
                        if current_time < measures_out['Zeitpunkt']:
                            latest_values[str(m_id)] = [
                                 measures_out[m_name],
                                 measures_out['Zeitpunkt']]
                else:
                    latest_values[str(m_id)] = [measures_out[m_name], measures_out['Zeitpunkt']]

            # Speichere die aktuellsten Stationsdaten (insbes. Luftqualitätsindex) in {station_data}
            if not station_data:
                station_data = measures_out
            else:
                if utc_date > station_data['Zeitpunkt']:
                    station_data = measures_out

        logging.debug(latest_values)

        # Setze die aktuellen Stationsdaten und Messwerte zusammen
        for m_id, m_name in descriptions_de.items():
            station_data[m_name] = latest_values[str(m_id)][0]

                        # lat ,             lon,            measures
        geodata.append([[station_desc[1], station_desc[0]], station_data])

    write_json_file(geodata, DATA_PATH + 'luftqualitaet_muenster.geojson')


def create_csv():
    """ create csv with data of one complete week"""

    # get start and end date of current kalenderwoche
    mydate = datetime.now() - timedelta(days=8, hours=0, minutes=0)

    kalenderwoche = int(mydate.strftime("%V"))
    if kalenderwoche < 1:
        kalenderwoche = 1
    kw_year = int(mydate.strftime("%Y"))

    timespan_end = datetime.strptime(f"{kw_year}-W{kalenderwoche}-1", "%Y-W%W-%w")
    logging.debug("Kalenderwoche %s - %s", kalenderwoche, timespan_end)

    timespan_start = (timespan_end - timedelta(days=8, hours=0, minutes=0)).strftime("%Y-%m-%d")
    timespan_end = timespan_end.strftime("%Y-%m-%d")
    mode = f'kw{kalenderwoche}'

    # more command line options for start and end date?
    if len(sys.argv) == 4:
        timespan_start = sys.argv[2]
        timespan_end = sys.argv[3]
        mode = f'{timespan_start}_{timespan_end}'

    for station_id, station_desc in stations.items():
        url = (BASE_URL + 'csv'
            f'?date_from={timespan_start}'
            f'&date_to={timespan_end}&time_from=01'
            f'&time_to=01&station={station_id}'
        )
        logging.debug("%s %s : %s", station_desc[2], station_desc[3], url)

        req = requests.get(url, timeout=300)
        if req.status_code > 399:
            logging.error('  - Request result: HTTP %s - %s', req.status_code, req.content)
        else:
            with open(DATA_PATH + f"luftqualitaet_muenster_{mode}_station{station_id}.csv", "wb") as csv_file:
                csv_file.write(req.content)


if len(sys.argv) <2:
    print("\033[1;41m")
    print(">> Please run with command line option 'json' or 'csv' \033[1;0m")
    print("\033[1;0m>> ")
    print(">> O P T I O N S ")
    print(">> ")
    print(">> json - can be run 1x per hour, writes a geojson file with latest data, e.g. ")
    print(">>        python3 read_data.py json ")
    print(">> ")
    print(">> csv - option 1: run 1x per week, without further command line options writes data of last week")
    print(">>                 -> python3 read_data.py csv ")
    print(">>       option 2: with $startdate and $enddate, e.g. ")
    print(">>                 -> python3 read_data.py csv 2000-01-01 2024-12-30")
    print("")
    raise ValueError("Missing command line value")

RUNMODE = sys.argv[1]
big_debug_text(f"Running mode: {RUNMODE}")

if RUNMODE == 'json':
    create_geojson()
else:
    create_csv()