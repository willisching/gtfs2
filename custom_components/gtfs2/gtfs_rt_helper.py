import logging
from datetime import datetime, timedelta
import json
import os

import homeassistant.helpers.config_validation as cv
import homeassistant.util.dt as dt_util
import requests
import voluptuous as vol
from google.transit import gtfs_realtime_pb2
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import ATTR_LATITUDE, ATTR_LONGITUDE, CONF_NAME
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle
import binascii
import base64
from .requests_testadapter import Resp

_LOGGER = logging.getLogger(__name__)

from .const import (

    ATTR_STOP_ID,
    ATTR_ROUTE,
    ATTR_TRIP,
    ATTR_DIRECTION_ID,
    ATTR_DUE_IN,
    ATTR_DUE_AT,
    ATTR_DELAY,
    ATTR_NEXT_UP,
    ATTR_NEXT_RT,
    ATTR_NEXT_RT_DELAYS,
    ATTR_ICON,
    ATTR_UNIT_OF_MEASUREMENT,
    ATTR_DEVICE_CLASS,
    ATTR_LATITUDE,
    ATTR_LONGITUDE,

    CONF_API_KEY,
    CONF_API_KEY_NAME,
    CONF_API_KEY_LOCATION,
    CONF_ACCEPT_HEADER_PB,
    CONF_STOP_ID,
    CONF_ROUTE,
    CONF_TRIP_UPDATE_URL,
    CONF_VEHICLE_POSITION_URL,
    CONF_ROUTE_DELIMITER,
    CONF_ICON,
    CONF_SERVICE_TYPE,

    DEFAULT_SERVICE,
    DEFAULT_ICON,
    DEFAULT_DIRECTION,
    DEFAULT_PATH,
    DEFAULT_PATH_GEOJSON,

    TIME_STR_FORMAT
)

def due_in_minutes(timestamp):
    """Get the remaining minutes from now until a given datetime object."""
    diff = timestamp - dt_util.now().replace(tzinfo=None)
    return int(diff.total_seconds() / 60)

def get_gtfs_feed_entities(url: str, headers, label: str):
    # Guard: if no URL configured for this feed type, skip silently
    if not url:
        _LOGGER.debug("GTFS RT get_feed_entities: no url configured for label: %s, skipping", label)
        return None

    _LOGGER.debug(f"GTFS RT get_feed_entities for url: {url} , headers: {headers}, label: {label}")
    feed = gtfs_realtime_pb2.FeedMessage()  # type: ignore

    # Ensure a User-Agent is always present. Some transit agency gateways
    # (e.g. Azure Application Gateway used by TTC) return 403 when no
    # User-Agent header is supplied, treating the request as a scraper.
    _default_headers = {
        "User-Agent": "Mozilla/5.0 (compatible; GTFS2-HomeAssistant/1.0)"
    }
    if headers:
        _default_headers.update(headers)

    if url.startswith('file'):
        requests_session = requests.session()
        requests_session.mount('file://', LocalFileAdapter())
        response = requests_session.get(url)   
    else:
        response = requests.get(url, headers=_default_headers, timeout=20)

    if response.status_code == 200 and "Bad Gateway" not in response.text and "Not Found" not in response.text :
        _LOGGER.debug("Successfully updated %s", label)
    else:
        _LOGGER.error("Trying to update %s, and got RT response(code): %s with text: %s", label, response.status_code, response.text)
        return None

    if label == "alerts":
        _LOGGER.debug("Feed : %s", feed)
        
    try:
        json_object = json.loads(response.text)    
        feed = json.loads(response.text)
    except ValueError as e:   
        _LOGGER.debug("GTFS RT data is not providing format json")
        if label == "vehicle_positions":
            feed = convert_gtfs_realtime_positions_to_json(response.content)
        elif label == "trip_data":
            feed = convert_gtfs_realtime_to_json(response.content)
        else: # not yet converted to json
            feed.ParseFromString(response.content)
            return feed.entity            
    
    return feed.get('entity')

def get_direction_from_static_gtfs(self, trip_id: str) -> str:
    """Resolve direction_id from static GTFS DB when RT feed omits it.

    Tries several common attribute paths used by the gtfs2 integration to
    locate the SQLite database file, so the lookup is resilient to different
    sensor configurations.
    """
    try:
        import sqlite3
        db_file = None
        for attr in ("_data", ):
            data = getattr(self, attr, None)
            if isinstance(data, dict):
                db_file = data.get("file") or data.get("gtfs_file") or data.get("db_file")
                if db_file:
                    break
        if not db_file:
            db_file = getattr(self, "_gtfs_file", None) or getattr(self, "_db_file", None)

        if not db_file:
            _LOGGER.debug("Static GTFS direction lookup: could not determine db file name for trip %s", trip_id)
            return "nn"

        db_path = self.hass.config.path(DEFAULT_PATH, db_file + ".db")
        _LOGGER.debug("Static GTFS direction lookup: using db path %s for trip %s", db_path, trip_id)
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT direction_id FROM trips WHERE trip_id = ?", (trip_id,)
            ).fetchone()
        if row is not None:
            _LOGGER.debug("Static GTFS direction lookup for trip %s: resolved direction_id %s", trip_id, row[0])
            return str(row[0])
        else:
            _LOGGER.debug("Static GTFS direction lookup: trip %s not found in DB", trip_id)
    except Exception as ex:
        _LOGGER.debug("Static GTFS direction lookup failed for trip %s: %s", trip_id, ex)
    return "nn"

def get_next_services(self):
    self._stop = self._stop_id
    self._destination = self._destination_id
    self._route = self._route_id
    self._trip = self._trip_id
    self._direction = self._direction
    self._trip_short_name = self._trip_short_name
    _LOGGER.debug("Configuration for RT route: %s, RT trip: %s, RT stop: %s, RT direction: %s, trip short name: %s", self._route, self._trip, self._stop, self._direction, self._trip_short_name)
    self._rt_group = "route"
    rt_departures = get_rt_route_trip_statuses(self)
    next_services = rt_departures.get(self._route, {}).get(self._direction, {}).get(self._stop, {}).get("departures", [])
    next_delays = rt_departures.get(self._route, {}).get(self._direction, {}).get(self._stop, {}).get("delays", [])
    
    if next_services:
        _LOGGER.debug("Next services: %s", next_services)
    
    if self._relative :
        due_in = (
            due_in_minutes(next_services[0])
            if len(next_services) > 0
            else "-"
        )
    else:
        due_in = (
            dt_util.as_utc(next_services[0])
            if len(next_services) > 0
            else "-"
        )
    
    attrs = {
        ATTR_DUE_IN: due_in,
        ATTR_STOP_ID: self._stop,
        ATTR_ROUTE: self._route,
        ATTR_TRIP: self._trip,
        ATTR_DIRECTION_ID: self._direction,
        ATTR_NEXT_RT: next_services,
        ATTR_NEXT_RT_DELAYS: next_delays                                        
    }
    
    if len(next_services) > 0:
        attrs[ATTR_DUE_AT] = (
            next_services[0].strftime(TIME_STR_FORMAT)
            if len(next_services) > 0
            else "-"
        )

    if len(next_services) > 1:
        attrs[ATTR_NEXT_UP] = (
            next_services[1].strftime(TIME_STR_FORMAT)
            if len(next_services) > 1
            else "-"
        )
    if len(next_delays) > 0:
        attrs[ATTR_DELAY] = (
            next_delays[0]
            if len(next_delays) > 0
            else "-"
        )                 
    if self._relative :
        attrs[ATTR_UNIT_OF_MEASUREMENT] = "min"
    else :
        attrs[ATTR_DEVICE_CLASS] = (
            "timestamp" 
            if len(next_services) > 0
            else ""
        )
    
    _LOGGER.debug("Next services attributes: %s", attrs)
    return attrs
    
def get_rt_route_trip_statuses(self):
    ''' Get next rt departure for route (multiple) or trip (single) '''
    # explanatory logic
    # sources can provide tip_id with or without route, route with or without direction hence a lot of conditions as the resultset has (!) to include the direction
    # if route-based info is required, for start/end stops, then one needs to cover also for routes without direction_id and thus trip
    # if response does not provide a direction_id then use trip_id, make directon temporarily nn and when the stop is identified make it equal to the requesting direction
    # in this case the trip still covers the direction

    departure_times = {}

    # Guard: if no trip update URL is configured (e.g. alerts-only feeds like
    # TTC subway), skip trip fetching entirely and return empty departures.
    if not self._trip_update_url:
        _LOGGER.debug("No trip_update_url configured — skipping RT trip fetch")
        return departure_times
    
    if self._vehicle_position_url:   
        vehicle_positions = get_rt_vehicle_positions(self)

    feed_entities = get_gtfs_feed_entities(
        url=self._trip_update_url, headers=self._headers, label="trip_data"
    )
    self._feed_entities = feed_entities
    
    if not feed_entities:
        _LOGGER.debug("No proper RT feed entities: %s", feed_entities)
        return {}

    _LOGGER.debug("Search departure times for route: %s, trip: %s, type: %s, direction: %s, short_name: %s", self._route_id, self._trip_id, self._rt_group, self._direction, self._trip_short_name)
    for entity in feed_entities:

        if entity.get('trip_update', False):
            
            # If delimiter specified split the route ID in the gtfs rt feed
            if self._route_delimiter is not None:
                route_id_split = entity["trip_update"]["trip"]["route_id"].split(
                    self._route_delimiter
                )
                if route_id_split[0] == self._route_delimiter:
                    route_id = entity["trip_update"]["trip"]["route_id"]
                else:
                    route_id = route_id_split[0]
            else:
                route_id = entity["trip_update"]["trip"]["route_id"]

            trip_id = entity["trip_update"]["trip"]["trip_id"]

            if "direction_id" in entity["trip_update"]["trip"]:
                # direction_id was explicitly present in the (converted) feed dict
                direction_id = entity["trip_update"]["trip"]["direction_id"]
            else:
                # RT feed did not set direction_id (e.g. TTC omits it).
                # convert_gtfs_realtime_to_json now omits the key rather than
                # writing the protobuf default of 0, which would incorrectly
                # label all un-tagged trips (including eastbound) as direction 0.
                # First try to resolve from the static GTFS SQLite DB; if that
                # also fails, fall back to "nn" so the existing trip_id-based
                # matching logic below can still catch single-trip sensors.
                direction_id = get_direction_from_static_gtfs(self, trip_id)
                _LOGGER.debug(
                    "RT feed missing direction_id for trip %s — resolved: %s",
                    trip_id, direction_id
                )
                
            # for route-based requests, if the rt-data has no route (ex. TER) then the selection should be on matching trip_id or matching RT-id with short_name (ex. MTA Metro North RR)
            # result will be that only one RT value will be collected
            if not route_id:
                self._rt_group = "trip"   
                route_id = self._route_id                
                
            if self._rt_group == "trip":
                direction_id = self._direction   

            entity_id = entity["id"]
            
            _LOGGER.debug("Search for entity with params - group: %s, route_id: %s, direction_id: %s, self_trip_id: %s, with rt trip: %s, rt id: %s", self._rt_group, route_id, direction_id, self._trip_id, entity["trip_update"]["trip"], entity_id)            

            # Determine whether this entity matches what we're looking for.
            # When direction_id is still "nn" after the static GTFS lookup (i.e.
            # lookup failed), treat a route_id match as sufficient for route-based
            # sensors so that departures are not silently dropped.
            direction_resolved = direction_id != "nn"
            route_match = (route_id == self._route_id or self._route_id in route_id)
            direction_match = (str(direction_id) == str(self._direction))

            # first part covers start/end and thus multiple RT are possible for the same stop, also, for SIRI route_id do not match so a 'in' is used 
            # the second part covers local stops, i.e. per trip, so only one RT possible for that stop         
            if (self._rt_group == "route" and route_match and (direction_match or (not direction_resolved and self._rt_group == "route"))) or (direction_id == "nn" and trip_id == self._trip_id) or (self._trip_id in trip_id) or (self._rt_group == "trip" and (trip_id == self._trip_id or self._trip_id in trip_id)) or entity_id == self._trip_short_name:
                
                _LOGGER.debug("Entity found params - group: %s, route_id: %s, direction_id: %s, self_trip_id: %s, with rt trip: %s, rt id: %s", self._rt_group, route_id, direction_id, self._trip_id, entity["trip_update"]["trip"], entity_id)
                
                for stop in entity["trip_update"]["stop_time_update"]:
                    stop_id = stop["stop_id"]
                    stop_sequence = stop["stop_sequence"]
                    if stop_id == self._stop_id or (stop_id == "" and stop_sequence == self._stop_sequence):
                        _LOGGER.debug("Stop found: %s", stop)
                        # if the data does not contain a stop_id but only a stop_sequence, assume stop_id being the correct stop based on sequence
                        # this does not have to be always correct but best-guess
                        if stop_id == "":
                            stop_id = self._stop_id
                        
                        if self._route_id not in departure_times:
                            departure_times[self._route_id] = {}
                                               
                        if direction_id == "nn" or entity_id == self._trip_short_name: # in this case the trip_id serves as a basis so one can safely set direction to the requesting entity direction
                            direction_id = self._direction                   

                        if direction_id not in departure_times[self._route_id]:
                            departure_times[self._route_id][direction_id] = {}
                            
                        if not departure_times[self._route_id][direction_id].get(
                            stop_id
                        ):
                            departure_times[self._route_id][direction_id][stop_id] = {}
                        
                        if not departure_times[self._route_id][direction_id][stop_id].get(
                            "departures"
                        ):                 
                            departure_times[self._route_id][direction_id][stop_id]["departures"] = []
                            departure_times[self._route_id][direction_id][stop_id]["delays"] = []
                        
                        # Use stop arrival time;
                        # fall back on departure time if not available                            
                        if stop["arrival"]["time"] == 0:
                            stop_time = stop["departure"]["time"]
                        else:
                            stop_time = stop["arrival"]["time"]
                            
                        if stop["departure"].get("delay",0) >= stop["arrival"].get("delay",0):
                            delay = stop["departure"].get("delay",0)
                        else: 
                            delay = stop["arrival"].get("delay",0)
                            
                        # Ignore arrival times in the past
                        
                        if due_in_minutes(datetime.fromtimestamp(stop_time)) >= 0:
                            departure_times[self._route_id][direction_id][
                                stop_id
                            ]["departures"].append(datetime.utcfromtimestamp(stop_time).replace(tzinfo=dt_util.get_time_zone("UTC")))
                            _LOGGER.debug("RT stoptime: %s, utcfromtimestamp: %s, format utc: %s", stop_time, datetime.utcfromtimestamp(stop_time), datetime.utcfromtimestamp(stop_time).replace(tzinfo=dt_util.get_time_zone("UTC")))
                        else:
                            _LOGGER.debug("Not using realtime stop data for old due-in-minutes: %s", due_in_minutes(datetime.fromtimestamp(stop_time)))
                            
                        departure_times[self._route_id][direction_id][stop_id]["delays"].append(delay)

                        
    # Sort by time
    for route in departure_times:
        for direction in departure_times[self._route_id]:
            for stop in departure_times[self._route_id][direction]:
                for t in departure_times[self._route_id][direction][stop]["departures"]:
                    departure_times[self._route_id][direction][stop]["departures"].sort()

    self.info = departure_times
    _LOGGER.debug("Departure times Route Trip: %s", departure_times)
    return departure_times    

def get_rt_vehicle_positions(self):
    feed_entities = get_gtfs_feed_entities(
        url=self._vehicle_position_url,
        headers=self._headers,
        label="vehicle_positions",
    )
    geojson_body = []
    geojson_element = {"geometry": {"coordinates":[],"type": "Point"}, "properties": {"id": "", "title": "", "trip_id": "", "route_id": "", "direction_id": "", "vehicle_id": "", "vehicle_label": ""}, "type": "Feature"}
    for entity in feed_entities:
        vehicle = entity["vehicle"]
        
        if not vehicle["trip"]["trip_id"]:
            # Vehicle is not in service
            continue
        if vehicle["trip"]["trip_id"] == self._trip_id: 
            _LOGGER.debug('Adding position for TripId: %s, RouteId: %s, DirectionId: %s, Lat: %s, Lon: %s, crc_trip_id: %s', vehicle["trip"]["trip_id"],vehicle["trip"]["route_id"],vehicle["trip"]["direction_id"],vehicle["position"]["latitude"],vehicle["position"]["longitude"], binascii.crc32((vehicle["trip"]["trip_id"]).encode('utf8')))  
            
        # add data if in the selected direction
        if (str(self._route_id) == str(vehicle["trip"]["route_id"]) or str(vehicle["trip"]["trip_id"]) == str(self._trip_id)) and str(self._direction) == str(vehicle["trip"]["direction_id"]):
            _LOGGER.debug("Found vehicle on route with attributes: %s", vehicle)
            _LOGGER.debug("crc : %s", binascii.crc32((vehicle["trip"]["trip_id"]).encode('utf8')))
            geojson_element = {"geometry": {"coordinates":[],"type": "Point"}, "properties": {"id": "", "title": "", "trip_id": "", "route_id": "", "direction_id": "", "vehicle_id": "", "vehicle_label": ""}, "type": "Feature"}
            geojson_element["geometry"]["coordinates"] = []
            geojson_element["geometry"]["coordinates"].append(vehicle["position"]["longitude"])
            geojson_element["geometry"]["coordinates"].append(vehicle["position"]["latitude"])
            geojson_element["properties"]["id"] = str(self._route_id) + "(" + str(vehicle["trip"]["direction_id"]) + ")" + str(binascii.crc32((vehicle["trip"]["trip_id"]).encode('utf8')))[-3:]
            geojson_element["properties"]["title"] = str(self._route_id) + "(" + str(vehicle["trip"]["direction_id"]) + ")" + str(binascii.crc32((vehicle["trip"]["trip_id"]).encode('utf8')))[-3:] + "_" + self._icon.split(':')[1]
            geojson_element["properties"]["trip_id"] = vehicle["trip"]["trip_id"]
            geojson_element["properties"]["route_id"] = str(self._route_id)
            geojson_element["properties"]["direction_id"] = vehicle["trip"]["direction_id"]
            geojson_element["properties"]["vehicle_id"] = vehicle["vehicle"]["id"]
            geojson_element["properties"]["vehicle_label"] = vehicle["vehicle"]["label"]
            geojson_element["properties"][vehicle["trip"]["trip_id"]] = geojson_element["geometry"]["coordinates"]
            geojson_body.append(geojson_element)
    
    self.geojson = {"features": geojson_body, "type": "FeatureCollection"}
        
    _LOGGER.debug("Vehicle geojson: %s", json.dumps(self.geojson))
    self._route_dir = str(self._route_id) + "_" + str(self._direction)
    update_geojson(self)
    return geojson_body
    
def get_rt_alerts(self):
    rt_alerts = {}
    if not self._alerts_url or not self._alerts_url.startswith("http"):
        return rt_alerts

    feed_entities = get_gtfs_feed_entities(
        url=self._alerts_url,
        headers=self._headers,
        label="alerts",
    )

    if not feed_entities:
        return rt_alerts

    for entity in feed_entities:
        if entity.HasField("alert"):
            # Extract alert text cleanly rather than string-splitting the proto repr
            alert_text = ""
            try:
                # Use description_text for the full message; fall back to
                # header_text if description is absent (e.g. some feeds omit it)
                desc = entity.alert.description_text.translation
                alert_text = desc[0].text if desc else ""
            except (IndexError, AttributeError):
                pass
            if not alert_text:
                try:
                    alert_text = entity.alert.header_text.translation[0].text
                except (IndexError, AttributeError):
                    pass

            # Evaluate each informed_entity independently so alerts covering
            # multiple routes/stops are not filtered down to just the last one.
            # Bug fix: the original code checked HasField("stop_id") for both
            # stop_id AND route_id (copy-paste error), causing route_id to always
            # resolve to "unknown" when stop_id was absent (e.g. route-only alerts
            # like TTC subway detours), and then never matching self._route_id.
            for x in entity.alert.informed_entity:
                try:
                    stop_id = x.stop_id if x.HasField("stop_id") else "unknown"
                except ValueError:
                    stop_id = x.stop_id or "unknown"
                try:
                    route_id = x.route_id if x.HasField("route_id") else "unknown"
                except ValueError:
                    route_id = x.route_id or "unknown"

                _LOGGER.debug("RT Alert check — route: %s, stop: %s, self_route: %s, self_stop: %s, self_dest: %s",
                    route_id, stop_id, self._route_id, self._stop_id, self._destination_id)

                if stop_id == self._stop_id and (route_id == "unknown" or route_id == self._route_id):
                    _LOGGER.debug("RT Alert matched origin stop for route: %s, stop: %s", route_id, stop_id)
                    rt_alerts["origin_stop_alert"] = alert_text
                if stop_id == self._destination_id and (route_id == "unknown" or route_id == self._route_id):
                    _LOGGER.debug("RT Alert matched destination stop for route: %s, stop: %s", route_id, stop_id)
                    rt_alerts["destination_stop_alert"] = alert_text
                if stop_id == "unknown" and route_id == self._route_id:
                    _LOGGER.debug("RT Alert matched route (no stop filter) for route: %s", route_id)
                    rt_alerts["origin_stop_alert"] = alert_text
                    rt_alerts["destination_stop_alert"] = alert_text

    return rt_alerts
    
def get_rt_alerts_json(self):
    rt_alerts = {}
    if not self._alerts_url or not self._alerts_url.startswith("http"):
        return rt_alerts

    feed_entities = get_gtfs_feed_entities(
        url=self._alerts_url,
        headers=self._headers,
        label="alerts",
    )

    if not feed_entities:
        return rt_alerts

    for entity in feed_entities:
        alert = entity.get("alert")
        if alert:
            # Use description_text for the full message; fall back to header_text
            _translations = alert.get("description_text", {}).get("translation", [])
            alert_text = _translations[0].get("text", "") if _translations else ""
            if not alert_text:
                _translations = alert.get("header_text", {}).get("translation", [])
                alert_text = _translations[0].get("text", "") if _translations else ""

            # Evaluate each informed_entity independently — same bug fix as
            # get_rt_alerts: loop over all entities and match inside the loop.
            for x in alert.get("informed_entity", []):
                stop_id = x.get("stop_id") or "unknown"
                route_id = x.get("route_id") or "unknown"

                _LOGGER.debug("RT Alert JSON check — route: %s, stop: %s, self_route: %s, self_stop: %s, self_dest: %s",
                    route_id, stop_id, self._route_id, self._stop_id, self._destination_id)

                if stop_id == self._stop_id and (route_id == "unknown" or route_id == self._route_id):
                    _LOGGER.debug("RT Alert JSON matched origin stop for route: %s, stop: %s", route_id, stop_id)
                    rt_alerts["origin_stop_alert"] = alert_text
                if stop_id == self._destination_id and (route_id == "unknown" or route_id == self._route_id):
                    _LOGGER.debug("RT Alert JSON matched destination stop for route: %s, stop: %s", route_id, stop_id)
                    rt_alerts["destination_stop_alert"] = alert_text
                if stop_id == "unknown" and route_id == self._route_id:
                    _LOGGER.debug("RT Alert JSON matched route (no stop filter) for route: %s", route_id)
                    rt_alerts["origin_stop_alert"] = alert_text
                    rt_alerts["destination_stop_alert"] = alert_text

    return rt_alerts
    
    
def update_geojson(self):    
    geojson_dir = self.hass.config.path(DEFAULT_PATH_GEOJSON)
    os.makedirs(geojson_dir, exist_ok=True)
    file = os.path.join(geojson_dir, self._route_dir + ".json")
    _LOGGER.debug("Creating geojson file: %s", file)
    with open(file, "w") as outfile:
        json.dump(self.geojson, outfile)
    
def get_gtfs_rt(hass, path, data):
    """Get gtfs rt data."""
    _LOGGER.debug("Getting gtfs rt locally with data: %s", data)
    _headers = data.get('headers','')
    _source_format = data.get('source_format',None)                                                  
    gtfs_dir = hass.config.path(path)
    os.makedirs(gtfs_dir, exist_ok=True)
    url = data["url"]
    file = data["file"] + ".rt"
    if data.get(CONF_API_KEY_LOCATION, None) == "query_string":
      if data.get(CONF_API_KEY, None):
        url = url + "?" + data[CONF_API_KEY_NAME] + "=" + data[CONF_API_KEY]
    if data.get(CONF_API_KEY_LOCATION, None) == "header":
        _headers = {data[CONF_API_KEY_NAME]: data[CONF_API_KEY]}
        if data.get(CONF_ACCEPT_HEADER_PB, False):
            _headers["Accept"] = "application/x-protobuf"
    _LOGGER.debug("Getting gtfs rt locally with headers: %s", _headers)
    
    if data.get('entity_for_siri',None):
        _LOGGER.debug("Getting siri RT departures with data: %s", data)
        entity_registry = er.async_get(hass)
        entity = er.async_get(hass).async_get(data["entity_for_siri"])
        _LOGGER.debug("entity: %s", entity)
        _LOGGER.debug("entity cfg id: %s", entity.config_entry_id)
        config_entry = hass.config_entries.async_get_entry(entity.config_entry_id)
        cf_data = config_entry.data
        cf_options = config_entry.options
        _stop_id = cf_data["origin"].split(':')[0]
        _LOGGER.debug("_stop_id: %s", _stop_id)
        _LOGGER.debug("config entry data: %s, options: %s", cf_data, cf_options)
        file = data["file"] + "_rt.json"
        #try:
        r = convert_realtime_siri_trips_to_json(url,_headers,_stop_id)
        open(os.path.join(gtfs_dir, file), "w").write(json.dumps(r))
        return "ok"
        #except Exception as ex:  # pylint: disable=broad-except
        #    _LOGGER.error("Ìssues with downloading GTFS RT SIRI data to: %s with error: 5s", os.path.join(gtfs_dir, file), ex)
        #    return "no_rt_data_file" 
        return "ok"                                
    try:
        r = requests.get(url, headers = _headers , allow_redirects=True)
        open(os.path.join(gtfs_dir, file), "wb").write(r.content)
        if r.status_code != 200:
            _LOGGER.error("Ìssues with downloading GTFS RT data, error: %s, content: %s", r.status_code, r.content)
            return "no_rt_data_file"
    except Exception as ex:  # pylint: disable=broad-except
        _LOGGER.error("Ìssues with downloading GTFS RT data to: %s", os.path.join(gtfs_dir, file))
        return "no_rt_data_file"

    
    if data.get("debug_output", False):
        try:
            data_out = ""
            feed_entities = get_gtfs_feed_entities(
                url=data.get("url", None),
                headers=_headers,
                label=data.get("rt_type", "-"),
            )  
            file_all = data["file"] + "_converted.txt"
            # check if content is json else write without format            
            try:
                open(os.path.join(gtfs_dir, file_all), "w").write(json.dumps(feed_entities, indent=4)) 
            except Exception as ex:
                _LOGGER.debug("Not writing to file as json because of error: %s", ex)
                open(os.path.join(gtfs_dir, file_all), "w").write(str(feed_entities))              
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.info("Ìssues with converting GTFS RT data to JSON, output to string") 
    return "ok"   
        
class LocalFileAdapter(requests.adapters.HTTPAdapter):
    """Used to allow requests.get for local file"""
    def build_response_from_file(self, request):
        file_path = request.url[7:]
        with open(file_path, 'rb') as file:
            buff = bytearray(os.path.getsize(file_path))
            file.readinto(buff)
            resp = Resp(buff)
            r = self.build_response(request, resp)
            return r

    def send(self, request, stream=False, timeout=None,
             verify=True, cert=None, proxies=None):
        return self.build_response_from_file(request)   

def convert_gtfs_realtime_to_json(gtfs_realtime_data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(gtfs_realtime_data)

    json_data = {
        "header": {
            "gtfs_realtime_version": feed.header.gtfs_realtime_version,
            "timestamp": feed.header.timestamp,
            "incrementality": feed.header.incrementality
        },
        "entity": []
    }

    for entity in feed.entity:
        # Build the trip dict without direction_id first.
        # direction_id is optional uint32 in proto2. When a feed omits it (e.g.
        # TTC), the protobuf library returns the default value of 0, which would
        # silently label every un-tagged trip — including eastbound ones — as
        # direction 0. We only write direction_id into the dict when it is
        # explicitly set in the feed, so that get_rt_route_trip_statuses can
        # detect the absence and fall back to the static GTFS DB lookup.
        trip_dict = {
            "trip_id": entity.trip_update.trip.trip_id,
            "start_time": entity.trip_update.trip.start_time,
            "start_date": entity.trip_update.trip.start_date,
            "route_id": entity.trip_update.trip.route_id,
        }
        try:
            if entity.trip_update.trip.HasField("direction_id"):
                trip_dict["direction_id"] = str(entity.trip_update.trip.direction_id)
            # else: leave direction_id absent — handled in get_rt_route_trip_statuses
        except ValueError:
            # HasField not supported for this field in this protobuf build;
            # fall back to original behaviour of always including it.
            trip_dict["direction_id"] = str(entity.trip_update.trip.direction_id)

        entity_dict = {
            "id": entity.id,
            "trip_update": {
                "trip": trip_dict,
                "stop_time_update": []
            }
        }
        for stop_time_update in entity.trip_update.stop_time_update:
            stop_time_update_dict = {
                "stop_sequence": stop_time_update.stop_sequence,
                "stop_id": stop_time_update.stop_id,
                "arrival": {
                    "delay": stop_time_update.arrival.delay,
                    "time": stop_time_update.arrival.time
                },
                "departure": {
                    "delay": stop_time_update.departure.delay,
                    "time": stop_time_update.departure.time
                }
            }
            entity_dict["trip_update"]["stop_time_update"].append(stop_time_update_dict)
        
        json_data["entity"].append(entity_dict)
    return json_data        

def convert_gtfs_realtime_positions_to_json(gtfs_realtime_data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(gtfs_realtime_data)

    json_data = {
        "entity": []
    }
    for ent in feed.entity:
        entity = ent.vehicle
        entity_dict = {
        "vehicle": {
            "trip": {
                "trip_id" : entity.trip.trip_id,
                "route_id": entity.trip.route_id,
                "direction_id": entity.trip.direction_id
                },
            "vehicle": {
                "id": entity.vehicle.id,
                "label": entity.vehicle.label
                },
            "position": {
                "latitude": entity.position.latitude,
                "longitude": entity.position.longitude,
                "bearing": entity.position.bearing,
                "speed": entity.position.speed
            },
            "stop_id": entity.stop_id,
            "timestamp": entity.timestamp
        }
        }
        json_data["entity"].append(entity_dict)
    return json_data    

def convert_gtfs_realtime_alerts_to_json(gtfs_realtime_data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(gtfs_realtime_data)

    json_data = {
        "entity": []
    }
    for entity in feed.entity:
        _LOGGER.debug("Alert entity: %s", entity)
        if entity.HasField('alert'):
            informed_entities = []
            for informed_entity in entity.alert.informed_entity:
                informed_entity_json = {
                        "route_id": informed_entity.route_id,
                        "trip_id": informed_entity.trip.trip_id
                    }
                informed_entities.append(informed_entity_json)
            entity_dict = {
                "alert": {
                    "id": entity.id,
                    #"active_period": {
                    #    "start": entity.alert.active_period.start,
                    #    "end": entity.alert.active_period.end
                    #},
                    "informed_entity": informed_entities,
                    "header_text": entity.alert.header_text,
                    "description_text": entity.alert.description_text
                }   
            }
        json_data["entity"].append(entity_dict)
        _LOGGER.debug("Alert entity JSON: %s", json_data["entity"])
    return json_data      
    
def convert_realtime_siri_trips_to_json(url,headers,stop_id):
    
    #Used for Strasbourg, but they differ on output too
    ##the Basic token is a base64 conversion of: d6452e5d-4894-4ee1-8d5b-11ce235eeef6	
    ## ZDY0NTJlNWQtNDg5NC00ZWUxLThkNWItMTFjZTIzNWVlZWY2
    ## ZDY0NTJlNWQtNDg5NC00ZWUxLThkNWItMTFjZTIzNWVlZWY2Og==    
    #_encoded = base64.b64encode(b'd6452e5d-4894-4ee1-8d5b-11ce235eeef6:').decode("utf-8") 
    #_headers = { "Authorization": f"Basic {_encoded}" }
    #url = "https://api.cts-strasbourg.eu/v1/siri/2.0/stop-monitoring?MonitoringRef=GACEN_20"

    #url = "https://bustime.mta.info/api/siri/stop-monitoring.json?key=f4f9c18e-0550-4cc7-bc36-275715015673&OperatorRef=MTA"
    
    url = url + f"&MonitoringRef={stop_id}"
    response = requests.get(url, headers=headers, timeout=20)

    json_object = json.loads(response.content)
    feed = json_object

    if feed.get('Siri'):
        try:
            feed_entities = feed['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
            feed = feed['Siri']
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.error("Ìssues getting GTFS RT SIRI data: %s", ex)
            return 'issues with getting siri data'        
    else:  
        try:
            feed_entities = feed['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.error("Ìssues getting GTFS RT SIRI data: %s", ex)
            return 'issues with getting siri data'
        
    _LOGGER.debug("Feed entities: %s", feed_entities)

    tt = datetime.fromisoformat(feed['ServiceDelivery']['ResponseTimestamp'])
    json_data = {
        "header": {
            "gtfs_realtime_version": feed['ServiceDelivery']['StopMonitoringDelivery'][0].get('version','not_provided'),
            "timestamp": feed['ServiceDelivery']['ResponseTimestamp'],
            "incrementality": "n/a"
        },
        "entity": []
    }


    for entity in feed_entities:
        entity_dict = {
            "id": entity['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef'],
            "trip_update": {
                "trip": {
                    "trip_id": entity['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef'],
                    "start_time": datetime.fromisoformat(entity['MonitoredVehicleJourney']['MonitoredCall'].get('ExpectedDepartureTime',entity['MonitoredVehicleJourney']['MonitoredCall'].get('AimedDepartureTime',None))).timestamp(),
                    "start_date": datetime.fromisoformat(entity['MonitoredVehicleJourney']['MonitoredCall'].get('ExpectedDepartureTime',entity['MonitoredVehicleJourney']['MonitoredCall'].get('AimedDepartureTime',None))).timestamp(),
                    "route_id": entity['MonitoredVehicleJourney']['LineRef'],
                    "direction_id": str(entity['MonitoredVehicleJourney']['DirectionRef'])
                },
                "stop_time_update": [{
                    "stop_sequence": "n.a",
                    "stop_id": stop_id,
                    "arrival": {
                        "delay": '',
                        "time": datetime.fromisoformat(entity['MonitoredVehicleJourney']['MonitoredCall'].get('ExpectedArrivlTime',entity['MonitoredVehicleJourney']['MonitoredCall'].get('AimedArrivalTime',None))).timestamp()
                    },
                    "departure": {
                        "delay": '',
                        "time": datetime.fromisoformat(entity['MonitoredVehicleJourney']['MonitoredCall'].get('ExpectedDepartureTime',entity['MonitoredVehicleJourney']['MonitoredCall'].get('AimedDepartureTime',None))).timestamp()
                    }
                }]
            }
        }
        
        json_data["entity"].append(entity_dict)
        
    _LOGGER.debug("json data: %s", json.dumps(json_data))
    return json_data
