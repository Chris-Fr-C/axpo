"""
This module will handle the scrapping of:
https://opendata.aemet.es/dist/index.html?
API_KEY must be set in the environment variables.
"""
import logging.handlers
from typing import *
import logging
import os
import http
import datetime
import string
import requests
import dataclasses
Url = str
Station = str

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(
    os.environ.get("LOG_FILE", "scrapping.log"))
logger.addHandler(handler)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
stdouthandler = logging.StreamHandler()
stdouthandler.setFormatter(formatter)
logger.addHandler(stdouthandler)


class AntarticaRequestResponse(TypedDict):
    descripcion: str
    estado: int
    datos: str  # Contains the URL to get the json with the actual data
    metadatos:	str  # Contains the URL to get the json with the metadata


class Data(TypedDict):
    identificacion: str
    nombre: Station
    fhora: str  # represents a datetime
    temp: float  # temperature in °C
    pres: float  # presure in hPa
    vel: float  # wind velocity in m/s


class Scrapper():
    """
    Downloads and parses the AEMET data.
    """
    url: Url
    session: requests.Session
    # AAAA-MM-DDTHH:MM:SSUTC
    # Server side time format (from the data source)
    DATEFORMAT = "%Y-%M-%dT%H:%M:%SUTC"

    def __init__(self, base_url: Url, api_key: str):
        self.url = base_url.rstrip("/")
        self.session = requests.Session()
        if not api_key:
            raise EnvironmentError("No api KEY provided")
        self.session.headers.update(
            api_key=api_key
        )

    @property
    def antarctica_url(self) -> string.Template:
        """Return a string template with the antartica url.

        Returns:
            string.Template: Fields to set:
                - start_date
                - end_date
                - location
        """
        return string.Template(self.url+"/api/antartida/datos/fechaini/${start_date}/fechafin/${end_date}/estacion/${location}")

    def request_data(self,
                     start_date: datetime.datetime,
                     end_date: datetime.date,
                     location: Station) -> None:
        logger.info("Data requested. {}".format({
            "endpoint": self.antarctica_url.template,
            "start_date": start_date,
            "end_date": end_date,
            "location": location,
        }))
        start = start_date.strftime(self.DATEFORMAT)
        end = start_date.strftime(self.DATEFORMAT)
        url = self.antarctica_url.safe_substitute(
            start_date=start, end_date=end, location=location)
        resp = self.session.get(url)

        def log_if_error(resp: requests.Response) -> None:
            if resp.status_code != http.HTTPStatus.OK:
                logger.error("Error atempting to download the data (1st stage). {}".format({
                    "url": url, "status_code": resp.status_code, "error": resp.content}))

        log_if_error(resp)
        resp.raise_for_status()
        first_response: AntarticaRequestResponse = resp.json()
        # We must then get the json itself.
        resp = self.session.get(first_response["datos"])
        log_if_error(resp)
        # The content will actually be a LIST of json objects
        data: List[Data] = resp.json()
        # We clean the data we dont need
        wanted_fields = {k for k in Data.__annotations__.keys()}
        output: List[Data] = []
        for entry in data:
            new_obj: Data = dict()
            for k in wanted_fields:
                new_obj[k] = entry[k]
            output.append(new_obj)
        return output

    @staticmethod
    def default() -> "Scrapper":
        return Scrapper(
            "https://opendata.aemet.es/opendata",
            os.environ.get("API_KEY", ""),
        )
