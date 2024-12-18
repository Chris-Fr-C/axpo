"""
This module will handle the scrapping of:
https://opendata.aemet.es/dist/index.html?
API_KEY must be set in the environment variables.
"""
import logging.handlers
from typing import *
import logging
import os
import itertools
import pytz
import sqlite3
import http
import datetime
import pathlib
import string
import requests
Url = str
StationId = str  # Technical identifier of the station. Example: 89070

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
    nombre: StationId
    fhora: str  # represents a datetime
    temp: float  # temperature in °C
    pres: float  # presure in hPa
    vel: float  # wind velocity in m/s


# We could also use an Enum for the oclumn names, or a dataclass.
class RenamedData(TypedDict):
    """
    Data columns standardized into english.
    """
    identifier: str
    name: StationId
    ts: datetime.datetime
    temperature: float
    pressure: float
    velocity: float

    @staticmethod
    def mapping() -> Dict[str, str]:
        """
        Returns the mapping of columns names Spanish -> English.
        """
        return {
            "fhora": "ts",
            "identificacion": "identifier",
            "nombre": "name",
            "pres": "pressure",
            "vel": "velocity",
            "temp": "temperature",
        }


class Scrapper():
    """
    Downloads and parses the AEMET data.
    """
    url: Url
    session: requests.Session
    # AAAA-MM-DDTHH:MM:SSUTC
    # Server side time format (from the data source)
    DATEFORMAT = "%Y-%m-%dT%H:%M:%SUTC"
    DATEBASE_TIMEZONE = pytz.UTC
    DATABASE_FORMAT = "%Y-%m-%dT%H:%M:%S"
    db_path: pathlib.Path

    def __init__(
        self, base_url: Url, api_key: str, database_path: pathlib.Path
    ):
        self.url = base_url.rstrip("/")
        self.session = requests.Session()
        if not api_key:
            raise EnvironmentError("No api KEY provided")
        self.session.headers.update(
            api_key=api_key
        )
        # Not keeping an open connection as it might have unexpected behavior.
        self.db_path = database_path
        self.setup_database()

    def setup_database(self) -> None:
        """
        Creates the different tables in the database if they are not present.
        Note: all values in table will be normalized to UTC dates, and standard units (example: Pa instead of hPa)
        Temperatures will be kept in °C despite °K being the standard.

        Structure:
        ```mermaid
        Measure {
            string identifier
            timestamp ts
            float temperature
            float pressure
            float velocity
        }

        Station {
            string name
            string identifier
        }


        Measure }|--|| Station: id
        ```

        """
        commands = ["create_measure.sql",
                    "create_station.sql", "insert_stations.sql"]
        list_of_statements: List[str] = []
        for command in commands:
            with open(os.path.join(os.path.dirname(__file__), "scripts", command), 'r') as fi:
                list_of_statements.append(fi.read())

        with sqlite3.connect(self.db_path) as connection:
            for statement in list_of_statements:
                # No need for a transaction here.
                logger.debug(
                    "Setting up database. Step command:\n{}".format(statement))
                connection.execute(statement)
        return

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

    def fetch_from_database(
        self, start_date: datetime.datetime,
        end_date: datetime.date,
        location: StationId
    ) -> Optional[List[RenamedData]]:
        # Note: this is optimizable, we can for instance just query the missing data, but for simplicity we
        # will just check start, end, otherwise query everything.
        check_start = string.Template(
            """
            SELECT count(*) as amount
            """)
        with sqlite3.connect(self.db_path) as connection:
            pass

        return None

    def _query_single_location(self, start: str, end: str, location: StationId) -> List[RenamedData]:
        """Queries distant source on specific location.

        Args:
            start (str): Start date (%Y-%M-%dT%H:%M:%SUTC)
            end (str): End date (%Y-%M-%dT%H:%M:%SUTC)
            location (Station): Location to query onto.

        Returns:
            List[RenamedData]: Formated data.
        """
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
        # To standardize the units.
        convertion_factor = {
            "pres": 1e2,  # 1hPa = 100 Pa
        }
        output: List[RenamedData] = []
        name_mapper = RenamedData.mapping()
        for entry in data:
            new_obj: RenamedData = dict()
            for k in wanted_fields:
                renamed_field = name_mapper[k]
                if k in convertion_factor:
                    new_obj[renamed_field] = convertion_factor[k] * entry[k]
                elif k == "fhora":
                    new_obj[renamed_field] = datetime.datetime.fromisoformat(
                        entry[k]).astimezone(self.DATEBASE_TIMEZONE)
                else:
                    new_obj[renamed_field] = entry[k]
            output.append(new_obj)
        return output

    def update_data(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        locations: List[StationId],
    ) -> None:
        """
        This function queries the distant endpoints and updates the database.
        This should regularly be called through a cron.
        """
        start = start_date.astimezone(pytz.UTC).strftime(self.DATEFORMAT)
        end = end_date.astimezone(pytz.UTC).strftime(self.DATEFORMAT)
        for location in locations:
            chunk = self._query_single_location(start, end, location)
            self.insert_into_db(chunk)

    def insert_into_db(self, data: List[RenamedData], batch_size: int = 50) -> None:
        # We do not need a transaction begin in this case.
        base_query = string.Template(
            """
        INSERT OR IGNORE INTO Measure(
            identifier, ts, temperature, pressure, velocity
        )
        VALUES
            ${values}
        ;
        """
        )
        logger.info("Attempting to insert {} rows by batch of {}".format(
            len(data), batch_size))
        with sqlite3.connect(self.db_path) as connection:
            for batch in itertools.batched(data, batch_size):
                rows: List[str] = []
                # Note that sqlite does NOT have a real time format. So we store everything in UTC.
                for val in batch:
                    row = """("{identifier}", "{ts}", {temperature}, {pressure}, {velocity})""".format(
                        identifier=val["identifier"],
                        ts=val["ts"].strftime(self.DATABASE_FORMAT),
                        temperature=val["temperature"],
                        pressure=val["pressure"],
                        velocity=val["velocity"],
                    )
                    rows.append(row)
            query = base_query.safe_substitute(values=",\n".join(rows))
            connection.execute(query)
        return

    def request_data(
            self,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            location: StationId
    ) -> List[RenamedData]:
        # We first check if the data is available

        logger.info("Data requested. {}".format({
            "endpoint": self.antarctica_url.template,
            "start_date": start_date,
            "end_date": end_date,
            "location": location,
        }))
        base_query = """
        SELECT
                identifier,
                ts,
                temperature,
                pressure,
                velocity

        FROM Measure
        WHERE
            ts>="{start_date}"
            AND ts<="{end_date}"
            AND identifier="{identifier}"
        """
        # TODO: we could add a LIMIT to make sure we dont get ddos.
        with sqlite3.connect(self.db_path) as connection:
            query = base_query.format(
                start_date=start_date.astimezone(
                    pytz.UTC).strftime(self.DATABASE_FORMAT),
                end_date=end_date.astimezone(
                    pytz.UTC).strftime(self.DATABASE_FORMAT),
                identifier=location,
            )
            data = connection.execute(query)

            names_list = connection.execute(
                "SELECT identifier,name FROM  Station"
            ).fetchall()
        names_mapper: Dict[StationId, str] = {
            x[0]: x[1] for x in names_list
        }
        ordered_fields = [
            "identifier",
            "ts",
            "temperature",
            "pressure",
            "velocity",
        ]
        parsed_data: List[RenamedData] = []
        for row in data.fetchall():
            value: RenamedData = {name: row[index]
                                  for index, name in enumerate(ordered_fields)}
            value["name"] = names_mapper[row[0]]
            value["ts"] = datetime.datetime.strptime(
                value["ts"], self.DATABASE_FORMAT).astimezone(self.DATEBASE_TIMEZONE)
            parsed_data.append(value)
        return parsed_data

    @staticmethod
    def default() -> "Scrapper":
        return Scrapper(
            "https://opendata.aemet.es/opendata",
            os.environ.get("API_KEY", ""),
            pathlib.Path(os.environ.get("DATABASE_PATH", "axpo.sqlite")),
        )
