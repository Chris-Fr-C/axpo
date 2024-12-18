import fastapi
import fastapi.encoders
import axpo.aemet.scrapping as scrapping
import os
from typing import *
import pandas as pd
import datetime
import http
import logging
import enum
import pytz

logger = logging.getLogger(__name__)

handler = logging.handlers.RotatingFileHandler(
    os.environ.get("LOG_FILE", "routes.log"))
logger.addHandler(handler)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
stdouthandler = logging.StreamHandler()
stdouthandler.setFormatter(formatter)
logger.addHandler(stdouthandler)

PREFIX = "/aemet"
router = fastapi.APIRouter(prefix=PREFIX)


def scrapper() -> scrapping.Scrapper:
    """
    Function used for dependency injection of the scrapper.
    """
    return scrapping.Scrapper.default()


class Station(enum.StrEnum):
    CASTILLA = "Meteo Station Gabriel de Castilla"
    JUAN_CARLOS = "Meteo Station Juan Carlos I"


IDENTITY_MAPPER: Dict[Station, str] = {
    "Meteo Station Gabriel de Castilla": "89070",
    "Meteo Station Juan Carlos I": "89064",
}

AggregationLevel = Union[Literal["hourly", "daily", "monthly"]]

DATEFORMAT = "%Y-%m-%dT%H:%M"

# For proper description in the openapi specification.
Timezone = enum.StrEnum("Timezones", pytz.all_timezones)


@router.get("/antartica")
def get_data(
    scrapper: Annotated[scrapping.Scrapper, fastapi.Depends(scrapper)],
    start_date: str = fastapi.Query(
        description="Start date to fetch data (included)."),
    end_date: str = fastapi.Query(
        description="End date to fetch data (included)."),
    timezone: Timezone = fastapi.Query(
        description="Timezone of the given dates. Defaults to UTC. Output will be in Europe/Madrid timezone.",
        default="utc",
    ),

    locations: List[Station] = fastapi.Query(
        description="List of stations to include in the request. Available: {}".format(
            ", ".join([x for x in IDENTITY_MAPPER.keys()])),
        examples=[x for x in IDENTITY_MAPPER.keys()]
    ),
    aggregation_level: Optional[AggregationLevel] = fastapi.Query(
        description="Aggregation level to set on output data.",
        default=None,
    )

) -> fastapi.Response:
    all_data: List[pd.DataFrame] = []
    tz = pytz.timezone(timezone.strip().lower())
    start_date = datetime.datetime.strptime(start_date, DATEFORMAT).replace(tzinfo=tz)
    end_date = datetime.datetime.strptime(end_date, DATEFORMAT).replace(tzinfo=tz)
    mapper: Dict[AggregationLevel, str] = {
        "hourly": "h",
        "daily": "d",
        "monthly": "M"
    }
    for loc in locations:
        data: List[scrapping.Data] = scrapper.request_data(
            start_date,
            end_date,
            IDENTITY_MAPPER[loc],
        )
        # Not efficient for Raw request but easier to read this exercice.
        df = pd.DataFrame(data)
        # Those string fields cannot be aggregated.
        df.drop(columns={"nombre", "identificacion"}, inplace=True)
        df["fhora"] = pd.to_datetime(df["fhora"])
        if aggregation_level:
            df = df.resample(mapper[aggregation_level], on="fhora").mean()

        df["nombre"] = loc
        all_data.append(df)
    logging.debug("Concatenating dataframes.",
                  extra={"amount_df": len(all_data)})
    grouped = pd.concat(all_data, axis=0)
    # We change the timezone to Europe/Madrid.
    output_tz = "Europe/Madrid"
    # Time is the index of the dataset. The input is already timezone aware since we receive the
    # timezone in the isoformat data from the data source.
    grouped.index = grouped.index.tz_convert(output_tz)
    # We must reset index to keep the date column.
    result = grouped.reset_index().to_dict(orient="records")
    # Datetime is not json serialzable so we use the fastapi encoder.
    return fastapi.responses.JSONResponse(content=fastapi.encoders.jsonable_encoder(result))
