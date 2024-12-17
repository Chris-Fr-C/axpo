import fastapi
import axpo.aemet.scrapping as scrapping
import os
from typing import *
import pandas as pd
import datetime
import http
import logging
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


Stations = Union[Literal["Meteo Station Gabriel de Castilla",
                         "Meteo Station Juan Carlos I"]]

IDENTITY_MAPPER: Dict[Stations, str] = {
    "Meteo Station Gabriel de Castilla": "89070",
    "Meteo Station Juan Carlos I": "89064",
}

AggregationLevel = Union[Literal["hourly", "daily", "monthly"]]

DATEFORMAT = "%Y-%m-%dT%H:%M"


@router.get("/antartica")
def get_data(
    scrapper: Annotated[scrapping.Scrapper, fastapi.Depends(scrapper)],
    start_date: str = fastapi.Query(
        description="Start date to fetch data (included)."),
    end_date: str = fastapi.Query(
        description="End date to fetch data (included)."),
    locations: List[str | Stations] = fastapi.Query(
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
    start_date = datetime.datetime.strptime(start_date, DATEFORMAT)
    end_date = datetime.datetime.strptime(end_date, DATEFORMAT)
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
    logging.debug("Concatenating dataframes.", extra={"amount_df": len(all_data)})
    grouped = pd.concat(all_data, axis=0)
    result = grouped.to_dict(orient="records")
    return fastapi.responses.JSONResponse(content=result)
