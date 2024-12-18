import pytest_httpserver as mock
import axpo.server
import axpo.aemet.routes as routes
import axpo.aemet.scrapping as scrapping
import fastapi.testclient
import pathlib
import http
import datetime
import pytz
import urllib.parse
from typing import *

# TODO: We should also make a test for the edge cases such as the none cases etc...
# TODO: We have to add more test cases for the different aggregation methods.
# TODO: We have to add more test cases for the different timezones.
# TODO: We have to add more test cases for update.


def test_get_valid_data(httpserver: mock.HTTPServer, tmp_path: pathlib.Path):
    class TestCase(NamedTuple):
        location: str
        station_name_english: routes.Station
        json_data: List[scrapping.Data]
        expected_url_call: str
    # We use a mock server for tests.
    api_key = "MOCK_KEY"
    operator = scrapping.Scrapper(httpserver.url_for(
        "/"), api_key, tmp_path/"database.sqlite")
    start_date = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=pytz.UTC)
    end_date = datetime.datetime(2024, 1, 1, 0, 20, tzinfo=pytz.UTC)
    testcases: list[TestCase] = [
        TestCase("89064",
                 station_name_english="Meteo Station Juan Carlos I",
                 expected_url_call="/api/antartida/datos/fechaini/2024-01-01T00:00:00UTC/fechafin/2024-01-01T00:20:00UTC/estacion/89064",
                 json_data=[
                     {
                         "identificacion": "89064",
                         "nombre": "JCI Estacion meteorologica",
                         "fhora": "2024-01-01T00:00:00+0000",
                         "pres": 990.8,
                         "temp": 2.4,
                         "vel": 1.3,
                     }, {
                         "identificacion": "89064",
                         "nombre": "JCI Estacion meteorologica",
                         "fhora": "2024-01-01T00:10:00+0000",
                         "pres": 990.8,
                         "temp": 2.4,
                         "vel": 1.1,
                     }, {
                         "identificacion": "89064",
                         "nombre": "JCI Estacion meteorologica",
                         "fhora": "2024-01-01T00:20:00+0000",
                         "pres": 990.9,
                         "temp": 2.4,
                         "vel": 1.3,
                     }
                 ]),

        TestCase("89070", station_name_english="Meteo Station Gabriel de Castilla",
                 expected_url_call="/api/antartida/datos/fechaini/2024-01-01T00:00:00UTC/fechafin/2024-01-01T00:20:00UTC/estacion/89070",
                 json_data=[
                     {
                         "identificacion": "89070",
                         "nombre": "GdC Estacion meteorologica",
                         "fhora": "2024-01-01T00:00:00+0000",
                         "pres": 991.4,
                         "temp": 2.7,
                         "vel": 1.4,
                     }, {
                         "identificacion": "89070",
                         "nombre": "GdC Estacion meteorologica",
                         "fhora": "2024-01-01T00:10:00+0000",
                         "pres": 991.5,
                         "temp": 2.4,
                         "vel": 1.1,
                     }, {
                         "identificacion": "89070",
                         "nombre": "GdC Estacion meteorologica",
                         "fhora": "2024-01-01T00:20:00+0000",
                         "pres": 991.6,
                         "temp": 2.2,
                         "vel": 0.8,
                     }
                 ]),
    ]
    for index, x in enumerate(testcases):
        actual_json_endpoint = "/{}".format(index)
        # Serving the json of response
        httpserver.expect_request(
            x.expected_url_call,
            method="GET",
        ).respond_with_json(
            scrapping.AntarticaRequestResponse(
                datos=httpserver.url_for(actual_json_endpoint)
            )
        )
        # We should add the headers but that library seems to require it fully provided, and not partially with the api key.
        httpserver.expect_request(
            actual_json_endpoint,
            method="GET",
        ).respond_with_json(
            x.json_data
        )
    operator.update_data(start_date, end_date, ["89064", "89070" ])

    axpo.server.app.dependency_overrides[routes.scrapper] = lambda: operator
    with fastapi.testclient.TestClient(axpo.server.app) as client:

        endpoint = "{}/antartica?{}".format(routes.PREFIX, urllib.parse.urlencode(
            {
                "start_date": start_date.strftime(routes.DATEFORMAT),
                "end_date": end_date.strftime(routes.DATEFORMAT),
                "locations": [x.station_name_english for x in testcases],
                "aggregation_level": "hourly",
            }, True,
        ))

        result = client.get(endpoint)
        assert result.status_code == http.HTTPStatus.OK, "Invalid status code when querying {}. Error : {}".format(
            endpoint, result.content)
        # We do not compare the data directly as we want more readable error messages.
        j: List[scrapping.RenamedData] = result.json()
        case_0 = j[0]
        case_1 = j[1]
        EPSILON = 1e-1
        assert case_0["ts"] == "2024-01-01T01:00:00+01:00"
        assert abs(case_0["temperature"] - 2.4) < EPSILON
        assert abs(case_0["pressure"] - 99083.333) < EPSILON
        assert abs(case_0["velocity"] - 1.233) < EPSILON
        assert case_0["name"] == "Meteo Station Juan Carlos I"

        assert case_1["ts"] == "2024-01-01T01:00:00+01:00"
        assert abs(case_1["temperature"] - 2.433) < EPSILON
        assert abs(case_1["pressure"] - 99150) < EPSILON
        assert abs(case_1["velocity"] - 1.099) < EPSILON
        assert case_1["name"] == "Meteo Station Gabriel de Castilla"
