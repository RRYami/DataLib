"""TODO: refactor everything. This was used in another project and needs to be adapted properly."""

import json
import os
from datetime import datetime

import pendulum as p
import requests

from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


# TODO: Refactor to use ApiKeyProvider from extract_polygon.py
class AlphaVantageClient:
    """
    Responsible for managing the Alpha Vantage API client connection.

    Single Responsibility: Client initialization and lifecycle management.
    """

    def __init__(self):
        """
        Initialize Alpha Vantage REST client.

        Args:
            api_key: Alpha Vantage API key
        """
        self.api_key = ApiKeyProvider.get_api_key("ALPHA_VANTAGE_KEY")
        self.destination_folder = os.getenv("FUNDAMENTALS_DATA_PATH")
        logger.info("Alpha Vantage API client initialized")

    def get_api_key(self) -> str:
        """Return the API key."""
        return self.api_key


class FundamentalsDataExtractor:
    def __str__(self) -> str:
        return f"FundamentalsDataDownloader({self.statements!r})"

    def __repr__(self) -> str:
        return f"FundamentalsDataDownloader({self.statements!r})"

    def __init__(
        self, client: AlphaVantageClient, tickers: list[str], statements="ALL"
    ):
        self.statements = statements
        self.api_key = client.api_key
        self.destination_folder: str = client.destination_folder
        self.tickers = tickers
        if self.destination_folder is None:
            logger.error("FUNDAMENTALS_DATA_PATH not found in environment")
            raise ValueError("FUNDAMENTALS_DATA_PATH not found in environment")

    def get_financial_statement(self):
        if self.statements == "ALL":
            self.statements = [
                "INCOME_STATEMENT",
                "BALANCE_SHEET",
                "CASH_FLOW",
                "EARNINGS",
                "OVERVIEW",
            ]
            try:
                logger.info("Starting download of all financial statements")
                for j in self.tickers:
                    for i in self.statements:
                        logger.info(f"Downloading {i} for {j}")
                        url = (
                            "https://www.alphavantage.co/query?function="
                            + i
                            + "&symbol="
                            + j
                            + "&apikey="
                            + self.api_key
                        )
                        try:
                            r = requests.get(url)
                            r.raise_for_status()
                        except requests.exceptions.HTTPError as e:
                            print(e)
                        match i:
                            case "INCOME_STATEMENT":
                                with open(
                                    self.destination_folder
                                    + "\\Income Statement\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(
                                        f"Saved Income Statement for {j}"
                                    )
                            case "BALANCE_SHEET":
                                with open(
                                    self.destination_folder
                                    + "\\Balance Sheet\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Balance Sheet for {j}")
                            case "CASH_FLOW":
                                with open(
                                    self.destination_folder
                                    + "\\Cash Flow\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Cash Flow for {j}")
                            case "EARNINGS":
                                with open(
                                    self.destination_folder
                                    + "\\Earnings\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Earnings for {j}")
                            case "OVERVIEW":
                                with open(
                                    self.destination_folder
                                    + "\\Overview\\"
                                    + j
                                    + "_"
                                    + i
                                    + p.now("Europe/London").format(
                                        "YYYY-MM-DD"
                                    )
                                    + ".json",
                                    "w",
                                ) as outfile:
                                    outfile.write(
                                        json.dumps(r.json(), indent=4)
                                    )
                                    logger.info(f"Saved Overview for {j}")
                            case _:
                                logger.warning(
                                    f"Invalid statement or not added ({self.statements})"
                                )
            except Exception as e:
                logger.error(f"Error getting data: {e}")
        else:
            try:
                for i in self.tickers:
                    url = (
                        "https://www.alphavantage.co/query?function="
                        + str(self.statements)
                        + "&symbol="
                        + i
                        + "&apikey="
                        + self.api_key
                    )
                    r = requests.get(url)
                    match self.statements:
                        case "INCOME_STATEMENT":
                            with open(
                                self.destination_folder
                                + "\\Income Statement\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Income Statement for {i}")
                        case "BALANCE_SHEET":
                            with open(
                                self.destination_folder
                                + "\\Balance Sheet\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Balance Sheet for {i}")
                        case "CASH_FLOW":
                            with open(
                                self.destination_folder
                                + "\\Cash Flow\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Cash Flow for {i}")
                        case "EARNINGS":
                            with open(
                                self.destination_folder
                                + "\\Earnings\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Earnings for {i}")
                        case "OVERVIEW":
                            with open(
                                self.destination_folder
                                + "\\Overview\\"
                                + i
                                + "_"
                                + p.now("Europe/London").format("YYYY-MM-DD")
                                + ".json",
                                "w",
                            ) as outfile:
                                outfile.write(json.dumps(r.json(), indent=4))
                                logger.info(f"Saved Overview for {i}")
                        case _:
                            logger.warning(
                                f"Invalid statement or not added ({self.statements})"
                            )
            except Exception as e:
                logger.error(f"Error getting data: {e}")

    def get_list_of_reports_dates(self, file_path: str) -> list[datetime]:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"No file found at {file_path}: {e}")
        try:
            dates_list = []
            for i in range(len(data["quarterlyReports"])):
                dates_list.append(
                    data["quarterlyReports"][i]["fiscalDateEnding"]
                )
        except Exception as e:
            logger.error(f"Error parsing dates from JSON: {e}")
        try:
            datetime_list = []
            for j in dates_list:
                coverted_string = [datetime.strptime(j, "%Y-%m-%d")]
                datetime_list.append(coverted_string)
        except Exception as e:
            logger.error(f"Error converting strings to datetime: {e}")
        return datetime_list
