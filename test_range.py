import os
from datetime import datetime

from channels import parallel_download, channels, origins


def test_range1():
    since_date: str = datetime(2020, 7, 20).strftime("%Y-%m-%d")
    until_date = datetime(2020, 7, 20).strftime("%Y-%m-%d")

    current_directory = os.getcwd()
    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels,
        folder,
        origins,
        getAll=False,
        since_date=since_date,
        until_date=until_date,
    )


def test_range3():
    since_date: str = datetime(2024, 11, 1).strftime("%Y-%m-%d")
    until_date = datetime(2024, 11, 30).strftime("%Y-%m-%d")

    current_directory = os.getcwd()
    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels,
        folder,
        origins,
        getAll=False,
        since_date=since_date,
        until_date=until_date,
    )


def test_range2():
    since_date: str = datetime(2024, 10, 1).strftime("%Y-%m-%d")
    until_date = datetime(2024, 10, 31).strftime("%Y-%m-%d")

    current_directory = os.getcwd()
    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels,
        folder,
        origins,
        getAll=False,
        since_date=since_date,
        until_date=until_date,
    )
