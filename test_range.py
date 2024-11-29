import os
from datetime import datetime

from channels import parallel_download


def test_range1():
    since_date: str = datetime(2020, 7, 20).strftime("%Y-%m-%d")
    until_date = datetime(2020, 7, 20).strftime("%Y-%m-%d")
    channels = [
        "febe5cf9074b4ce6a52fd3d34d7a5cba",
        "55dbc14f9bea476bb09743d5f1c8c842",
        "c8318fc200764e38b30116c2d5f72b4b",
        "9ebc4198232e496e8bebf1b1bb1778ef",
    ]
    current_directory = os.getcwd()
    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels, folder, getAll=False, since_date=since_date, until_date=until_date
    )


def test_range3():
    since_date: str = datetime(2024, 11, 1).strftime("%Y-%m-%d")
    until_date = datetime(2024, 11, 30).strftime("%Y-%m-%d")
    channels = [
        "febe5cf9074b4ce6a52fd3d34d7a5cba",
        "55dbc14f9bea476bb09743d5f1c8c842",
        "c8318fc200764e38b30116c2d5f72b4b",
        "9ebc4198232e496e8bebf1b1bb1778ef",
    ]
    current_directory = os.getcwd()
    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels, folder, getAll=False, since_date=since_date, until_date=until_date
    )


def test_range2():
    since_date: str = datetime(2024, 10, 1).strftime("%Y-%m-%d")
    until_date = datetime(2024, 10, 31).strftime("%Y-%m-%d")
    channels = [
        "febe5cf9074b4ce6a52fd3d34d7a5cba",
        "55dbc14f9bea476bb09743d5f1c8c842",
        "c8318fc200764e38b30116c2d5f72b4b",
        "9ebc4198232e496e8bebf1b1bb1778ef",
    ]
    current_directory = os.getcwd()
    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels, folder, getAll=False, since_date=since_date, until_date=until_date
    )
