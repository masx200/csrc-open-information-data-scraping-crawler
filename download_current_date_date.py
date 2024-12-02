import os
from datetime import datetime

from channels import parallel_download, channels, origins

if __name__ == "__main__":
    since_date: str = datetime.now().strftime("%Y-%m-%d")
    until_date = datetime.now().strftime("%Y-%m-%d")

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
