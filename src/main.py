import os

import requests
import json
import time
from src.DatabaseManager import DatabaseManager
from src.GithubDownloader import GithubDownloader


if __name__ == '__main__':

    project_root = os.path.abspath(".")
    github_token = os.environ.get("GITHUB_TOKEN")
    database_user = os.environ.get("DATABASE_USER")
    database_password = os.environ.get("DATABASE_PASSWORD")

    database_manager = DatabaseManager(project_root, database_user, database_password)
    github_downloader = GithubDownloader(project_root, database_manager, github_token)

    github_downloader.update_repo_data()

