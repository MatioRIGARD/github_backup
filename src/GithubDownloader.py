import os
import requests
import json
import time


class GithubDownloader:

    def __init__(self, project_root, database_manager, github_token):
        self.__project_root = project_root
        self.__database_manager = database_manager
        self.__github_token = github_token

    def update_repo_data(self):
        headers = {"Authorization": f"token {self.__github_token}"}

        # Paramètres de recherche
        min_stars = 250
        max_stars = 100000000
        per_page = 100
        page = 1
        data = []
        file_count = 0

        while True:
            query = f"stars:100..{max_stars}"
            url = f"https://api.github.com/search/repositories?q={query}&per_page={per_page}&page={page}"
            response = requests.get(url, headers=headers)
            json_data = json.loads(response.text)
            data = data + json_data["items"]

            if response.status_code == 403:
                reset_time = int(response.headers.get("X-RateLimit-Reset"))
                sleep_time = reset_time - time.time() + 5
                print(f"API rate limit exceeded. Sleeping for {sleep_time:.0f} seconds...")
                time.sleep(sleep_time)
                continue
            elif page == 10:
                if len(json_data["items"]) > 1:
                    max_stars = json_data["items"][-1]["stargazers_count"]
                    page = 0
                    file_count = file_count + 1
                    backup_file = os.path.join(self.__project_root, f"github_top_repos/repositories_page{str(file_count)}.json")
                    if not os.path.exists(backup_file):
                        f = open(backup_file, "w+")
                        f.write(json.dumps(data, indent=2))
                        f.close()
                    data = []

            if json_data["items"][0]["stargazers_count"] == min_stars-1:
                break

            if response.status_code != 200:
                print(f"Error: {response.status_code}")

            page += 1
