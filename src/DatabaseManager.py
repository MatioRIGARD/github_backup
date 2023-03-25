import os
import sqlite3


class DatabaseManager:

    def __init__(self, project_root, user: str, password: str):
        self.__project_root = project_root
        self.__user = user
        self.__password = password
        self.__repo_bool_fileds = [
            "private",
            "fork",
            "has_issues",
            "has_projects",
            "has_downloads",
            "has_wiki",
            "has_pages",
            "has_discussions",
            "archived",
            "disabled",
            "allow_forking",
            "is_template",
            "web_commit_signoff_required"
        ]
        self.__repo_stranger_key = [
            "owner",
            "license",
            "topics"
        ]
        self.__repo_datetime_key = [
            "created_at",
            "updated_at",
            "pushed_at"
        ]
        self.__repo_str_fields = [
            "node_id",
            "name",
            "full_name",
            "html_url",
            "description",
            "url",
            "forks_url",
            "keys_url",
            "collaborators_url",
            "teams_url",
            "hooks_url",
            "issue_events_url",
            "events_url",
            "assignees_url",
            "branches_url",
            "tags_url",
            "blobs_url",
            "git_tags_url",
            "git_refs_url",
            "trees_url",
            "statuses_url",
            "languages_url",
            "stargazers_url",
            "contributors_url",
            "subscribers_url",
            "subscription_url",
            "commits_url",
            "git_commits_url",
            "comments_url",
            "issue_comment_url",
            "contents_url",
            "compare_url",
            "merges_url",
            "archive_url",
            "downloads_url",
            "issues_url",
            "pulls_url",
            "milestones_url",
            "notifications_url",
            "labels_url",
            "releases_url",
            "deployments_url",
            "git_url",
            "ssh_url",
            "clone_url",
            "svn_url",
            "homepage",
            "language",
            "mirror_url",
            "visibility",
            "default_branch"
        ]
        self.__repo_float_fields = [
            "score"
        ]
        self.__repo_int_fields = [
            "id",
            "size",
            "stargazers_count",
            "watchers_count",
            "forks_count",
            "open_issues_count",
            "forks",
            "open_issues",
            "watchers",
        ]
        self.__owner_bool_fields = [
            "site_admin"
        ]
        self.__owner_int_fields = [
            "id"
        ]
        self.__owner_str_fields = [
            "login",
            "node_id",
            "avatar_url",
            "gravatar_id",
            "url",
            "html_url",
            "followers_url",
            "following_url",
            "gists_url",
            "starred_url",
            "subscriptions_url",
            "organizations_url",
            "repos_url",
            "events_url",
            "received_events_url",
            "type"
        ]

        self.__repo_special_fields = ["permissions"]
        self.__licence_str_fields = [
            "key",
            "name",
            "spdx_id",
            "url",
            "node_id"
        ]

        self.__database_path = os.path.abspath(os.path.join(self.__project_root, "database", "github_backup.db"))
        self.__database_connection = None
        self.__database_sql = None
        self.__database_generator_script_path = os.path.abspath(os.path.join(self.__project_root, "database",
                                                                             "mysql_workbench", "generated_script.sql"))

    def open_database_connection(self):
        db_exists = False
        db_dir_path = os.path.split(self.__database_path)[0]
        if os.path.exists(self.__database_path):
            db_exists = True
        if not os.path.exists(db_dir_path):
            os.mkdir(db_dir_path)
        self.__database_connection = sqlite3.connect(self.__database_path)
        self.__database_sql = self.__database_connection.cursor()

        if not db_exists:
            self.__execute_sql_script()

    def close_database_connection(self):
        self.__database_connection.close()

    def __execute_sql_script(self):
        if os.path.exists(self.__database_generator_script_path):
            f = open(self.__database_generator_script_path)
            script_content = f.read()
            f.close()

            new_content = ""
            for line in script_content.split("\n"):
                if line.strip().lower().find("set") != 0 and \
                        line.strip().lower().find("--") != 0 and \
                        line.strip().lower().find("create schema") != 0 and \
                        line.strip().lower().find("index") != 0 and \
                        line.strip().lower().find("use") != 0:
                    new_line = line.replace("`github_backup`.", "")
                    new_line = new_line.replace("ENGINE = InnoDB", "")
                    new_line = new_line.replace("AUTO_INCREMENT", "")
                    if line.strip().lower().find("comment") == 0:
                        new_line = ";"
                    new_content = new_content + new_line + "\n"

            print(new_content)
            splited_requests = new_content.split(";")

            for request in splited_requests:
                self.__database_sql.execute(request + ";")

        else:
            raise Exception("Error, sql generated script does not exists")

    def add_topics_from_data(self, json_data, repo_id):
        if json_data is not None:
            for topic in json_data:
                if not self.is_topic_with_id_exists(topic):
                    request = "INSERT INTO Topics ('name') VALUES(?)"
                    request_str = "INSERT INTO Topics ('name') VALUES(" + str(topic) + ")"
                    request_values = [topic]

                    print(request_str)
                    self.__database_sql.execute(request, request_values)
                    self.__database_connection.commit()

            for topic in json_data:
                if self.is_topic_with_id_exists(topic) and not self.is_topic_has_repo(topic, repo_id):
                    request = "INSERT INTO Topics_has_Repo ('Topics_name', 'Repo_idRepo') VALUES(?, ?)"
                    request_str = "INSERT INTO Topics_has_Repo ('Topics_name', 'Repo_idRepo') VALUES(" + str(topic) + ", " + str(repo_id) + ")"
                    request_values = [topic, repo_id]

                    print(request_str)
                    self.__database_sql.execute(request, request_values)
                    self.__database_connection.commit()

    def add_license_from_data(self, json_data):
        if json_data is not None:
            if not self.is_license_with_id_exists(json_data["key"]):
                request_keys = ""
                request_values = []
                request_values_str = ""
                request_values_question_mark = ""

                for key in json_data.keys():
                    if key in self.__licence_str_fields:
                        request_keys = request_keys + "'" + key + "', "
                        request_values.append(json_data[key])
                        request_values_str = request_values_str + "'" + str(json_data[key]) + "', "
                        request_values_question_mark = request_values_question_mark + "?, "

                request_keys = request_keys[:-2]
                request_values_question_mark = request_values_question_mark[:-2]
                request_values_str = request_values_str[:-2]

                request = "INSERT INTO License (" + str(request_keys) + ") VALUES(" + request_values_question_mark + ")"
                request_str = "INSERT INTO License (" + str(request_keys) + ") VALUES(" + str(request_values_str) + ")"

                print(request_str)
                self.__database_sql.execute(request, request_values)
                self.__database_connection.commit()

    def add_owner_from_data(self, json_data):
        if json_data is not None:
            if not self.is_owner_with_id_exists(json_data["id"]):
                request_keys = ""
                request_values = []
                request_values_str = ""
                request_values_question_mark = ""

                for key in json_data.keys():
                    if key in self.__owner_bool_fields:
                        bool_value = 1 if json_data[key] else 0
                        request_keys = request_keys + "'idOwner', "
                        request_values.append(json_data[key])
                        request_values_str = request_values_str + str(bool_value) + ", "
                        request_values_question_mark = request_values_question_mark + "?, "

                    elif key == "id":
                        request_keys = request_keys + "'idOwner', "
                        request_values.append(json_data["id"])
                        request_values_str = request_values_str + str(json_data[key]) + ", "
                        request_values_question_mark = request_values_question_mark + "?, "

                    elif key in self.__owner_str_fields:
                        request_keys = request_keys + "'" + key + "', "
                        request_values.append(json_data[key])
                        request_values_str = request_values_str + "'" + str(json_data[key]) + "', "
                        request_values_question_mark = request_values_question_mark + "?, "

                    elif key in self.__owner_int_fields:
                        request_keys = request_keys + "'" + key + "', "
                        request_values.append(json_data[key])
                        request_values_str = request_values_str + str(json_data[key]) + ", "
                        request_values_question_mark = request_values_question_mark + "?, "

                request_keys = request_keys[:-2]
                request_values_question_mark = request_values_question_mark[:-2]
                request_values_str = request_values_str[:-2]

                request = "INSERT INTO Owner (" + request_keys + ") VALUES(" + request_values_question_mark + ")"
                request_str = "INSERT INTO Owner (" + request_keys + ") VALUES(" + request_values_str + ")"

                self.__database_sql.execute(request, request_values)
                self.__database_connection.commit()
                print(request_str)

    def add_repo_from_data(self, json_data):
        self.open_database_connection()
        keys = json_data.keys()
        request_keys = ""
        request_values = []
        request_values_question_mark = ""
        request_values_str = ""

        for key in keys:
            if key in self.__repo_stranger_key:
                if key == "owner":
                    self.add_owner_from_data(json_data["owner"])
                    request_keys = request_keys + "'Owner_idOwner', "
                    request_values.append(json_data["owner"]["id"])
                    request_values_str = request_values_str + str(json_data["owner"]["id"]) + ", "
                    request_values_question_mark = request_values_question_mark + "?, "

                if key == "license":
                    # todo deal with None objects. Create a specific license for None
                    if json_data["license"] is not None:
                        self.add_license_from_data(json_data["license"])
                        request_keys = request_keys + "'License_key', "
                        request_values.append(json_data["license"]["key"])
                        request_values_str = request_values_str + str(json_data["license"]["key"]) + ", "
                        request_values_question_mark = request_values_question_mark + "?, "

                if key == "topics":
                    self.add_topics_from_data(json_data["topics"], json_data["id"])

            elif key in self.__repo_special_fields:
                if key == "permissions":
                    for permission_key in json_data["permissions"].keys():
                        request_keys = request_keys + "'permission_" + permission_key + "', "
                        request_values.append(json_data["permissions"][permission_key])
                        request_values_str = request_values_str + str(json_data["permissions"][permission_key]) + ", "
                        request_values_question_mark = request_values_question_mark + "?, "

            elif key in self.__repo_datetime_key:
                request_keys = request_keys + "'" + key + "', "
                request_values.append(json_data[key])
                request_values_str = request_values_str + "'" + str(json_data[key]) + "', "
                request_values_question_mark = request_values_question_mark + "?, "

            elif key in self.__repo_bool_fileds:
                bool_value = 1 if json_data[key] else 0
                request_keys = request_keys + "'" + key + "', "
                request_values.append(json_data[key])
                request_values_str = request_values_str + str(bool_value) + ", "
                request_values_question_mark = request_values_question_mark + "?, "

            elif key == "id":
                request_keys = request_keys + "'idRepo', "
                request_values.append(json_data[key])
                request_values_str = request_values_str + str(json_data[key]) + ", "
                request_values_question_mark = request_values_question_mark + "?, "

            elif key in self.__repo_int_fields:
                request_keys = request_keys + "'" + key + "', "
                request_values.append(json_data[key])
                request_values_str = request_values_str + str(json_data[key]) + ", "
                request_values_question_mark = request_values_question_mark + "?, "

            elif key in self.__repo_str_fields:
                request_keys = request_keys + "'" + key + "', "
                request_values.append(json_data[key])
                request_values_str = request_values_str + "\"" + str(json_data[key]).replace("\"", "\\\"") + "\", "
                request_values_question_mark = request_values_question_mark + "?, "

        # remove last ',' from request str
        request_keys = request_keys[:-2]
        request_values_question_mark = request_values_question_mark[:-2]
        request_values_str = request_values_str[:-2]

        request = "INSERT INTO Repo (" + request_keys + ") VALUES(" + request_values_question_mark + ")"
        request_str = "INSERT INTO Repo (" + request_keys + ") VALUES(" + request_values_str + ")"

        if not self.is_repo_with_id_exists(json_data["id"]):

            print(request_str)
            self.__database_sql.execute(request, request_values)
            self.__database_connection.commit()

        self.close_database_connection()

    def get_total_size(self):
        pass

    def get_repo_data(self, repo_id: int):
        return self.__database_sql.execute(f"SELECT * FROM Repo WHERE idRepo={repo_id}").fetchall()

    def is_repo_with_id_exists(self, repo_id):
        return self.__database_sql.execute(f"SELECT EXISTS(SELECT 1 FROM Repo WHERE idRepo={repo_id})").fetchall()[0][0] == 1

    def is_owner_with_id_exists(self, repo_id):
        return self.__database_sql.execute(f"SELECT EXISTS(SELECT 1 FROM Owner WHERE idOwner={repo_id})").fetchall()[0][0] == 1

    def is_topic_with_id_exists(self, repo_id):
        return self.__database_sql.execute(f"SELECT EXISTS(SELECT 1 FROM Topics WHERE name='{repo_id}')").fetchall()[0][0] == 1

    def is_license_with_id_exists(self, repo_id):
        return self.__database_sql.execute(f"SELECT EXISTS(SELECT 1 FROM License WHERE key='{repo_id}')").fetchall()[0][0] == 1

    # @todo
    def is_topic_has_repo(self, topic_name, repo_id):
        return self.__database_sql.execute(f"SELECT COUNT(*) FROM Topics_has_Repo WHERE Repo_idRepo = ? AND Topics_name = ?",
                                           (repo_id, topic_name)).fetchall()[0][0] > 0

    def clone_repo(self, repo_url):
        pass
