"""
The script deploys the modifiied SQL and Snowpark based store procedures to
the assigned snowflake workspace.

If the user wants to deploy all store procedures within the repo, then un-comment 
    # args.files_list = "ALL"

This script contains the following
functions:
    * check_store_procedures - checks modified files and verfies if 
        the file is a store procedure
    * deploy_changes - only deploys the modified store procedures
    * deploy_all - deploys all sql and snowpark store procedures
"""

import os
import sys

from argparse import ArgumentParser


def check_store_procedures(file_paths):
    """
    Method that takes in the file paths based on the modified
    files from a pull request. It returns the unique sql
    store procedure paths as a list and the snowpark store
    procedure paths as a list.

    Parameters
    ------------
    file_paths : str
        modified files in pull request

    Return
    ------------
    sql_sps : list
        string of unqiue sql store procedure paths
    python_sps : list
        string of unqiue snowpark store procedure paths
    """

    sql_sps = []
    python_sps = []
    for f in file_paths:
        if "tables.sql" in f or "pipeline_stage.sql" in f or "create.sql" in f:
            sql_sps.append(f)
        elif "Tasks" in f:
            python_sps.append(os.path.dirname(f))

    sql_sps = list(set(sql_sps))
    python_sps = list(set(python_sps))

    return sql_sps, python_sps


def deploy_changes(root_directory, sql_sps=None, python_sps=None):
    """
    Method that takes in the project directory and deploys
    only the modifieid sql and snowpark store procedures.

    Parameters
    ------------
    root_directory : str
        project directory
    """

    if sql_sps:
        for sp_path in sql_sps:
            path_file = os.path.join(root_directory, sp_path)
            os.system(f"snow login -c {root_directory}/config -C main")
            os.system(f"snow sql --filename={path_file}")

    if python_sps:
        for py_path in python_sps:
            path_file = os.path.join(root_directory, py_path)
            os.chdir(f"{path_file}")
            os.system(f"snow login -c {root_directory}/config -C main")
            os.system(f"snow procedure create")

            try:
                os.remove(os.path.join(py_path, "app.zip"))
                os.remove(os.path.join(py_path, "requirements.snowflake.txt"))
            except:
                continue
    try:
        os.remove(os.path.join(root_directory, "app.toml"))
    except:
        pass


def deploy_all(root_directory):
    """
    Method that takes in the project directory and deploys
    all sql and snowpark store procedures.

    Parameters
    ------------
    root_directory : str
        project directory
    """

    for directory_path, directory_names, file_names in os.walk(root_directory):
        # Get the last folder name in the directory path
        base_name = os.path.basename(directory_path)

        # If sql based store procedure is present then deploy to workspace
        files_in = [
            f
            for f in file_names
            if "tables.sql" in f or "pipeline_stage.sql" in f or "create.sql" in f
        ]
        if files_in:
            print(f"Found sql based store procedure in folder {directory_path}")
            for f in files_in:
                file_path = os.path.join(directory_path, f)
                os.chdir(f"{directory_path}")
                os.system(f"snow login -c {root_directory}/config -C main")
                os.system(f"snow sql --filename={file_path}")

        # If python based store procedure is present then deploy to workspace
        if "app.toml" not in file_names:
            continue
        else:
            print(f"Found python based store procedure in folder {directory_path}")
            os.chdir(f"{directory_path}")
            os.system(f"snow login -c {root_directory}/config -C main")
            os.system(f"snow procedure create")

            # Delete uneccessary files from project repo
            try:
                os.remove(os.path.join(directory_path, "app.zip"))
                os.remove(os.path.join(directory_path, "requirements.snowflake.txt"))
            except:
                continue
    try:
        os.remove(os.path.join(root_directory, "app.toml"))
    except:
        pass


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--root_directory",
        dest="root_directory",
        required=True,
        help="directory to deploy",
    )
    parser.add_argument(
        "-f",
        "--files_list",
        dest="files_list",
        help="list of files names to deploy",
    )
    args = parser.parse_args()

    # args.files_list = "ALL"

    # Deploying all or just the modified store procedures
    if args.files_list == "ALL":
        deploy_all(args.root_directory)
    else:
        files = str(args.files_list).split()
        sql_sps, python_sps = check_store_procedures(files)
        if sql_sps or python_sps:
            # There are SQL and/or Snowpark store procedures are present
            deploy_changes(args.root_directory, sql_sps, python_sps)
        else:
            print("No SQL or Snowpark store procedures modified in the changed/updated/created file(s)")
