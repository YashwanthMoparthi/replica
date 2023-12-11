# Project Setup Guide

This guide provides step-by-step instructions to set up your local environment for a project involving Snowflake and ServiceNow.

1. Create a Snowflake Account: Visit the [Snowflake website](https://www.snowflake.com/) and sign up for a Snowflake account.

2. Install Visual Studio Code (VSCode): Download and install Visual Studio Code from [here](https://code.visualstudio.com/download).

3. Install Snowflake Extension in VSCode:

   1. Open VSCode.
   2. Go to the Extensions view by clicking on the Extensions icon in the Activity Bar on the side of the window.
   3. Search for "Snowflake" in the Extensions view search box.
   4. Install the "Snowflake" extension provided by Snowflake Inc.
   5. Once installed, reload VSCode.

4. Login with Snowflake Account Credentials in VSCode

   1. Open VSCode.
   2. Navigate to the Snowflake extension.
   3. Click on the Snowflake icon in the sidebar.
   4. Enter your Snowflake account credentials to log in.

5. Install Anaconda: Download and install Anaconda from (https://www.anaconda.com/products/individual).

6. Navigate to the Project Folder with Feature Stores: Use the file option above in VSCode and click on open folder and select the appropriate folder available on your local machine.

7. Install Conda Environment

   In the terminal within VSCode, type the following command to install the Snowflake Snowpark Python package:

   ```bash
   conda create --file=./environment.yml
   ```
## Deployment
Deployment is configured using CI/CD. You can either deploy the code in 2 ways: Please view Additional Information or setup a git repo and commit/push to branches within the build_and_deployment.yaml file.

Note: The repo was built using GitHub's workflow jobs for CI/CD execution.

## Naming Convention
- Feature Domain Folders: FD_<domain_name>
- Feature Domain SQL Scripts: sp_fd_<domain_name>.sql
- Feature Domain Tables Store Procedure: SP_FD_<domain_name>_TABLES
- Feaute Domain Task Names: LOAD_FD_<domain_name>_<layer_begin>_into_<layer_end> (e.g., LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN)

## Additional Information
In order to deploy the code below, ensure there is a config file in the root directory with the necessary [connections.<environment>] tab. For an example config file, view below:
```text
[connections.dev]
accountname = <account_name>
username = <username>
password = <password>
dbname = <db_name>
rolename = <role_name>
schemaname = <schema_name>
warehousename = <warehouse_name>
```

To run and deploy SQL based Stored Procedures:

In the terminal run the following code by modifying the root directory:
```bash
python command_center_deploy.py "root directory path"
```

To run and deploy Python-based tasks or procedures:
In the terminal run the following code by modifying the root directory:
```bash
python "root_directory\path_app.py\app.py"
```
