# weekly-db-audits
The goal of this project is to create a python script that grabs all SQL files in a defined directory, runs them against a desired database, and sends the results out via email.  The script will be run weekly using Windows Task Scheduler, or however long is deemed appropriate.

As of now SQL files are not included in this repository.  This script serves as a shell for the user to set up the desired db connections and create queries to use as needed.

In addition to package installation from the requirements.txt file and creation of DB connection(s) and SQL files, the user will also need a valid email address, with appropriate info filled out in the emailinfo.csv.
