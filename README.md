# dat500-project

## Datasets
Links to the datasets are added to [this folder in Onedrive](https://liveuis-my.sharepoint.com/:f:/g/personal/250607_uis_no/EmgpwiQR3VJBrQYtKx51PuYBX83CDBQH2iOFH-1nes_6BQ?e=QKhGfT). **Note**: You must access the folder with a UiS account.

The files must put in **HDFS** such that the folder structure is equal to this: 

    ├── data
    │   ├── news
    │   ├── stocks
    │   ├── sectors.csv
    │   ├── companynames.csv

Run this command to put the files in HDFS:

    hadoop fs -put news stocks sectors.csv companynames.csv /data/

## Directory structure
All commands must be run from the project directory (same as the Makefile). Folder structure of project:

    ├── project
    ├   ├── src
    │   │   ├── spark
    │   │   │   ├── installation
    │   │   │   ├── main.py
    │   │   │   ├── sentiment_analysis.py
    │   │   │   ├── show.py
    │   │   │   ├── storage.py
    │   │   │   ├── transformations.py
    │   │   │   ├── udfs.py
    │   │   ├── hadoop
    │   │   │   ├── models
    │   │   │   │   ├── news_article.py
    │   │   │   │   ├── stock_result.py
    │   │   │   │   ├── stock.py
    │   │   │   ├── tools
    │   │   │   │   ├── console_printer.py
    │   │   │   │   ├── number_or_defaults.py
    │   │   │   ├── news_article_mp.py
    │   │   │   ├── stocks_mr_job.py
    ├   ├── conf
    │   │   ├── log4j2.properties
    │   │   ├── spark-defaults.conf
    │   │   ├── spark-env.sh
    │   │   ├── spark
    ├   ├── Makefile
    ├   ├── README.md
    └   └── requirements.txt

## Requirements
In order to install packages for sentiment analysis, MRjob and Delta run:

    make requirements
    
Replace the configuration files in `$SPARK_HOME$/conf/` with the files in `project/conf/`

## Run pipeline
Run the full pipeline with both Hadoop and Spark:

    make pipeline

If you encounter any errors:
- Make sure the file path to the python files are correct:
    - Check line 14 in `project/src/spark/main.py` and update the variable to point to the folder `project/src/spark/`. Do not use relative paths.
    - Check line 14 in `project/src/spark/sentiment_analysis.py` and update the variable to point to the folder `project/src/spark/`. Do not use relative paths.
- The command will output the results to the terminal, and thus it is necessary to keep a connection to the cluster open for the whole duration.