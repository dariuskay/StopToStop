# Setting Up

The repository code assumes that you have downloaded up-to-date GTFS static data for New York. You can access the data [here](https://transitfeeds.com/p/mta). Each borough has its own folder. The data is then agglomerated using `setup_static.py`. 

You must run 

`setup_static.py` 

using the filepath to the downloaded GTFS static feeds as an argument, e.g. 

`python setup_static.py ~/Downloads/`. 

The **Airflow** script relies on the creation of a *data* directory. Run 

`mkdir ~/data/` 

before you run `python airflow/dags/daily_spark_job.py`. Of course, the Airflow script needs the Airflow scheduler to be up and running.

Finally, the Spark scripts depend on your Spark master node being the HDFS namenode. The DNS address of this node is required in your **config** file. A config file is used in both Spark scripts as well as the Airflow script. 
