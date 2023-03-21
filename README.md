this is the dbt code repository for silver and gold layer. 

### Models : 
 data models keep in this directory under respected folder 
 source is external tables on bronze layer

### Adding new model 
add models in dbt_project models section
then add required \<model\>.yml and \<model\>.sql file
#####  \<model\>.yml : consist schema and config
##### \<model\>.sql  : consist transformation from source to target 



### running the jobs 

#### compile : to check syntax locally 
#### debug   : to check connection 
#### test    : execute tests defined in configuration
#### run     : Perform  data load using run with various 


## set up for development env
```
# setting up virtual env pthon 
python3.9 -m venv dbt_databricks
source dbt_databricks/bin/activate

#install dbt dependency 

pip install -r requirements.txt
export DBT_TOKEN="XXXXXXXXXXXXXX-XX"
export DBT_ING_DATE = 20230212 
export DBT_WSENV = test 

#install dependency package 
dbt deps 

#refresh external table 
dbt run-operation stage_external_sources --profiles-dir ./profiles/databricks

# run the model 
dbt run --vars '{ ing_date : 20230212}' --profiles-dir ./profiles/databricks
```

### Resources:
