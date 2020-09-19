# Do all imports and installs here
import pandas as pd
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def create_spark_session():

    '''
    - Create spark session 
    - Return the session builder stored in 'spark'
    '''
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark


def process_cities_demographics_data(spark):
    '''
    - Read "us-cities-demographics.csv" as spark dataframes, delimetered by ';'
    - Select the required columns for cities_demographics_table
    - create cities_demographics template
        # write time table to parquet files partitioned by year and month

    '''
    global cities_demographics_table
    
    df_spark = spark.read.csv("us-cities-demographics.csv",sep=";", inferSchema=True, header=True)
    
        #  ---------cities_demographics Table ------------

    cities_demographics_table = df_spark.select('City', 'State Code', 'State','Median Age', \
                                     'Male Population', 'Female Population', 'Total Population'\
                                      , 'Number of Veterans', 'Foreign-born', 'Race')\
                               .withColumnRenamed('State Code', 'State_Code')\
                               .withColumnRenamed('Median Age', 'Median_Age')\
                               .withColumnRenamed('Male Population', 'Male_Population')\
                               .withColumnRenamed('Female Population', 'Female_Population')\
                               .withColumnRenamed('Total Population', 'Total_Population')\
                               .withColumnRenamed('Number of Veterans', 'Number_of_Veterans')\
                               .withColumn('City', upper(col('City')))\
                               .dropDuplicates()
    
    cities_demographics= cities_demographics_table.createOrReplaceTempView("cities_demographics")
    
def process_temprature_data(spark):
    '''
    - Read '../../data2/GlobalLandTemperaturesByCity.csv' as spark dataframes
    - Filter 'United States' rows to get the temprature data of the US cities  
    - Select the required columns for US_weather_table
    - create US_weather template
        # write time table to parquet files partitioned by year and month

    '''
    
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    df_spark = spark.read.csv(fname, inferSchema=True, header=True)
    
    US_weather = df_spark.filter(df_spark.Country == "United States")
    
        #  ---------US_weather dimension table ------------
    
    US_weather_table = US_weather.select('dt', 'AverageTemperature','City', 'Latitude', 'Longitude')\
                       .withColumn("dt",to_date(col("dt")))\
                       .withColumn('City', upper(col('City')))\
                       .dropDuplicates()
    
    US_weather = US_weather_table.createOrReplaceTempView("US_weather")

def process_immigration_data(spark):
    '''
    - Read '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat' as spark dataframes
    - clean data provided as dictionary in 'i94port.txt' & 'i94res' to get the values of encoded data
    - Transform the values of 'i94port' and 'i94res' by provided dictionaries
    - Select the required columns for i94Card_table and create i94Card template
    - Select the required columns for immigrants_table and create immigrants template
    - extract columns from joined immigratio, us-cities-demographic, and weather datasets to create immigration fact table 

     # write time table to parquet files partitioned by year and month
     # write time table to parquet files partitioned by year and month
     # write time table to parquet files partitioned by year and month

    '''   
    global immigration_fact_table, immigration, i94Card_table, immigrants_table

     df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
     fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
     df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
    
    
   #--------Start cleaning process to get the values of encoded data in the dictionary----------
    #i94port
    with open('i94port.txt', 'r') as i94portData:
        i94portData = i94portData.readlines()
    #i94cty
    with open('i94res.txt', 'r') as i94resData:
        i94resData = i94resData.readlines()
        
    #i94port
    i94portData = {i.translate(str.maketrans('\t",', '   ')) for i in i94portData}
    i94portData = {i.translate(str.maketrans("'", " ")) for i in i94portData}

    #i94res
    i94resData = {i.translate(str.maketrans(",'", '  ')) for i in i94resData} 
   
    #i94port splitting process
    i94portData = {k.split('=')[0].strip():k.split('=')[1].strip() for k in i94portData} 

    #i94res splitting process
    i94resData = {k.split('=')[0].strip():k.split('=')[1].strip() for k in i94resData} 
    
    #-------------------End cleaning and spliting data process---------------------
    
    #---------Start transforming the values of 'i94port' and 'i94res' by provided dictionaries------------ 

    #i94port
    i94port= df['i94port'].values.tolist()
    ports = []
    for i in i94port:
        ports.append(i94portData[i])

    # convert 'i94res' data type to fit the values of the values in the provided data dictionary in 'i94res.txt'
    df['i94res'] = df['i94res'].astype(int)
    
    #i94cty
    i94res= df['i94res'].values.tolist()
    i94res = [str(item) for item in i94res]
    
    ctyRes = []
    for i in i94res:
        ctyRes.append(i94resData[i])
        
    df['i94port'] = ports
    df['i94res'] = ctyRes
    
    i94DF = df.loc[:, ['cicid','i94port', 'i94res']]
    i94DF = i94DF.rename(columns = {'i94port': 'port', 'i94res': 'original_residancy'})
    
    i94dfSpark = spark.createDataFrame(i94DF)
    
    # append the transformed data into immigration log dataframes
    immigration = df_spark.withColumn("arrdate", to_timestamp(col("arrdate")))\
                              .withColumn("arrdate",to_date(col("arrdate")))\
                              .withColumnRenamed('arrdate', 'arrival_date')\
                              .join(i94dfSpark, "cicid", "inner")
    immigration_log = immigration.createOrReplaceTempView("immigration_log")

    
        #---------- End transforming process---------
      
    
    #  ---------i94_card dimension table ------------
    
    i94Card_table = immigration.select('cicid','i94yr', 'i94mon', 'depdate', 'i94visa', \
                          'visatype', 'admnum', 'dtaddto')\
                          .withColumnRenamed('cicid', 'immigration_id')\
                          .withColumn("depdate", to_timestamp(col("depdate")))\
                          .withColumn("depdate",to_date(col("depdate")))\
                          .withColumn("dtaddto", to_timestamp(col("dtaddto")))\
                          .withColumn("dtaddto",to_date(col("dtaddto")))\
                          .withColumnRenamed('i94yr', 'i94year')\
                          .withColumnRenamed('i94mon', 'i94month')\
                          .withColumnRenamed('depdate', 'depature_date')\
                          .withColumnRenamed('admnum', 'admission_number')\
                          .withColumnRenamed('dtaddto', 'admission_date')\
                          .dropDuplicates()\
                          .withColumn("i94card_id", monotonically_increasing_id()) 
    
    i94Card = i94Card_table.createOrReplaceTempView("i94Card")
    
   # ---------immigrants dimension table ------------

    immigrants_table = immigration.select('cicid', 'biryear', 'gender', 'occup','i94visa', 'visatype'\
                                        , 'original_residancy')\
                                    .withColumnRenamed('cicid', 'immigration_id')\
                                    .withColumnRenamed('biryear', 'birth_year')\
                                    .withColumnRenamed('occup', 'occupation')\
                                    .dropDuplicates()\
                                    .withColumn("immigrant_id", monotonically_increasing_id())
        
    immigrants = immigrants_table.createOrReplaceTempView("immigrants")
    
        
   # ---------immigration fact table ------------

    immigration_fact_table = spark.sql('''
              SELECT  
              DISTINCT
              immigration_log.cicid AS immigration_id,
              immigration_log.arrival_date,
              immigration_log.i94mode AS immigration_model,
              immigration_log.port,
              cities_demographics.City,
              immigration_log.i94addr AS residancy_state,
              i94Card.i94card_id,
              immigrants.immigrant_id,
              US_weather.AverageTemperature,
              US_weather.Latitude,
              US_weather.Longitude
              FROM  
                  immigration_log JOIN i94Card ON immigration_log.cicid == i94Card.immigration_id
                  JOIN immigrants ON immigration_log.cicid == immigrants.immigration_id
                  LEFT JOIN cities_demographics ON immigration_log.port LIKE '%' || cities_demographics.City || '%'
                  LEFT JOIN US_weather ON (US_weather.dt == immigration_log.arrival_date AND 
                  US_weather.City == cities_demographics.City)
                   ''')
    immigration_fact = immigration_fact_table.createOrReplaceTempView("immigration_fact")
     
    #immigration_fact_table.write.partitionBy('residancy_state','City').parquet('immigration_fact/')

    
def   check_data_quality():
    '''
    1. check the uniqueness of immigration_id in immigration_table, i94Card_table, and immigrants_table
    2. check missing values of immigration_id in immigration_table, i94Card_table, and immigrants_table
    3. check that all immigration_ids are transformed appropriatly from immigration_log
    '''
    print("\n---------- Data Quality Checks ----------")
    
#check the uniqueness of immigration_id in immigration_fact_table, i94Card_table, and immigrants_table
    if (immigration_fact_table.select('immigration_id').count() != immigration_fact_table.select('immigration_id').dropDuplicates().count()):
        print("immigration_id at immigration_fact_table are duplicated!")
    else:
         print("immigration_id at immigration_table are unique!")
        
    if (i94Card_table.select('immigration_id').count() != i94Card_table.select('immigration_id').dropDuplicates().count()):
        print("immigration_id at i94Card_table are duplicated!")
    else:
        print("immigration_id at i94Card_table are unique!")
        
    if (immigrants_table.select('immigration_id').count() != immigrants_table.select('immigration_id').dropDuplicates().count()):
        print("immigration_id at immigrants_table are duplicated!")
    else:
        print("immigration_id at immigrants_table are unique!")
        

        
#check missing values of immigration_id in immigration_table, i94Card_table, and immigrants_table
   
    print("The NULL values at immigration_id column of immigration_table are shown below!")
    print(immigration_fact_table.select(count(when(col('immigration_id').isNull() , immigration_fact_table.immigration_id))).show())
        
    print("The NULL values at immigration_id column of i94Card_table are shown below!")
    print(i94Card_table.select(count(when(col('immigration_id').isNull() , i94Card_table.immigration_id))).show())
        
   
    print("The NULL values at immigration_id column of immigrants_table are shown below!")
    print(immigrants_table.select(count(when(col('immigration_id').isNull() , immigrants_table.immigration_id))).show())
        
        
#check that all immigration_ids are transformed appropriatly from immigration_log
    if((i94Card_table.select('immigration_id').count() < 1) or (immigration.select('cicid').count() != i94Card_table.select('immigration_id').count())):
        print(str(immigration.select('cicid').count() - i94Card_table.select('immigration_id').count()) + " rows at i94Card_table did not transformed appropriatly from immigration log!")
        
    if((immigrants_table.select('immigration_id').count() < 1) or (immigration.select('cicid').count() != immigrants_table.select('immigration_id').count())):
        print(str(immigration.select('cicid').count() - immigrants_table.select('immigration_id').count()) +" rows at i94Card_table did not transformed appropriatly from immigration log!")

    if((immigration_fact_table.select('immigration_id').count() < 1) or (immigration.select('cicid').count() != immigration_fact_table.select('immigration_id').count())):
        print(str(immigration.select('cicid').count() - i94Card_table.select('immigration_id').count()) + " rows at i94Card_table did not transformed appropriatly from immigration log!")
        
    print("\n----------Done!----------")

def main():
    
    spark = create_spark_session()
    process_cities_demographics_data(spark)
    process_temprature_data(spark)
    process_immigration_data(spark)
    check_data_quality()


if __name__ == "__main__":
    main()
