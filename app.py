# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 19:42:03 2022

@author: roman
"""

import findspark
findspark.init()

from Funcoes.spark_handler import split_column, columns_to_snake, get_uniques_icao
from Funcoes.helper import exec_requests

from pyspark.sql import SparkSession, Row
from pyspark import SparkContext


spark = SparkSession.builder.master('local[2]').appName('sparkdf').getOrCreate()

df_air_cia = spark.read.options(delimiter=';', header=True) \
            .csv('Dados/AIR_CIA')

df_air_cia = split_column(df_air_cia)
df_air_cia = columns_to_snake(df_air_cia)

df_vra = spark.read.json("Dados/VRA/*.json")
df_vra = columns_to_snake(df_vra)

icao = get_uniques_icao(df_vra)

airfields_info = exec_requests(icao)
df_airfields = spark.createDataFrame(airfields_info)

DB_URL = 'projetas.c65br71y0aqc.us-east-1.rds.amazonaws.com'
DRIVER = 'com.mysql.jdbc.Driver'
USER='admin'
PASSWORD='romano123'


df_air_cia.write.format('jdbc').options(
    url=DB_URL,
    driver=DRIVER,
    dbtable='T_AIR_CIA',
    user=USER,
    password=PASSWORD).mode('append').save()