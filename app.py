# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 19:42:03 2022

@author: roman
"""

import os

import findspark
findspark.init()

from Funcoes.spark_handler import split_column, columns_to_snake, get_uniques_icao
from Funcoes.helper import exec_requests, split_dataframe
from Funcoes.database import DB

from pyspark.sql import SparkSession


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

USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')
HOST = os.environ.get('DBHOST')
PORT = '3306'
SCHEMA = 'PROJETO'

inst_db = DB(USER, PASSWORD, HOST, SCHEMA, PORT)

for df in split_dataframe(df_vra.toPandas(), 1000):
    print(df)
    dict_insert = {
        't_vra' : df
        }

    inst_db.insert_db(dict_insert)
    
inst_db.insert_db({'t_air_cia' : df_air_cia.toPandas()})
inst_db.insert_db({'t_airfields' : airfields_info})
