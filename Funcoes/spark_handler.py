# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 20:49:43 2022

@author: roman
"""

import pyspark.sql.functions as f

from re import sub
from unidecode import unidecode

def split_column(df):
    split = f.split(df['ICAO IATA'], ' ')    
        
    df = df.withColumn('ICAO', split.getItem(0))
    df = df.withColumn('IATA', split.getItem(1))

    df = df.drop('ICAO IATA')
    
    
    return df

def columns_to_snake(df):

    def snake_case(string):
        string = unidecode(string)
        
        return '_'.join(
          sub('([A-Z][a-z]+)', r' \1',
          sub('([A-Z]+)', r' \1',
          string.replace('-', ' '))).split()).lower()

    for column in df.schema.names:
        df = df.withColumnRenamed(column, snake_case(column))
    
    
    return df

def get_uniques_icao(df):
    
    df_uniques_destino = df.dropDuplicates(['icao_aerodromo_destino'])
    df_uniques_origem = df.dropDuplicates(['icao_aerodromo_origem'])

    lt_destino = [data[0] for data in df_uniques_destino.
                  select('icao_aerodromo_destino').collect()]

    lt_origem = [data[0] for data in df_uniques_origem.
                  select('icao_aerodromo_origem').collect()]

    lt_total = lt_destino + lt_origem

    
    return list(set(lt_total))
    