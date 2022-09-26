# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 20:49:43 2022

@author: roman
"""

import pyspark.sql.functions as f

from re import sub
from unidecode import unidecode

def split_column(df):
    """
    Divide a coluna "ICAO IATA".

    Parameters
    ----------
    df : spark dataframe
        Dataframe contendo coluna "ICAO IATA.

    Returns
    -------
    df : spark dataframe
        dataframe contendo colunas "ICAO" e "IATA", mas sem a coluna 
        "ICAO IATA".

    """
    split = f.split(df['ICAO IATA'], ' ')    
        
    df = df.withColumn('ICAO', split.getItem(0))
    df = df.withColumn('IATA', split.getItem(1))

    df = df.drop('ICAO IATA')
    
    
    return df

def columns_to_snake(df):
    """
    Renomeia colunas do dataframe para snake_case

    Parameters
    ----------
    df : Spark Dataframe
        qualquer dataframe do spark.

    Returns
    -------
    Spark Dataframe
        dataframe com colunas renomeadas em snake_case.

    """

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
    """
    Seleciona valores únicos de ICAO, tanto para aeroportos de origem como de 
    destino

    Parameters
    ----------
    df : Spark DataFrame
        dataframe contendo os voos da base de dados.

    Returns
    -------
    List
        Lista contendo valores únicos de ICAO.

    """
    
    df_uniques_destino = df.dropDuplicates(['icao_aerodromo_destino'])
    df_uniques_origem = df.dropDuplicates(['icao_aerodromo_origem'])

    lt_destino = [data[0] for data in df_uniques_destino.
                  select('icao_aerodromo_destino').collect()]

    lt_origem = [data[0] for data in df_uniques_origem.
                  select('icao_aerodromo_origem').collect()]

    lt_total = lt_destino + lt_origem

    
    return list(set(lt_total))
    