# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 23:29:18 2022

@author: roman
"""

import requests
import pandas as pd

from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor


def exec_requests(icao_list):
    def get_info(icao):
        url = "https://airport-info.p.rapidapi.com/airport"
        
        headers = {
        	"X-RapidAPI-Key": "6996772c29msh27276f0a7a49501p1fc3c5jsnb2c0d4c05f1b",
        	"X-RapidAPI-Host": "airport-info.p.rapidapi.com"
        }
        
        params = {
            'icao' : icao
            }
        
        response = requests.request("GET", url, headers=headers, params=params)
        r_json = response.json()
        
        if 'error' in r_json:
            return params
        
        return r_json
    
    result_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_info, icao): icao for icao in icao_list}
        for future in as_completed(futures):
            result_list.append(future.result())

    df = pd.DataFrame(result_list)
    df = df.dropna()

    return df

def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks