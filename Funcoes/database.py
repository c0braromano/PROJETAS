# -*- coding: utf-8 -*-
"""
Created on Sat Sep 24 17:53:37 2022

@author: victor.romano
"""

import mysql.connector

import pandas as pd
from datetime import date, timedelta


class DB:
    
    def __init__(self, user, password, host, db, port):
        self.CONFIG = {
            'user' : user,
            'password' : password,
            'host' : host,
            'database' : db,
            'port' : port
            }
        
        self.con = mysql.connector.connect(**self.CONFIG)
        self.cur = self.con.cursor()
        
    
    def insert_db(self, tabelas):
        def make_inter(numero_colunas):
            a = '('
            for i in range(0,numero_colunas):
                a += '%s'
                if i == (len(range(0,numero_colunas)) - 1):
                    a += ')'
                else:
                    a += ','
        
            return a

        for key, tabela in tabelas.items():
            query_data = []
            for index, row in tabela.iterrows():
                query = f'INSERT INTO {key} VALUES {make_inter(len(row.values))}'
                query_data.append(list(row.values))

            #for query_ in query_data:
            self.cur.executemany(query, query_data)
            self.con.commit()
