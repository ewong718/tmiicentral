'''
Models
Created on Apr 8, 2015

@author: edmundwong
'''

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
import keyring
import os

with open(os.path.dirname(__file__) + '/config.json') as f:
    cfg = json.load(f)["mysql"]
host = cfg["host"]
port = cfg["port"]
user = cfg["user"]
pw = keyring.get_password(cfg["pw_key_name"], cfg["pw_acct_name"])
c_engine = create_engine('mysql+pymysql://{user}:{pw}@{host}:{port}/{db}'.format(host=host, user=user,
                                                                                   pw=pw, db = cfg["calpendo_db"], port=port), echo=False)
r_engine = create_engine('mysql+pymysql://{user}:{pw}@{host}:{port}/{db}'.format(host=host, user=user,
                                                                                   pw=pw, db = cfg["rmc_db"], port=port), echo=False)
c_metadata = MetaData(bind=c_engine)
c_session = sessionmaker(bind=c_engine)

r_metadata = MetaData(bind=r_engine)
r_session = sessionmaker(bind=r_engine)

        
def db_connect(db):
    if db == "calpendo":
        s = c_session()
    elif db == "rmc":
        s = r_session()
    return s

        
class Ris(declarative_base()):                
    __table__ = Table('ris', r_metadata, autoload=True)
    
    
class Rates(declarative_base()):
    __table__ = Table('rates', r_metadata, autoload=True)
    
    
class Project_basics(declarative_base()):
    __table__ = Table('project_basics', r_metadata, autoload=True)
    