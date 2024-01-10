from typing import Any
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
import subprocess
import configparser
import asyncio

#SQLAlchemy_utils method
# url = 'mssql+pymssql://cl:5203@DESKTOP-J5B6K6C/fps'
# engine = create_engine(url)
# print(engine.url)
# if not database_exists(engine.url):
#     create_database(engine.url)

#Create DB
Base = declarative_base()
class Auto_DB(Base):
    #Define table structure
    __tablename__ = 'FPS_result'
    cam_number = Column(Integer,nullable=False)
    FPS_result = Column(Float,nullable=False)
    Time = Column(DateTime,primary_key=True)   

    def get_db_info(self):
        self.config_par = configparser.ConfigParser()
        self.config_par.read('config.ini')
        self.dev_name = subprocess.check_output(['wmic','cpu','get','systemname']).split()[-1].decode()
        self.db_user = self.config_par['DBinfo']['db_acct']
        self.db_pass = self.config_par['DBinfo']['db_pw']
        return self.db_user,self.db_pass,self.dev_name
    def create_db(self):
        self.get_db_info()
        self.root_engine = create_engine('mssql+pymssql://{}:{}@{}'.format(self.db_user,self.db_pass,self.dev_name),connect_args={'autocommit':True})
        conn = self.root_engine.connect()
        try:
            conn.execute(text('CREATE DATABASE AutoFPSbenchmark'))
            self.code = 1
        except OperationalError:
            self.code = 0
        #Create table
        self.db_eng = create_engine('mssql+pymssql://{}:{}@{}/AutoFPSbenchmark'.format(self.db_user,self.db_pass,self.dev_name))
        if self.code:
            Base.metadata.create_all(self.db_eng)
        return self.code

class Modify_DB():       
    def insert_data(self,fps_data):
        self.db_user,self.db_pass,self.dev_name = Auto_DB.get_db_info(Auto_DB)
        self.db_eng = create_engine('mssql+pymssql://{}:{}@{}/AutoFPSbenchmark'.format(self.db_user,self.db_pass,self.dev_name))
        Session = sessionmaker(bind=self.db_eng)
        session = Session()
        session.add(fps_data)
        session.commit()
        session.close()

if __name__ == '__main__':
    Auto_DB.create_db()