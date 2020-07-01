#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import sys
import getopt
from sqlalchemy import create_engine

if __name__ == "__main__":

	unixOptions = "s:e:"
	gnuOptions = ["start_dt=", "end_dt="]
	
	fullCmdArguments = sys.argv
	argumentList = fullCmdArguments[1:] #excluding script name

	try:
		arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
	except getopt.error as err:
		print (str(err))
		sys.exit(2)

	start_dt = ''
	end_dt = ''
	for currentArgument, currentValue in arguments:
		if currentArgument in ("-s", "--start_dt"):
			start_dt = currentValue
		elif currentArgument in ("-e", "--end_dt"):
			end_dt = currentValue

	db_config = {'user': 'my_user',
                 'pwd': 'my_user_password',
                 'host': 'localhost',
                 'port': 5432,
                 'db': 'zen'}   

	connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                             db_config['pwd'],
                                                             db_config['host'],
                                                             db_config['port'],
                                                             db_config['db'])                 

	#запрашиваем сырые данные
	engine = create_engine(connection_string)    

	query = '''
                SELECT *, TO_TIMESTAMP(ts/1000) AT TIME ZONE 'Etc/UTC' AS dt FROM log_raw
                WHERE TO_TIMESTAMP(ts/1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP;
            '''.format(start_dt, end_dt)

	raw = pd.io.sql.read_sql(query, con = engine)
    #raw.to_csv('C:\\log_raw.csv', encoding='utf-8', index=False)
	raw['dt'] = pd.to_datetime(raw['dt']).dt.floor('min')
	dash_visits = (raw
		.groupby(['item_topic','source_topic','age_segment','dt'])
		.agg({'event':'count'})
		.rename(columns={'event':'visits'})
		.reset_index()
				)
	dash_engagement = (raw
		.groupby(['item_topic','event','age_segment','dt'])
		.agg({'user_id':'nunique'})
		.reset_index()
		.rename(columns={'user_id':'unique_users'})
				)
	tables = ['dash_visits','dash_engagement']
	for table in tables:
		query = '''
			DELETE FROM {} WHERE dt BETWEEN '{}'::TIMESTAMP
			AND '{}'::TIMESTAMP;
			'''.format(table,start_dt,end_dt)
		engine.execute(query)

	dash_visits.to_sql(name = 'dash_visits', con = engine, if_exists = 'append', index = False)
	dash_engagement.to_sql(name = 'dash_engagement', con = engine, if_exists = 'append', index = False)
