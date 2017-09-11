import os
import csv
import sys
import time
import string
import random
import logging
import datetime
import argparse
import threading
import pandas as pd
#Conexion con AWS
#import boto3
#Datos de las bajadas
from bajadas import todasLasBajadas
from bajadas import tiposDeBajadas
from bajadas import portalesRE
#Conexion con Google Api
from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.service_account import ServiceAccountCredentials

def is_non_zero_file(fpath):  
    return True if os.path.isfile(fpath) and os.path.getsize(fpath) > 0 else False

def createDir(path):
	try:
   		os.stat(path)
	except:
	    os.mkdir(path)
	return path

def get_script_path():
	return os.path.dirname(os.path.realpath(sys.argv[0]))

script_path = get_script_path()

BajadasParciales = createDir(script_path+'/BajadasParciales/')
Logs = createDir(script_path+'/Logs/')
BajadasFinales = createDir(script_path+'/BajadasFinales/')


scopes = ['https://www.googleapis.com/auth/analytics.readonly']
 
json_data = {
  "type": "service_account",
  "project_id": "ga-reporting-",
  "private_key_id": "",
  "private_key": "",
  "client_email": "",
  "client_id": "",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "",
  "client_x509_cert_url": ""
}

credentials = ServiceAccountCredentials._from_parsed_json_keyfile(keyfile_dict=json_data, scopes=scopes)

class SampledDataError(Exception): pass
date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('20%y-%m-%d')
date_ranges = [(date, date)]


def main(argv, metrics, dimensions, sort, profileId, profileName, writer, logger):
	service, flags = sample_tools.init(argv, 'analytics', 'v3', __doc__, __file__, scope='https://www.googleapis.com/auth/analytics.readonly')
	for n in range(0, 6):
		try:
			if not profileId:
				logger.log(50, 'Could not find a valid profile for this user.')
			else:
				for start_date, end_date in date_ranges:
					limit = ga_query(service, profileId, 0,start_date, end_date, metrics, dimensions, sort).get('totalResults')
					for pag_index in range(0, limit, 10000):
						results = ga_query(service, profileId, pag_index,start_date, end_date, metrics, dimensions, sort)
						if results.get('containsSampledData'):
							raise SampledDataError
						print_results(results, pag_index, start_date, end_date, writer, logger)
				return None

		except TypeError as error:
			logger.log(50, 'There was an error in constructing your query : %s' % error)

		except HttpError as error:
			if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded','internalServerError', 'backendError']:
				time.sleep((2 ** n) + random.random())
			else:
				logger.log(50, 'Arg, there was an API error : %s : %s' %(error.resp.status, error._get_reason()))
				break

		except SampledDataError:
			logger.log(50, 'Error: Query contains sampled data!')

def print_results(results, pag_index, start_date, end_date, writer, logger):
	if pag_index == 0:
		if (start_date, end_date) == date_ranges[0]:
			logger.log(50, 'Profile Name: %s' % results.get('profileInfo').get('profileName'))
			columnHeaders = results.get('columnHeaders')
			cleanHeaders = [str(h['name']) for h in columnHeaders]
			writer.writerow(cleanHeaders)
		logger.log(50, 'Now pulling data from %s to %s.' %(start_date, end_date))

	if results.get('rows', []):
		for row in results.get('rows'):
			for i in range(len(row)):
				old, new = row[i], str()
				for s in old:
					new += s if s in string.printable else ''
				row[i] = new.replace(',','_')
				row[i].encode("utf-8")
			writer.writerow(row)
	else:
		logger.log(50, 'No Rows Found')
	limit = results.get('totalResults')
	logger.log(50, str(pag_index) + ' of about ' + str(int(round(limit, -4))) + 'rows.')
	return None

def ga_query(service, profile_id, pag_index, start_date, end_date, metrics, dimensions, sort):
	return service.data().ga().get(
		ids='ga:' + profile_id,
		start_date=start_date,
		end_date=end_date,
		metrics=metrics,
		dimensions=dimensions,
		sort=sort,
		samplingLevel='HIGHER_PRECISION',
		start_index=str(pag_index+1),
		max_results=str(pag_index+10000)).execute()

def query(portal, profileName, profileId, dimensions, metrics, sort):
	logger = logging.getLogger(profileName+'-'+portal)
	logger.setLevel(logging.CRITICAL)
	file_handler = logging.FileHandler( Logs + '/log-'+profileName+'-'+date+'-'+portal+'.log')
	formatter = logging.Formatter('(%(threadName)-10s) %(message)s')
	file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)
	filename = profileName + '_' + date + '-' + portal +'.csv'
	with open(BajadasParciales + '/' + filename, 'wt') as f:
		writer = csv.writer(f, lineterminator='\n')
		if __name__ == '__main__': 
			main(sys.argv, metrics, dimensions, sort, profileId, profileName, writer, logger)
	logger.log(50, "Profile done.")

threads = []
for x in todasLasBajadas:
	t = threading.Thread(target=query, args=(x['portalName'], x['profileName'], x['profileId'], x['dimensions'], x['metrics'], x['sort']), name=x['profileName'] + '-' + x['portalName'])
	threads.append(t)

aux = threads

for y in range(1,6):
	aux[y].start()
	print('inicio'+str(aux[y]))
	del aux[y]

while not aux == []:
	if len(threading.enumerate())<5:
		aux[0].start()
		print('inicio'+str(aux[0]))
		del aux[0]

for x in threads:
	x.join()

print("All profiles finished")

fallidos = []
for tipo in tiposDeBajadas:
	framesArchivos = []
	for portal in portalesRE:
		pathCSV = BajadasParciales + '/' + tipo + '_' + date + '-' + portal['portalName'] + '.csv'
		if is_non_zero_file(pathCSV):
			a = pd.read_csv(pathCSV, low_memory=False, header=0)
			framesArchivos.append(a)
		else:
			print("Fallo:" + pathCSV)
			fallidos.append(pathCSV)
	final = pd.concat(framesArchivos, axis=0)
	final.to_csv(BajadasFinales + '/' + tipo + '-' + date + '.csv', index=False)

filenames = []
for file in os.listdir(Logs):
	filenames.append(Logs+'/'+file)

with open(BajadasFinales+'/log-'+date+'.log', 'w') as outfile:
	for fname in filenames:
		with open(fname) as infile:
			outfile.write(infile.read())






