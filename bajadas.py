import argparse
import csv
import string
import os
import sys

class bajada:
	portalName = ''
	profileId = ''	 
	profileName = ''		
	metrics = ''
	dimensions = ''
	sort = ''

portalesRE = []
portalesJobs = []

portalesRE.append(dict(portalName='',profileId=''))
# portalesRE.append(dict(portalName='',profileId=''))
# portalesRE.append(dict(portalName='',profileId=''))
# portalesRE.append(dict(portalName='',profileId=''))
# portalesRE.append(dict(portalName='',profileId=''))
# portalesRE.append(dict(portalName='',profileId=''))
# portalesRE.append(dict(portalName='',profileId=''))

SessionesRE1 = dict(
	profileName='Sessiones',
	metrics='ga:sessions,ga:goal3Completions,ga:sessionDuration,ga:bounces,ga:searchSessions',
	dimensions='ga:dimension16,ga:source,ga:medium,ga:campaign,ga:channelGrouping,ga:landingPagePath',
	sort='ga:dimension16')

SessionesRE2 = dict(
	profileName='Sessiones1',
	metrics='ga:sessions',
	dimensions='ga:dimension16,ga:sessionCount,ga:deviceCategory,ga:userType,ga:browser,ga:hostname',
	sort='ga:dimension16')

PageviewsRE = dict(
	profileName='Pageviews',
	metrics='ga:pageviews,ga:timeOnPage,ga:entrances,ga:exits',
	dimensions='ga:dimension16,ga:dimension17,ga:pagePath,ga:hostname',
	sort='ga:dimension16,ga:dimension17')

AvisosRE = dict(
	profileName='Avisos',
	metrics='ga:pageviews',
	dimensions='ga:dimension16,ga:dimension17,ga:dimension9',
	sort='ga:dimension16,ga:dimension17')

EventosRE = dict(
	profileName='Eventos',
	metrics='ga:totalEvents',
	dimensions='ga:dimension16,ga:dimension17,ga:eventCategory,ga:eventAction,ga:eventLabel',
	sort='ga:dimension16,ga:dimension17')



tiposDeBajadas = ['Sessiones', 'Pageviews', 'Avisos', 'Eventos', 'Sessiones1']
bajadasRE = [SessionesRE1, SessionesRE2, PageviewsRE, AvisosRE, EventosRE]
bajadasJobs = []

todasLasBajadas = []

for bajada in bajadasRE:
	for portal in portalesRE:
		unaBajada = reduce(lambda x,y: dict(x, **y), (portal, bajada))
		todasLasBajadas.append(unaBajada)

for bajada in bajadasJobs:
	for portal in portalesJobs:
		unaBajada = reduce(lambda x,y: dict(x, **y), (portal, bajada))
		todasLasBajadas.append(unaBajada)










