import json
#import simplejson as json
from pprint import pprint
import re
import math
import csv
from datetime import timedelta
from datetime import datetime
import requests
from pymongo import MongoClient

# Set a search phrase
# If empty, the tool will download all companies
#search_phrase = "'big data'"
search_phrase = 'search engine marketing seo sem'
#search_phrase = ""

# Set other settings
fundedDateLimit = timedelta(days=365) # i.e. within 1 year
acquiredDateLimit = timedelta(days=365) # i.e. over a year ago
SEARCH_BASE = 'http://api.crunchbase.com/v/1/search.js?'
RETRIEVE_BASE = 'http://api.crunchbase.com/v/1/company/'
ALL_COMPANIES_ENDPOINT = 'http://api.crunchbase.com/v/1/companies.js'
outfile = r'JSON Parser output.csv'
keyfile = r'api_key.txt'

# Connect to MongoDB
cbase = MongoClient().cbase.crunchbase_db

# Pull the API key from a file
f = open(keyfile)
MasheryKey = f.read().lstrip().rstrip()
f.close()

# Pull the HQ types from a file
HQ_types = []
file_hq_types = open('HQ_types.txt', 'r+')
for line in file_hq_types.readlines():
	HQ_types.append(line.rstrip().lstrip())

# Initialize values
total = 0
results = 10
start = 1

def search_with_query(api_key, query, results, start, **kwargs):
	kwargs.update({
		'query': query,
	        'page': start,
	        'api_key': api_key
	})

	url = SEARCH_BASE
	# print url

	result = ""
	try:
		request = requests.get(url, params=kwargs)
	        result = request.json()
	except ValueError:
                # Skip any pages where the HTML generates error
	        print "ValueError in search"
	        return dict('')
	except AttributeError:
		print "AttributeError. request code =", result
		return dict('')

	if request.status_code != 200:
		raise Exception('Request status code is not 200')

	return result


def search(api_key, query, results, start, **kwargs):
	permalinks = []

	if (query <> ""):
		result = search_with_query(MasheryKey, search_phrase, 10, 1)
		total = result["total"]
		iter = int(math.ceil(total/10))
		print "Will iterate " + str(iter) + " times"

		for i in range(iter):
		    i += 1

		    # Iterate i times, where i is pages in the search results
		    print "Page " + str(i)
		    j = search_with_query(MasheryKey, search_phrase, 10, i)

		    for k in j.keys():
		        if (k=="results"):

		            for r in j[k]:
		                n = re.search("u'namespace': u'(.*?)'", str(r))
		                # Only match companies, not products or people
                		if n.group(1)=="company":
		                        p = re.search("u'permalink': u'(.*?)',", str(r))
		                        if p is not None:
		                            permalinks.append(p.group(1))

	else:
		p = {'api_key': api_key}
		r = requests.get(ALL_COMPANIES_ENDPOINT, params=p)
		links = r.json()

		for link in links:
			permalinks.append(link['permalink'])

	return permalinks


def retrieve(api_key, company, **kwargs):
        url = RETRIEVE_BASE + company + ".js?" + "api_key=" + api_key

        try:
                returnObject = requests.get(url)
                result = returnObject.json()
        except ValueError:
                # Skip any retreivals
                print "ValueError in retrieval"
                return {"error" : "error"}
	except IOError:
		print "IOError in retrieval"
                return {"error" : "error"}

        return result

permalinks = search(MasheryKey, search_phrase, 10, 1)
print "Ready to process: " , len(permalinks), " entries"

# Uncomment for testing purposes
#permalinks = []
#permalinks.insert(0,"gatim-language-services")


# Process each permalink separately
for page in permalinks:
	if (cbase.find( {"permalink": page} ).count() != 0):
		# Already exists in Mongo: skip processing
		continue

        #print "Processing permalink: " + page
        l = retrieve(MasheryKey, page)

        for k in l.keys():
                #print "Now processing " + k

                if (k=="error"):
                        print "ERROR at " + l[k]

                if (k=="name"):
                        enc = ""

                        if (l[k] is not None):
                                enc = l[k].encode('ascii', 'ignore')
                        names = enc

                if (k=="homepage_url"):
                        homepages = l[k]

                if (k=="founded_year"):
                        founded_years = l[k]

                if (k=="phone_number"):
                        phone_numbers = l[k]

                if (k=="email_address"):
                        email_address = l[k]

                if (k=="offices"):
			if (l[k] is None or str(l[k]) == "[]"):
				states = ""
				countries = ""			
				continue

			off = []
			st = []
			country = []

                        for items in l[k]:
				off.append(items['description'])
				st.append(items['state_code'])
				country.append(items['country_code'])

			#off = re.findall("u'description': u'(.*?)',", str(l[k]))
                        #st = re.findall("u'state_code': u'(.*?)',", str(l[k]))
                        #country = re.findall("u'country_code': u'(.*?)',", str(l[k]))

		
                        numberOfOffices = len(l[k])

                        if numberOfOffices == 0:
                                off.insert(0,"HQ")

                        for b in range(numberOfOffices-len(st)):
                                st.insert(0,"")

                        foundHQ = 0

                        if(numberOfOffices == 0):
                                states = ""
                                foundHQ = 1
                                #print "No offices. appending nothing"

                        for o in range(numberOfOffices):
                                if(off[o] is not None and st[o] is not None):
					office_candidate = off[o].rstrip().lstrip()

                                        if(numberOfOffices > 1 and office_candidate in HQ_types):
                                                states = st[o]
                                                countries = country[o]
                                                foundHQ = 1
                                                #print "Have HQ. appending " + st[o]
                                                break

                                        elif (numberOfOffices == 1):
                                                states = st[o]
                                                countries = country[o]
                                                foundHQ = 1
                                                #print "One office. appending " + st[o]
                                                break

                                        elif (o == numberOfOffices):
                                                states = st[o]
                                                countries = country[o]
                                                foundHQ = 1
                                                #print "No HQ and >1 office. appending " + st[o]

                        if (foundHQ == 0 and l[k][0]['description'] is not None):
                                # Print any offices that may potentially be HQs, where we know the state
				for item in l[k]:
					if (item['description'] is not None and item['description'] <> "" and item['state_code'] is not None):
		                                print item['description'].encode('ascii', 'ignore'), " might be HQ for ", page

                        if (foundHQ == 0):
                                # No HQ found
                                states = ""
                                countries = ""

                if (k=="number_of_employees"):
                        employees = l[k]

                if (k=="description"):
                        enc = ""

                        if (l[k] is not None):
                                enc = l[k].encode('ascii', 'ignore')
                        descriptions = enc

                #if (k=="funding_rounds" and str(l[k]) == "[]"):
                #        funded_amount = ""
                #        funded_last_date = ""

                if (k=="funding_rounds"):
			if (l[k] is None or str(l[k]) == "[]"):
				funded_amount = ""	
				funded_last_date = ""
				continue

                        totalFunded = 0

			fraised = []
			fy = []
			fm = []
			fd = []

                        # process list object
			for rnd in l[k]:
				amt = rnd['raised_amount']
				if (amt is not None and amt != ''):
					fraised.append(amt)
					totalFunded += float(amt)
				fy.append(rnd['funded_year'])
				fm.append(rnd['funded_month'])
				fd.append(rnd['funded_day'])

                        #r = re.findall("u'raised_amount': (.*?),", str(l[k]))
                        #fy = re.findall("u'funded_year': (.*?),", str(l[k]))
                        #fm = re.findall("u'funded_month': (.*?),", str(l[k]))
                        #fd = re.findall("u'funded_day': (.*?),", str(l[k]))

                        latestDate = datetime(1,1,1)

                        for a in range(len(fy)):
                                if (fd[a] is None):
                                        fd[a] = 1

                                if (fy[a] is not None and fm[a] is not None):
                                        currentDate = datetime(int(fy[a]), int(fm[a]), int(fd[a]))
                                        if (latestDate < currentDate) or (latestDate==datetime(1,1,1)):
                                                latestDate = currentDate

                        if latestDate != datetime(1,1,1):
                                try:
                                        funded_last_date = latestDate.strftime("%m-%d-%y")
                                except ValueError:
                                        funded_last_date = ""
                        else:
                                funded_last_date = ""

			if (totalFunded != 0):
	                        funded_amount = totalFunded
			else:
				funded_amount = ""


                if (k=="acquisition"):
                        if (l[k] is None or str(l[k]) == "[]"):
                                acquired_amount = ""
				acquired_date = ""
                      		continue 
			
			aa = []
			ay = []
			am = []
			ad = []

			# Assumes one acquisition per company
			aa.append(l[k]['price_amount'])
			ay.append(l[k]['acquired_year'])
			am.append(l[k]['acquired_month'])
			ad.append(l[k]['acquired_day'])	

                        #aa = re.search("u'price_amount': (.*?),", str(l[k]))
                        #ay = re.findall("u'acquired_year': (.*?).$", str(l[k]))
                        #am = re.findall("u'acquired_month': (.*?),", str(l[k]))
                        #ad = re.findall("u'acquired_day': (.*?),", str(l[k]))

                        latestDate = datetime(1,1,1)

                        for a in range(len(ay)):
                                if (ad[a] is None):
                                        ad[a] = 1

                                if (ay[a] is not None and am[a] is not None):
                                        currentDate = datetime(int(ay[a]), int(am[a]), int(ad[a]))
                                        if (latestDate < currentDate) or (latestDate==datetime(1,1,1)):
                                                latestDate = currentDate
 
			if (aa is None or aa[0] == ""):
                                acquired_amount = "Price Not Known" # Acquired but price unknown
                        else:
                                acquired_amount = aa[0]

                        if latestDate != datetime(1,1,1):
                                acquired_date = latestDate.strftime("%m-%d-%y")
                        else:
                                acquired_date = ""

	if (cbase.find( {"permalink": page} ).count() == 0):
		cbase_entry = [{
			"search_phrase": search_phrase,
			"permalink": page,
			"company_name": names,
			"homepage": homepages,
			"founding_year": founded_years,
			"email_address": email_address,
			"phone_number": phone_numbers,
			"states": states,
			"countries": countries,
			"description": descriptions,
			"employees": employees,
			"acquired_amount": acquired_amount,
			"aqcuired_date": acquired_date,
			"funded_amount": funded_amount,
			"funded_last_date": funded_last_date,
		}]

		insert_id = cbase.insert(cbase_entry)
		#print "inserted: ", page
		#print "funding: ", funded_amount, " | acquired: ", acquired_amount


print "Download complete"
#i = raw_input('Press any key to close\n')
