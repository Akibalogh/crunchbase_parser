import simplejson as json
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
search_phrase = ""

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

	try:
	        result = json.load(requests.get(url, params=kwargs).content)
	except ValueError:
                # Skip any pages where the HTML generates error
	        print "ValueError in search"
	        return dict('')

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
                result = json.load(returnObject.content)
        except ValueError:
                # Skip any retreivals
                print "ValueError in retrieval"
                return {"error" : "error"}
	except IOError:
		print "IOError in retrieval"
                return {"error" : "error"}

        return result

permalinks = search(MasheryKey, search_phrase, 10, 1)

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
                        """
                        names.append("")
                        homepages.append("")
                        founded_years.append("")
                        phone_numbers.append("")
                        states.append("")
                        countries.append("")
                        descriptions.append("")
                        funded_amount.append("")
                        employees.append("")
                        acquired_amount.append("")
                        funded_last_date.append("")
                        acquired_date.append("")
                        """

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

                if (k=="offices"):

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

                        HQ_types = ['HQ', "dynaTrace HQ", "Zorch Headquarters", "Sales / Marketing Office", "Baltimore (Headquarters)", "New York Headquarters", "NA Headquarters", "Headquaters", "US Head office", "US Corporate HQ", "American Headquarters", "NYC HQ", "N America Headquarters", "World/US Headquarters", 'Admeld NYC (HQ)', 'Headquarters (US)', 'HQ USA', 'H-FARM USA', 'StepOut HQ', 'HQ / USA', 'Seattle East (HQ)', 'HQ Los Angeles', 'MyBuys HQ', 'Google Headquarters', 'Global HQ', 'Corporate Headquarters', "Main Office", "California Office", "InFact Group GmbH", "Seattle", "Auronix - Sillicon Valley", "Registered office", "Sewri Office", "Portland Office", "iWatchLife", "Houston", "101 W. Kirkwood Ave.", "Technical", "Boston Office", "Atlanta Office", "Main Office ", "Sales Office", "San Francisco Office", "Toronto Office", "NYC Office", "Kronos headquarters", "Business Development", "Recognia Inc.", "San Francisco", "Los Angeles Office", "World HQ", "Webvisionz", "OFFSITENOC Services", "USA HQ", "PERONii Solutions", "World Headquarter", "Corporate Headquarters:", "Corporate headquarters:", "San Jose Headquarters", "NewQuest - Paris", "San Francisco", "Main Office", "US Sales Office", "U.S. Headquarters", "3scale USA", "North American HQ", "Corporate Office 4CS ", "Operations HQ", "London, UK", "Interactive Buzz, LLC.", "ISACGlobal, US", "Office", "TimeWave Media Vermont", "European HQ", "US Office", "ClickFuel", "USA Head office", "London Office", "SiteWit Headquarter", "Head office", "Direct Partners", "Jivox US Headquarters", "Offices", "iKen Solutions - India", "Kronos headquarters", "WORLDWIDE HEADQUARTERS", "Corporate Office 4CS", "Grupa Nokaut HQ", "Miami Headquarter", "Worldwide Headquarters", "Boston HQ Office", "Terapeak HQ", "European Headquarters", "Next Big Sound HQ", "Paris HQ Office", "IIH Nordic Headquarters", "Suzerein Solutions HQ", "Argyle HQ", "Amsterdam HQ", "Birst Headquarters", "Sales Headquarters", "San Francisco HQ", "Asia-Pacific HQ", "Raleigh-Durham HQ", "UserReport.com, Inc. (HQ)", "Crowdbooster HQ", "Global Headquarters", "TOA Technologies - US ", "EzineArticles.com HQ", "Paris HQ", "ReachForce HQ", "Main Headquarters", "US Headquarters", "North America HQ", "USA Headquarters", "Home Office", "French Headquarters", "Sweden (HQ)", "Tampa HQ", "Board HQ", "Australian HQ", "Xpandion HQ", "London HQ", "Global HQ", "Social Apps HQ", "Richmond (HQ)", "US HQ", "Company Headquarters", "USA Marketing Unit", "US Address", "Main office", "Palo Alto HQ", "Corporate Office", u"HQ", u'Head Office', u'Headquarters', u'Headquarter', 'New York Office', u'Head Office', u'Flurry San Francisco', u'Corporate Headquarters', 'Corporate HQ', u'CORPORATE OFFICE', u'World Headquarters', 'Headquarters', 'Operations Office', 'USA - San Francisco - HQ', 'USA Headquarters- NYC', 'BTBM HQ', 'SF HQ', 'Administrative HQ', 'InfoReach Inc. (HQ)']

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
		                                print item['description'].encode('ascii', 'ignore'), " for ", page

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

                if (k=="funding_rounds" and str(l[k]) == "[]"):
                        funded_amount = ""
                        funded_last_date = ""

                if (k=="funding_rounds" and str(l[k]) != "[]"):
                        totalFunded = 0

                        # process list object
                        r = re.findall("u'raised_amount': (.*?),", str(l[k]))
                        fy = re.findall("u'funded_year': (.*?),", str(l[k]))
                        fm = re.findall("u'funded_month': (.*?),", str(l[k]))
                        fd = re.findall("u'funded_day': (.*?),", str(l[k]))

                        for g in r:
                                if (g != 'None'):
                                        totalFunded += float(g)

                        latestDate = datetime(1,1,1)

                        for a in range(len(fy)):
                                if (fd[a] == "None"):
                                        fd[a] = 1

                                if (fy[a] != "None" and fm[a] != "None"):
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

                        funded_amount = totalFunded


                if (k=="acquisition"):
                        aa = re.search("u'price_amount': (.*?),", str(l[k]))
                        ay = re.findall("u'acquired_year': (.*?).$", str(l[k]))
                        am = re.findall("u'acquired_month': (.*?),", str(l[k]))
                        ad = re.findall("u'acquired_day': (.*?),", str(l[k]))

                        latestDate = datetime(1,1,1)

                        for a in range(len(ay)):
                                if (ad[a] == "None"):
                                        ad[a] = 1

                                if (ay[a] != "None" and am[a] != "None"):
                                        currentDate = datetime(int(ay[a]), int(am[a]), int(ad[a]))
                                        if (latestDate < currentDate) or (latestDate==datetime(1,1,1)):
                                                latestDate = currentDate

                        if (l[k] is None):
                                acquired_amount = ""
                        else:
                                if (aa is None or aa.group(1) == "None"):
                                        acquired_amount = "Price Not Known" # Acquired but price unknown
                                else:
                                        acquired_amount = aa.group(1)


                        if latestDate != datetime(1,1,1):
                                acquired_date = latestDate.strftime("%m-%d-%y")
                        else:
                                acquired_date = ""

	if (cbase.find( {"permalink": page} ).count() == 0):
		cbase_entry = [{
			"permalink": page,
			"names": names,
			"homepages": homepages,
			"founded_years": founded_years,
			"phone_numbers": phone_numbers,
			"states": states,
			"countries": countries,
			"descriptions": descriptions,
			"employees": employees,
			"acquired_amount": acquired_amount,
			"funded_amount": funded_amount,
			"funded_last_date": funded_last_date,
			"aqcuired_date": acquired_date
		}]

		insert_id = cbase.insert(cbase_entry)
		#print "inserted: ", page


#print "Ready to write"
#print "Name " + str(len(names))
#print "Homep " + str(len(homepages))
#print "Founded " + str(len(founded_years))
#print "Emply " + str(len(employees))
#print "Phone " + str(len(phone_numbers))
#print "State " + str(len(states))
#print "Country " + str(len(countries))
#print "Descr " + str(len(descriptions))
#print "Funded " + str(len(funded_amount))
#print "Last Funding Date " + str(len(funded_last_date))
#print "Acquired For " + str(len(acquired_amount))
#print "Acquired Date " + str(len(acquired_date))


#beforeFilter = zip(names, homepages, founded_years, employees, phone_numbers, states, countries, descriptions, funded_amount, funded_last_date, acquired_amount, acquired_date)


#finalResults = beforeFilter # Filter disabled for now

"""

##### FILTERS

# Filter by number of employees
# Filter by time range
finalResults = []
numberOfCompanies = len(beforeFilter)

for item in range(numberOfCompanies):
        c = beforeFilter.pop(0)
        employeeFilter = 0
        fundedDateFilter = 0
        acquiredDateFilter = 0

        # Pull out digits from str
        # numEmp = re.search("(\d*)", str(c[3]))

        # Only include companies where # of employees is > 20 or None
        if (c[3] > 20) or (c[3] is None):
                employeeFilter = 1

        fundingDate = datetime(1,1,1)

        if (c[7] != ""):
                fundingDate = datetime.strptime(c[7], "%m-%d-%y")

        # Company was funded in last 6 months
        if (fundingDate == datetime(1,1,1)) or (fundingDate > (datetime.today() - fundedDateLimit)):
                fundedTimeFilter = 1

        acquiredDate = datetime(1,1,1)

        # Company was acquired over a year ago
        if (c[9] != "" and c[9] != "Price Not Known"):
                acquiredDate = datetime.strptime(c[9], "%m-%d-%y")

        if (acquiredDate == datetime(1,1,1)) or (acquiredDate < (datetime.today() - acquiredDateLimit)):
                acquiredTimeFilter = 1


        if employeeFilter == 1 and (fundedDateFilter == 1 or acquiredTimeFilter == 1):
                finalResults.append(c)

"""




# Insert headers
#finalResults.insert(0,("Name", "Homepage", "Founded", "Employees", "Phone", "HQ State", "HQ Country", "Description", "Total VC Funding", "Last Funding Date", "Acquired For", "Acquired On"))


#w = open(outfile, 'wb')
#writer = csv.writer(w)

#for r in finalResults:
#        try:
#                writer.writerow(r)
#        except UnicodeEncodeError:
#                print "UnicodeEncodeError"
#                writer.writerow("")

#        w.flush
#w.close

#print "All set. " + str(len(finalResults) - 1) + " record(s) processed.\n"
#print
#print "Written to " + outfile
i = raw_input('Press any key to close\n')
