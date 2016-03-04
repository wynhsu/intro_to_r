from __future__ import division
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import numpy as np
import os
import re 
import sys
import time

import data_dicts
import recode
import missing 
from filepaths import get_filepaths


def print_and_log(phrase):
	from PyJobTools import rlog
	print phrase
	rlog.log(phrase)

def run_local():

	if os.path.isdir('H:/') == True:
		j = 'J:'
		h = 'H:'
		#cod_dict, cod_raw, cod_missingness = run_local()
	elif os.path.isdir('/home/j/') == True:
		j = '/home/j'
		h = '/homes/abertozz'
	else:
		print_and_log('What am I supposed to do?')
	
	cod_dict = {}
	cod_raw = {}
	cod_missingness = {}

	nrows = None # number of rows of file to read; make it small for running local jobs 
	map_dir = '%s/Project/us_counties/mortality/data_prep/counties/01_clean_microdata/state_map.csv' %j

	yearvals = [1992]

	for year in yearvals:
		if year in range(1968, 1989):
			us_indir = "%s/DATA/USA/VR/%d/USA_VITAL_STATISTICS_%d_MORTALITY.TXT" %(j, year, year)
			ps_indir = 'NONE'
		elif year in range(1989, 1994): 
			fname = get_filepaths(year)
			us_indir = '%s/LIMITED_USE/PROJECT_FOLDERS/USA/NVSS_MORTALITY/%d/%s' %(j, year, fname)
			ps_indir = 'NONE'
		else:
			fname = get_filepaths(year)
			us_indir = '%s/LIMITED_USE/PROJECT_FOLDERS/USA/NVSS_MORTALITY/%d/%s' %(j, year, fname['US'])
			ps_indir = '%s/LIMITED_USE/PROJECT_FOLDERS/USA/NVSS_MORTALITY/%d/%s' %(j, year, fname['PS'])

		rlog.open('%s/temp/amelia/counties/parse_death_files/debug_parse_%d.log' %(j, year))
		rlog.log('Hello, world')

		rlog.log('Initializing')

		cod_data, cod_data_raw, cod_missingness = parse_cod_mortality(year, us_indir, ps_indir, map_dir, nrows)

	return cod_data, cod_data_raw, cod_missingness

def run_on_cluster():
	#set parameters
	year, us_indir, ps_indir, parent_dir, log_dir, map_dir= sys.argv[1:7]
	year = int(year)
	nrows = None # number of rows of file to read; make it small if you're just testing things out

	#if NOT running the entire file (for testing), note this in name when saving 
	if nrows != None:
		nrow_str = '_TEST_%d_ROWS' %nrows
	else:
		nrow_str = ''

	#get date and time info
	date_regex = re.compile('\W')
	date_unformatted = str(datetime.now().replace(microsecond=0))
	date_str = date_regex.sub('_', date_unformatted)

	rlog.open('%s/parse_fwf_%d_%s.log' %(log_dir, year, date_str))

	print_and_log('Hello, world')
	print_and_log('Initializing')

	#run code
	cod_data, cod_data_raw, cod_missingness  = parse_cod_mortality(year, us_indir, ps_indir, map_dir, nrows)

	#save files:

	# 1. Save parsed-but-not-cleaned files:
	print_and_log('saving parsed files')
	cod_data_raw.to_csv('%s/parsed/data_%s_parsed.csv' %(parent_dir, year))

	# 2. Save missingness 
	print_and_log('saving missingness data')
	cod_missingness.to_csv('%s/cleaned/missingness/missingness_info_%s.csv' %(parent_dir, year))

	# 3. Save cleaned data.
	# NOTE: if the year is 1980/1981 or 1988-1991, there will be deaths misassigned to nonexistent counties or deaths assigned to 'missing' due to censorship, respectively.
	# we re-assign these deaths to real counties in the next steps (in the prep_for_redistribution folder), but until then we must save these pre-adjusted files somewhere else, 
	# hence the logical tree below.
	if year in (range(1980, 1982) + range(1988, 1992)):
		print_and_log('saving pre-adjusted data to special folder')
		cod_data.to_csv('%s/cleaned/pre_adjust_ak_ga_ny/data_%s_pre_adjust.csv' %(parent_dir, year))
	else:
		print_and_log('saving cleaned data')
		cod_data.to_csv('%s/cleaned/data_%s_cleaned.csv' %(parent_dir, year))

	print_and_log('File is parsed, cleaned, and saved!')


def parse_cod_mortality(year, us_indir, ps_indir, map_dir, nrows = None):

	#Extract variables of interest using the appropriate data dictionary for that year
	from PyJobTools import rlog
	
	print_and_log('Loading Data Dictionary')

	cod_data_dict = data_dicts.give_data_dict(year)

	dictlen = len(cod_data_dict)
	convert = {}
	for idx in range(dictlen):	
		convert[idx] = str

	print_and_log('Parsing File')
	cod_data_raw = pd.io.parsers.read_fwf(us_indir, colspecs = cod_data_dict.values(), header = None, nrows = nrows, converters= convert)
	cod_data_raw.columns = cod_data_dict.keys()

	#add deaths in territories if they exist
	if ps_indir!='NONE':
		print_and_log('loading territories')
		cod_data_ps = pd.io.parsers.read_fwf(ps_indir, colspecs = cod_data_dict.values(), header = None, nrows = nrows, converters= convert)
		cod_data_ps.columns = cod_data_dict.keys()
		print_and_log('merging territories')

		cod_data_raw = cod_data_raw.append(cod_data_ps)

	index = cod_data_raw.index

	#create a new df that will contain only data that is consistent over years and coding.  
	#it will get filled over the course of this function
	if year < 1980:
		new_columns = ['year', 'state_res_numeric', 'state_res_alpha', 'cause', 'icd_version']
	else:
		new_columns = ['year', 'state_res_numeric', 'state_res_alpha', 'county_res', 'full_fips_res_numeric',
		'full_fips_res_alpha', 'sex', 'race', 'age', 'hisp_desc', 'hisp_recode', 'education', 'cause', 'icd_version',
		'industry', 'occupation']

	cod_data = DataFrame(index = index, columns = new_columns)

	# set year (note: this isn't in our data dict because it's coded inconsistently across years)
	cod_data['year'] = year

	# icd version
	if year in range (1979, 1999):
		cod_data.icd_version = "ICD9"
	else:
		cod_data.icd_version = "ICD10"

	print_and_log('Year %s: ICD code %s' %(year, cod_data.icd_version.iloc[0]))

	deathcount = len(index)
	print_and_log('Number of Deaths: %d' %deathcount)

	state_map = pd.read_csv(map_dir, dtype=str)

	# STATE AND COUNTY LABELS 
	# years after 2003 only have the alphabetic state label, years before only have the numeric one.
	# we meap each of these to the other for a full dataset of both.
	print_and_log('recoding state and county labels')

	if year<1982:

		## 01. MERGE MAP PREP
		#	we need to translate the non-fips state and county labels into fips.  For counties, we use sandeeps' county mapping dataset. 
		print_and_log('prepping pre-fips to fips map')
		pre_fips_map_dir = '/home/j/Project/USBODI/falling behind/reproduce results/data/mort/merge/sk_mcounty_death_7081.dta'
		pre_fips_map = pd.read_stata(pre_fips_map_dir)
		pre_fips_map.rename(columns={'fip7081': 'pre_fips_county'},inplace=True)
		
		#this dataset has duplicate FIPS for two counties: 
		# 	--Ste Genevieve, MO, has FIPS of 29186 and 29193, probably due to a name change.  29193 was not used after 1979; 
		#		stick to 29186
		#  -- Washabaugh, SD, has FIPS of 46131 and 46001. Washabaugh had 46131 for 1982, then was promptly merged in its 
		# 		entirety with Jackson county in 1983.  I have no idea where the 46001 came from, let's get rid of it.
		pre_fips_map = pre_fips_map[(pre_fips_map['fips']!='29193') & (pre_fips_map['fips']!='46001')]		
		#pre_fips_map.drop(['statename', 'countyname', 'mcounty'], axis=1, inplace=True)
		fips_dict = dict(zip(pre_fips_map.pre_fips_county, pre_fips_map.fips))

		## 02. ALAKSA ADJUSTMENT
		#	In 1980 and 1981 in Alaska, some deaths were mapped to 1960s election districts instead of 1980s boroughs.  In a few cases,
		#	The 1960s district and the 1980s borough are similar enough that we can feel safe mapping the former directly to the latter, 
		# 	which we do for the first five districts below. The final four districts overlap multiple 1980s boroughs, and deaths here are 
		#	reallocated proportional to mean deaths in the 1980s (see the script '01_adjust_alaska.py' in the folder '../02_prep_for_redistribution')

		print_and_log('recoding alaska districts')
		# DISTRICTS MAP DIRECTLY TO COUNTIES
		fips_dict['02021'] = '02185' #Barrow to North Slope
		fips_dict['02011'] = '02122' #Seward to Kenai Penninsula
		fips_dict['02008'] = '02261' #Valdez-Chitina to Valdez-Cordova
		fips_dict['02007'] = '02261' #Cordova to Valdez-Cordova
		fips_dict['02009'] = '02170' #Palmer-Wasilla to Matanuska-Sustina

		#PREP FOR MORE INVOLVED REASSINGMENT
		fips_dict['02020'] = '02998' #SOME IDIOT assigned anchorage a FIPS of 02020, so we need to make this adjustment for Yukon-Koyukuk to remain itself until re redistribute it
		fips_dict['02005'] = '02005' #Old Juneau (02005) will successfully map to new Juneau(02110) if we let it, but old Juneau takes up more area than new juneau, so we keep it as is 
									 #until we redistribute it
		fips_dict['02015'] = '02015' #Old Bristol Bay (02015) will successfully map to new Bristol Bay (02060) if we let it, but old Bristol Bay takes up more area than new Bristol Bay, so we keep it as is 
									 #until we redistribute it
		fips_dict['02019'] = '02019' #Old Fairbanks North Star (02015) will successfully map to new Fairbanks North Star (02060) if we let it, but old Fairbanks North Star takes up more
									 # area than new Fairbanks North Star, so we keep it as is until we redistribute it
		fips_dict['02017'] = '02017' #Kuskokwim needs to remain as itself until we redistribute it
		fips_dict['02006'] = '02006' #Lynn Canal-Icy Strait needs to remain as itself until we redistribute it

	for county_type in ['res', 'occ']: #repeat this process for state/county of residence and occurrence

		print 'recoding %s' %county_type

		if year<1982:

			# merge onto original data
			cod_data_raw['pre_fips_county_%s' %county_type] = cod_data_raw['state_%s_NONFIPS' %county_type] + cod_data_raw['county_%s_NONFIPS' %county_type]
			cod_data_raw['fips_%s' %county_type] = cod_data_raw['pre_fips_county_%s' %county_type].map(fips_dict)
			cod_data_raw['fips_%s' %county_type].fillna('00000', inplace=True) #na values will be foreign residents; fill these spots with the appropriate '00' code for state and '000' for county
			cod_data['county_%s' %county_type] = cod_data_raw['fips_%s' %county_type].apply(lambda x: x[2:5])

			#map the nonfips state codes to fips, both alpha and numeric.
			fips_map = dict(zip(state_map['nonfips'], state_map['fips']))
			alpha_map= dict(zip(state_map['nonfips'], state_map['alpha']))

			cod_data['state_%s_numeric' %county_type] = cod_data_raw['state_%s_NONFIPS' %county_type].map(fips_map)
			cod_data['state_%s_alpha' %county_type]= cod_data_raw['state_%s_NONFIPS' %county_type].map(alpha_map)

		elif year>=2003:
			fips_map = dict(zip(state_map['alpha'], state_map['fips']))
			cod_data['state_%s_alpha' %county_type] = cod_data_raw['state_%s_alpha' %county_type]
			cod_data['state_%s_numeric' %county_type] = cod_data['state_%s_alpha' %county_type].map(fips_map)
			# this dataset includes us territories.  frustratingly, American Samoa and the Northern Marianas have counties coded to '000' for everybody (residents and otherwise).
			# here, we recode deaths to territory residents that occur in that territory to the special code '998', so we can identify the true '0's later in the cleaning process.
			print "recoding marianas and samoa deaths"
			cod_data_raw['county_%s' %county_type] = np.vectorize(recode.recode_mp_as)(cod_data_raw['state_occ_alpha'], cod_data_raw['state_res_alpha'], cod_data_raw['county_%s' %county_type])
			# also frustratingly, there are 32 deaths with null values for 'county_occ' and 'county_res' in 2004. set these to 999.
			if year==2004:
				cod_data_raw['county_%s' %county_type] = cod_data_raw['county_%s' %county_type].replace('nan', '999')
			cod_data['county_%s' %county_type] = cod_data_raw['county_%s' %county_type]

		else:
			alpha_map= dict(zip(state_map['fips'], state_map['alpha']))
			cod_data['state_%s_numeric' %county_type] = cod_data_raw['state_%s_numeric' %county_type]
			cod_data['state_%s_alpha' %county_type] = cod_data['state_%s_numeric' %county_type].map(alpha_map)
			cod_data['county_%s' %county_type] = cod_data_raw['county_%s' %county_type]

		cod_data['full_fips_%s_numeric' %county_type] = cod_data['state_%s_numeric' %county_type] + cod_data['county_%s' %county_type]

	# AGE: recode age to be consistent across groups
	print_and_log('recoding age')
	cod_data['age'] = np.vectorize(recode.recode_age)(cod_data['year'], cod_data_raw['age'])

	# SEX: after 2003, sex gets coded as M/F rather than 1/2. make this consistent.
	print_and_log('recoding sex')
	cod_data['sex'] = cod_data_raw['sex'].apply(recode.recode_sex)

	# RACE: lots of messiness with this, see recode.py for documentation.  The final result should be consistent.
	print_and_log('recoding race')
	cod_data['race'] = np.vectorize(recode.recode_race)(cod_data['year'], cod_data_raw['race'])

	# HISPANIC ORIGIN: more straightforward than race, but only exists for some years.
	print_and_log('recoding hispanic origin')
	if year<1984:   # we need some value for 'hisp_desc' in the raw dataset or the function will break
		cod_data_raw['hisp_desc'] = '99'
	cod_data['hisp_desc'] = np.vectorize(recode.recode_hisp_desc)(cod_data['year'], cod_data_raw['hisp_desc'])

	# HISPANIC RECODE: more detailed information on hispanic origin * race
	print_and_log('recoding hispanic recode')
	if year<1989:
		cod_data['hisp_recode'] = np.vectorize(recode.recode_hisp_recode)(cod_data['hisp_desc'], cod_data['race'])
	else:
		cod_data['hisp_recode'] = cod_data_raw['hisp_recode']

	#EDUCATION : we had to make a new system for this in order to combine changes in all previous mapping methods;
	# 			see recode.py for details
	print_and_log('recoding education')
	if year<1989:
		cod_data_raw['education'] = '99'
		cod_data_raw['edu_flag'] = '2' # no education data for pre-1989
	elif year in range(1989, 2003):
		cod_data_raw['edu_flag'] = '0' # years from 1989 to 2002 won't have a flag, they just ARE flag 0
	else:
		# for years greater than 2003, two education columns are listed: edu_1989, for the states that report
		# using the 1989 system, and edu_2003, for the states that report using the 2003 system.  We have to create
		# one unified 'education' column that combines these two, to pass into the function (along with the necessary flags)
		cod_data_raw['education'] = cod_data_raw['edu_1989'].fillna(cod_data_raw['edu_2003'])

	cod_data['education'] = np.vectorize(recode.recode_education)(cod_data_raw['education'], cod_data_raw['edu_flag'])

	# INDUSTRY AND OCCUPATION: we don't have enough years to (necessarily) make this worthwhile, but we can keep it 
	# for now. (see recode.py for details on this) Short version: we only have data from 1985-1999, only the 1992-1999 
	# data needs to be recoded. All other years, fill in the 'unknown' values
	print_and_log('recoding industry and occupation')
	if year in range(1985, 1992):
		cod_data['industry'] = cod_data_raw['industry_recode']
		cod_data['occupation'] = cod_data_raw['occupation_recode']
	elif year in range(1992, 2000):
		cod_data['industry'] = cod_data_raw['industry'].apply(recode.recode_industry)
		cod_data['occupation'] = cod_data_raw['occupation'].apply(recode.recode_occupation)
	else:
		cod_data['industry'] = '51'
		cod_data['occupation'] = '59'

	# CAUSE: causes are numeric or alphanumeric codes of length three (i.e. 243 for ICD9 or A14 for ICD10) or 4 with
	# a decimal (i.e. 243.8 or A14.3).  However, these codes are recorded in the data without the decimal point.  
	# this means that if we don't change anything, and read them in as numbers later, the codes 042.0 and 420 will
	# look identical.  To clarify this, we add the decimal point and standardize everything to length 4, and make sure to
	# read things in as strings in the future.
	# We recode entity codes (the other codes on the certificate) the same way
	print_and_log('recoding cause')

	for entity_idx in range(0,21):
		print 'recoding cause number %s' %entity_idx

		if entity_idx==0:
			new_colname = 'cause'
			old_colname = 'cause'
		else:
			new_colname = 'multiple_cause_%s' %entity_idx
			old_colname = 'entity_%s' %entity_idx

			#the code has a bunch of extraneous information; just keep the cause code
			cod_data_raw[new_colname] = cod_data_raw[old_colname].fillna('0000000')
			cod_data_raw[new_colname] = cod_data_raw[new_colname].apply(lambda x: x[2:6].strip())
			
		#cod_data[new_colname] = cod_data_raw[new_colname].apply(lambda x: x if len(x)==4 else x+'0') #add a zero on the end if not already 4 elements long
		cod_data[new_colname] = cod_data_raw[new_colname].apply(lambda x: x if len(x)==3 else x[0:3] + '.' + x[3]) #add a decimal point if it's 4 elements long, otherwise keep it 3-digit

	###MISSINGNESS###
	#count missingness/unknowns, get percentages
	print_and_log('Calculating Missingness')
	cod_missingness = missing.calc_missingness(cod_data)

	cod_data.set_index('year', inplace=True)

	return cod_data, cod_data_raw, cod_missingness

#where the work begins
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 10)

pyjobtools_home = 'us_counties/_common'

if os.path.isdir('H:/') == True:
	j = 'J:'
	h = 'H:'
	#cod_dict, cod_raw, cod_missingness = run_local()
elif os.path.isdir('/home/j/') == True:
	j = '/home/j'
	h = '/homes/abertozz'
else:
	print_and_log('What am I supposed to do?')

pyjob_dir = '%s/%s' %(h, pyjobtools_home)
sys.path.append(pyjob_dir)
from PyJobTools import rlog

try:
	test=sys.argv[1]
	print "running remotely!"
	run_on_cluster()
except:
	print "running locally!"
	cod_data, cod_data_raw, cod_missingness = run_local()


sys.path.remove(pyjob_dir)