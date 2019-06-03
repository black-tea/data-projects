#### Identifying Candidate Locations for New Signals #####

#### This script counts of the number of collisions that could be corrected with a signal,
#### and outputs the list of candidate locations (and qualifying locations) to a CSV file.

##### Setup Workspace #####

import operator
import arcpy
import csv
from arcpy import env

# Inputs
env.workspace = "Z:/VisionZero/GIS/Projects/CitywideWarrantSearch_May2016/WarrantSearch.gdb"
UnSigInt = "UnSigInt_Outside3miBuffer"
Collisions = "SWITRS2009_to_2013"
Parties = "Collisions2009to2013PartiesJoin"

# Output
outpath = "Z:/VisionZero/GIS/Projects/CitywideWarrantSearch_May2016/potential_signals.csv"

##### Main Function #####
def SignalWarrantSearch(intersection_fc):

	# TESTING
	ct = 0

	# This dictionary stores all the qualifying collisions for each intersection
	intersection_dict = {}
	intersection_dict2 = {} 

	# Query List of Unsignalized Intersections
	int_fields = ["ASSETID"]
	int_rows = arcpy.da.SearchCursor(intersection_fc, int_fields)
	for intersection in int_rows:

		# TESTING
		int_id = intersection[0]
		ct += 1
		if ct % 10 == 0:
			print ct
		#if ct == 200:
		#	break

		# Add AssetID to the intersection dictionary
		intersection_dict[int_id] = {}

		# This is the ct for bike/ped ksi
		bike_ped_ksi = 0

		# Query Collisions Attached to Intersection, Query Parties for each Collision
		collision_fields = ["CASE_ID","COLLISION_DATE","PARTY_COUNT","DISTANCE","ALCOHOL_INVOLVED","IntID","PEDESTRIAN_ACCIDENT","BICYCLE_ACCIDENT","COLLISION_SEVERITY"]
		collision_query =  """ IntID = %d AND DISTANCE <= 100 AND ALCOHOL_INVOLVED IS NULL """ % (int_id)
		collision_rows =  arcpy.da.SearchCursor(Collisions, collision_fields, where_clause=collision_query)
		
		# If the party criteria is met, add to the dictionary of intersections
		for collision in collision_rows:
			col_id = int(collision[0])
			if PartyFilter(col_id) is True: 
				intersection_dict[int_id][col_id] = collision[1]

			ped_inv = collision[6]
			bik_inv = collision[7]
			col_sev = collision[8]
			if (ped_inv == 'Y' or bik_inv == 'Y') and (col_sev == 1 or col_sev == 2):
				bike_ped_ksi += 1

		# Add bike/ped KSI collision count to the second intersection database
		intersection_dict2[int_id] = bike_ped_ksi

	# Count, Sort, and Write the qualifying intersections to a csv
	CountByYear(intersection_dict, intersection_dict2)


##### Check Party Criteria Function #####
def PartyFilter(case_id):
	
	# Query the Party Table
	party_fields = ["Parties_CASE_ID","Parties_DIR_OF_TRAVEL","Parties_MOVE_PRE_ACC"]
	party_query =  """ Parties_CASE_ID = '%s' """ % (str(case_id))
	party_rows = arcpy.da.SearchCursor(Parties, party_fields,where_clause=party_query) 
	filtered_party_list = []

	# Filter out Movement Preceding Collision
	excluded_movements = ["A", "C", "K","N","O","Q"]
	# A: Stopped, B: Proceeding Straight, c: Ran Off Road, D: Making Right Turn, E: Making Left Turn, F: Making U-Turn, G: Backing,
	# H: Slowing / Stopping, I: Passing Other Vehicle, J: Changing Lanes, K: Parking Maneuver, L: Entering Traffic, M: Other Unsafe Turning,
	# N: Crossed Into Opposing Lane, O: Parked, P: Merging, Q: Traveling Wrong Way, R: Other

	for party in party_rows:
		if party[2] not in excluded_movements:
			filtered_party_list.append(party[1])

	# If at least 2 parties remaining, check for conflicting directions
	if len(filtered_party_list) >= 2:
		if (filtered_party_list[0] == "N") or (filtered_party_list[0] == "S"):
			if (filtered_party_list[1] == "E") or (filtered_party_list[1] == "W"):
				return True
		elif (filtered_party_list[0] == "E") or (filtered_party_list[0] == "W"):
			if (filtered_party_list[1] == "N") or (filtered_party_list[1] == "S"):
				return True

##### Count, Sort, Write #####
def CountByYear(intersection_dictionary, intersection_dictionary2):
	with open(outpath, 'wb') as fout:
		writer = csv.writer(fout, lineterminator='\n')
		
		# First check: does the intersection even have 5 collisions total?
		for intersection in intersection_dictionary:
			if len(intersection_dictionary[intersection]) >= 5:
				print ">5"

				# Second step: sort collisions by date for that intersection
				sorted_intersection = sorted(intersection_dictionary[intersection].items(), key=operator.itemgetter(1)) 
				all_dates = []
				lenslist = [] # this is a list of lengths

				# Loop through the sorted collision dates and count how many collisions were in a 1-year time-period after each collision
				for collision in sorted_intersection:
					initial_date = collision[1]
					all_dates.append(initial_date)
					one_year_later = initial_date + datetime.timedelta(days=(365))
					
					# count how many within the 1-year period
					col_ct = 0
					for collision2 in sorted_intersection:
						if initial_date <= collision2[1] <= one_year_later:
							col_ct += 1
					
					# append the count of collisions to my list of lengths
					lenslist.append(col_ct)

				# get the largest value for max_ct
				max_ct = max(lenslist)
				
				# if it qualifies, find the latest period
				print "intersection"
				if max_ct >= 5:

					greater_than_five =[]
					for length in lenslist:
						if length >= 5:
							greater_than_five.append(lenslist.index(length))
					latest_qualifying_period_index = max(greater_than_five)
					latest_qualifying_period_startdate = all_dates[latest_qualifying_period_index]	

					print "second" + str(intersection)
					print sorted_intersection
					print "The max count is " + str(max_ct)
					print "The max index is " + str(lenslist.index(max_ct))
					writer.writerow([intersection] + [all_dates[latest_qualifying_period_index]] + [intersection_dictionary2[intersection]] + sorted_intersection)

##### Run the Script #####
if __name__ == '__main__':

 	# Main Function
	SignalWarrantSearch(UnSigInt)
