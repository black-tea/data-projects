##### Setup Workspace #####

import operator
import arcpy
import csv
from arcpy import env
from collections import defaultdict

# Inputs
env.workspace = "Z:/VisionZero/GIS/Projects/PrioritizationCorridors/data/RoadDietStats.gdb"

Segment_Table = "Z:/VisionZero/GIS/Projects/PrioritizationCorridors/data/segment_table.csv"
Intersection_Table = "Z:/VisionZero/GIS/Projects/PrioritizationCorridors/data/int_table.csv"

Collisions = "SWITRS2009_to_2013"
Parties = "Collisions2009to2013PartiesJoin"

##### Prep Function #####
def csvTableToList(seg_csv, int_csv):
	
	linking_table_dict = defaultdict(list)
	int_table_dict = defaultdict(list)
	dir_table_dict = defaultdict(list)
	
	# Convert Segment CSV list to dictionary
	with open(seg_csv, 'rb') as fin:
		reader = csv.reader(fin)
		for row in reader:
			corridor_id = int(row[0])
			seg_id = row[1]
			direction = row[2]

			linking_table_dict[corridor_id].append(seg_id)
			dir_table_dict[corridor_id].append(direction)

	with open(int_csv, 'rb') as fin:
		print "beginning to read the csv"
		reader = csv.reader(fin)
		for row in reader:
			corridor_id = int(row[0])
			int_id = row[1]
			print "int_id: " + str(corridor_id) + " " + str(int_id) 
			int_table_dict[corridor_id].append(int_id)
	
	# Testing
	print linking_table_dict
	SegmentQuery(linking_table_dict, dir_table_dict, int_table_dict)

##### Main Function #####
def SegmentQuery(linking_table_dict, dir_table_dict, int_table_dict):

	segment_dict = defaultdict(list)

	# Loop through each segment
	for segment in linking_table_dict:
		
		# Cut #1 Counters
		NonRD_col_ct = 0
		primary_pcf22350_ct = 0
		secondary_pcf22350_ct = 0

		# Road Diet (Cut #2) Counters
		ped_ct = 0
		bike_ct = 0
		sideswipe = 0
		rear_end = 0
		L_Turn = 0
		other_rd_collisions = 0

		# Testing
		print linking_table_dict[segment]
		print 'PRINTING INT LINKING TABLE'
		print int_table_dict[segment]


		# Testing: Prints all the field names
		flds=[]
		fldObj=arcpy.ListFields(Collisions)
		for fld in fldObj:
			print fld.name

		# Direction
		direction = set(dir_table_dict[segment])
		direction_f =  (''.join(list(direction))).split(',')
		print direction_f

		# Query Collisions Attached to Intersection, Query Parties for each Collision
		pcf_vio_cat = ['03','06','07','09','10','11']
		str_pcf_vio_cat = str(('03','06','07','09','10','11'))
		str_linking_dict = ", ".join(linking_table_dict[segment])
		str_int_dict = ", ".join(int_table_dict[segment])
		collision_fields = ["CASE_ID","COLLISION_DATE","PARTY_COUNT","DISTANCE","ALCOHOL_INVOLVED","IntID","PEDESTRIAN_ACCIDENT","BICYCLE_ACCIDENT","COLLISION_SEVERITY","SegID","PCF_VIOL_CATEGORY","TYPE_OF_COLLISION"]
		#collision_query =  """ SegID IN (%s) AND ALCOHOL_INVOLVED IS NULL AND PCF_VIOL_CATEGORY IN %s """ % (str_linking_dict, str_pcf_vio_cat)
		collision_query =  """ SegID IN (%s) OR (IntID IN (%s) AND DISTANCE = 0) """ % (str_linking_dict, str_int_dict)

		# Loop through the result
		collision_rows =  arcpy.da.SearchCursor(Collisions, collision_fields, where_clause=collision_query)
		for collision in collision_rows:
			col_id = int(collision[0])
			print "----------"
			print col_id

			alcohol = collision[4]
			pcf = collision[10]
			
			# col_cat is the return from running the PartyFilter function
			col_cat = []
			col_cat = PartyFilter(col_id, direction_f, alcohol, pcf)
			
			if col_cat is not None:
				# If the collision would be directly targeted by a road diet
				if col_cat[0] >= 1:	
					print segment, col_id
					
					# Append to final table of segment ID and qualifying collisions
					linking_table_dict[segment].append(col_id)

					# Checks for bike/ped related
					if collision[6] == 'Y': # Ped collisions
						ped_ct += 1
					elif collision[7] == 'Y': # Bike collisions
						bike_ct += 1
					elif collision[11] == 'B': # Sideswipe Type
						sideswipe += 1
					elif col_cat[0] == 4:
						L_Turn += 1
					elif collision[11] == 'C': # Rear-end Type
						rear_end += 1
					else:
						other_rd_collisions += 1 # All other collisions

				# Collision is on the corridor, but would not directly be affected by a road diet
				elif col_cat[0] == 0:
					NonRD_col_ct += 1

				# Check PCF 22350
				if pcf == "03":
					primary_pcf22350_ct += 1
				elif col_cat[1] == True:
					secondary_pcf22350_ct += 1

		# Calculate total # of collisions on Corridor
		total_col = ped_ct + bike_ct + sideswipe + rear_end + L_Turn + other_rd_collisions + NonRD_col_ct

		print "Stats for Segment: " + str(segment)
		print "ped ct " + str(ped_ct)
		print "bike ct " + str(bike_ct)
		print "sideswipe " + str(sideswipe)
		print "L Turns " + str(L_Turn)
		print "rear end " + str(rear_end)
		print "other_rd_target " + str(other_rd_collisions)
		print "total collision ct: " + str(total_col)
		print "Primary PCF22350 Ct: " + str(primary_pcf22350_ct)
		print "Secondary PCF22350 Ct: " + str(secondary_pcf22350_ct)
		

##### Check Party Criteria Function #####
def PartyFilter(case_id, direction, alcohol, pcf):

	# Query the Party Table
	party_fields = ["Parties_CASE_ID","Parties_DIR_OF_TRAVEL","Parties_PARTY_TYPE","Parties_MOVE_PRE_ACC","Parties_OAF_VIOL_SECTION"]
	party_query =  """ Parties_CASE_ID = '%s' """ % (str(case_id))
	party_rows = arcpy.da.SearchCursor(Parties, party_fields, where_clause=party_query)
	
	pcf_vio_cat = ['03','06','07','09','10','11']
	party_traveldir_list = []
	mvmt_pre_acc_list = []
	other_collision_on_corridor = False # This is the check that there is at least one car assigned to the segment going the same direction
	oaf_22350 = False
	col_code = 0
	output = [] # The output of the PartyFilter will be a two-item list 1) code and 2) oaf pcf 22350

	# Get Party Type and Direction
	for party in party_rows:
		
		# Check for OAF CVC22350 (Speeding)
		if party[4] is not None:
			if int(party[4]) == 22350:
				oaf_22350 = True

		# If the party is a driver, check that direction matches direction of corridor
		if party[2] == 1:
			if party[1] in direction:
				party_traveldir_list.append(party[1])
				mvmt_pre_acc_list.append(party[3])
				other_collision_on_corridor = True

		# Party is pedestrian
		elif party[2] == 2:
			party_traveldir_list.append(party[1])
			mvmt_pre_acc_list.append(party[3])

		# Party is bicyclist
		elif party[2] == 4:
			party_traveldir_list.append(party[1])
			mvmt_pre_acc_list.append(party[3])

	# If more than two parties, collision qualifies. BUT, first check for L Turns w/ opposing direction
	print "length of party_traveldir_list: " + str(len(party_traveldir_list))

	if len(party_traveldir_list) >= 2:
		
		# If alcohol is invovlved, separate and end
		if alcohol == 'Y':
			print "alcohol involved"
			col_code = 0
		
		# If alcohol not involved, separate btw in/out of the pcf vio categories
		elif pcf in pcf_vio_cat:
			print "pcf in pcf_vio_cat"

			# Check for L Turns
			if 'E' in mvmt_pre_acc_list:
				
				# Get the direction of the left turn
				L_turn_index = mvmt_pre_acc_list.index('E')
				L_turn_direction = party_traveldir_list[L_turn_index]

				# Testing
				print "Checking L Turn..."
				print "L_turn_direction " + str(L_turn_direction)
				print "All directions " + str(party_traveldir_list)

				# Check the direction of the opposing vehicle
				if (L_turn_direction == "N"):
					if "S" in party_traveldir_list:
						print "N v S"
						col_code = 4

				elif (L_turn_direction == "S"):
					if "N" in party_traveldir_list:
						print "S v N"
						col_code = 4

				elif (L_turn_direction == "E"):
					if "W" in party_traveldir_list:
						print "E v W"
						col_code = 4

				elif (L_turn_direction == "W"):
					if "E" in party_traveldir_list:
						print "W v E"
						col_code = 4

			# Alcohol not involved, pcf in category, but not L Turn
			else:
				col_code = 3
		
		# Alcohol not involved, and pcf not in our defined categories
		else:
			col_code = 0

	# Here is where i need to do the elif for at least one party travelling the direction of the corridor
	elif other_collision_on_corridor == True:
		col_code = 0

	if len(party_traveldir_list) < 1:
		print "discarding..."
		return None

	output = [col_code, oaf_22350]
	print output
	return output


##### Run the Script #####
if __name__ == '__main__':

 	#Function
	csvTableToList(Segment_Table,Intersection_Table)
