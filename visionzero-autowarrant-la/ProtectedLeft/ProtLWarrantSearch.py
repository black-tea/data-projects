#### Identifying Candidate Locations for Protected Left Turns #####

#### This script counts of the number of collisions involving L or U Turns (for each direction)
#### and appends the count to the attribute table of the Signalized Intersection Feature Class
#### with an Update Cursor.

#### Note that the process diagram includes looping through each party associated with a collision.
#### In this script, we had already joined the party table to the collision table, and subset out
#### collisions that did not involve a left or U-Turn.

import arcpy
from arcpy import env

##### Setup Workspace #####

env.workspace = "Z:/VisionZero/GIS/Data/OtherDOT_AnalysisProjects/MetroExpressLanes Mar2016/MetroExpressLanesMar2016.gdb"
SigInt = "SigIntwithin3miBuffer" # Feature class containing all signalized intersections in the City
Collisions = "Parties2013DriversUorLTurn" # Parties table that has been joined to the collisions table, which includes the unique intersection ID

##### Loop through the Intersections & Count Collisions per Intersection #####

Collision_Table = []
int_fields = ["ASSETID","LTurn_N","LTurn_S","LTurn_E","LTurn_W","LTurn_None"]
collision_fields = ["Parties_DIR_OF_TRAVEL","SWITRS2009_to_2013_IntID"]
int_rows = arcpy.da.UpdateCursor(SigInt, int_fields)

ct = 0

# Loop through all signalized intersections
for row in int_rows:
	
	# Add unique intersection ID to the Collision Table
	row_list = []
	row_list.append(row[0])

	# Set the counters for each direction of travel
	N_ct = 0
	S_ct = 0
	E_ct = 0
	W_ct = 0
	NotStated = 0

	# Loop through all the collisions; add to the count for each direction
	collision_rows = arcpy.da.SearchCursor(Collisions,collision_fields)
	for row2 in collision_rows:
		if row2[1] == row[0]:
			if row2[0] == "N":
				N_ct += 1
			elif row2[0] == "S":
				S_ct += 1
			elif row2[0] == "E":
				E_ct += 1
			elif row2[0] == "W":
				W_ct += 1
			else:
				NotStated += 1


	# Update the Collision_Table with final counts
	row_list.append(N_ct)
	row[1] = N_ct
	row_list.append(S_ct)
	row[2] = S_ct
	row_list.append(E_ct)
	row[3] = E_ct
	row_list.append(W_ct)
	row[4] = W_ct
	row_list.append(NotStated)
	row[5] = NotStated
	Collision_Table.append(row_list)
	int_rows.updateRow(row)

	# Update the counter variable
	ct += 1

print "Done"





