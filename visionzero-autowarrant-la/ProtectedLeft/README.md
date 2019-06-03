# Protected Left Warrant Search

This script was used to identify candidate locations for new protected-left turn signals in the City of Los Angeles based on safety warrant criteria specified in the [California Manual on Uniform Traffic Control Devices] (http://www.dot.ca.gov/trafficops/camutcd/). LADOT then submitted the top locations for funding through the [Metro ExpressLanes Net Toll Revenue Re-Investment Grant Program.] (https://www.metro.net/projects/expresslanes/projectsprograms/)

Please suggest improvements! I am by no means a professional pythoner.

### Requirements

- Python
- Geocoded collision data with the following fields:
  - Movement preceding the collision for each party
  - Direction of travel for each party
- Geocoded intersections
- This script uses the arcpy cursor functions within Arcmap, but you could reformat it to match the way your data is stored.

### Pre-Script Preparation

1. First assign all collisions the unique ID of the nearest intersection. The end result should be an additional field in the collisions table that has the unique ID for the related intersection. This script gives more detail on our process at LADOT.
2. For the intersection data, subset out those that are not signalized.
3. For the collision data, filter for collisions involving left or u-turns. Also, remove those involving alcohol.
4. This project was focused only on our most recent year of available data (2013) so we removed those before that year. If you would like to do a multi-year search, see the [Signalized Safety Warrant Project] (https://github.com/black-tea/VisionZero/tree/master/NewSignals) for a method to aggregate by year and rank by most recent year that warrant criteria was met.

### Script Inputs

### Process Diagram

![Left Turn Warrant Process Diagram](https://github.com/black-tea/VisionZero/blob/master/ProtectedLeft/HSIP_CityWide_LeftTurn.png)
