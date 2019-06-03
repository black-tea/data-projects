# New Signal Warrant Search

This script was used to identify candidate locations for new signals in the City of Los Angeles based on safety warrant criteria specified in the [California Manual on Uniform Traffic Control Devices] (http://www.dot.ca.gov/trafficops/camutcd/). LADOT then submitted these candidate locations for funding through the [Metro ExpressLanes Net Toll Revenue Re-Investment Grant Program] (https://www.metro.net/projects/expresslanes/projectsprograms/) and the [California Highway Safety Improvement Program (HSIP)] (http://dot.ca.gov/hq/LocalPrograms/hsip.html).

Please suggest improvements! I am by no means a professional pythoner.

### Requirements

- Python
- Geocoded collision data with the following fields:
  - Involvement of alcohol
  - Distance of the collision from the intersection
  - Movement preceding the collision for each party
  - Direction of travel for each party
- Geocoded intersections
- This script uses the arcpy cursor functions within Arcmap, but you could reformat it to match the way your data is stored.

### Preparation

1. First assign all collisions the unique ID of the nearest intersection. The end result should be an additional field in the collisions table that has the unique ID for the related intersection. This script gives more detail on our process at LADOT.
2. For the intersection data, subset out those that are not signalized.
3. For the collision data, filter out collisions involving alcohol.

### Process Diagram

  
  

![Signal Warrant Process Diagram](https://github.com/black-tea/VisionZero/blob/master/NewSignals/HSIP_CityWide_SignalWarrant_portrait.png)

