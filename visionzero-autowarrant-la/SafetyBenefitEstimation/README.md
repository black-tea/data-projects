# Quantifying the Estimated Safety Benefits of Engineering Changes

This script was used to estimate the safety benefit from engineering changes, specificly a road diet, which transforms a roadway that with two vehicle travel lanes in each direction into a roadway with one vehicle travel lane in each direction and a center turn lane.



Please suggest improvements! I am by no means a professional pythoner.

### General Requirements

- Python
- Geocoded collision data with the following fields:
  - Party type
  - Movement preceding the collision for each party
  - Direction of travel for each party
- A geocoded network that includes [segments] (http://geohub.lacity.org/datasets/d3cd48afaacd4913b923fd98c6591276_36) and [intersections] (http://geohub.lacity.org/datasets/0372aa1fb42a4e29adb9caadcfb210bb_9).
- This script uses the arcpy cursor functions within Arcmap, but you could reformat it to match the way your data is stored.

### Pre-Script Preparation 

1. First assign all collisions the unique IDs of the nearest segment and intersection. The end result should be two additional fields in the collisions table that has the unique IDs for the related intersection. This script gives more detail on our process at LADOT.
2. Each corridor will include a number of segments and intersections. For each corridor that will be analyzed, gather the IDs for all those segments and intersections in each corridor. In our case, we formatted it into a csv file (see below for detail).

### Script Input

- SWITRS-formatted collision data including the following tables:
  - [Collision Table] (http://geohub.lacity.org/datasets/bed43aa2945a47b18ae888246712ccb1_0) that includes fields with the IDs of the nearest intersection and segment
  - [Party Table] (http://geohub.lacity.org/datasets/8cfe25a12dca4826b6a311470c76f1ea_1)
- CSV table listing the corridor ID(s), centerline segment IDs, and travel directions of the segment (example below)

> | Corridor ID   | BOE Segment ID| Dir   |
> | ------------- |---------------| ------|
> | 1             | 2098          | E,W   |
> | 1             | 4643          | E,W   |
> | 1             | 5325          | E,W   |
> | 1             | 7135          | E,W   |
> | 1             | 12095         | E,W   |
  
- another CSV table listing the corridor ID(s) and centerline intersection IDs (example below)

> | Corridor ID   | BOE Intersection ID|
> | ------------- |--------------------|
> | 1             | 96407              |
> | 1             | 99928              |
> | 1             | 110461             |
> | 1             | 117460             |
> | 1             | 117487             |

### Process Diagram

![Safety Benefit Flowchart Diagram](https://github.com/black-tea/VisionZero/blob/master/SafetyBenefitEstimation/SafetyBenefitFlowchart.png)

