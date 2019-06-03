# Centerline and Intersection Data Cleaning

The Bureau of Street Engineering currently provides the most comprehensive street network for the City of Los Angeles. This street centerline file, and the intersection nodes generated from it, serve as the foundation for the City's data-driven approach to eliminating traffic deaths by 2025. The Department of Transporation used this python script (and manual checking) to make sure the data was well prepared for analysis.

Please suggest improvements! I am by no means a professional pythoner.

### Requirements

- Python
- [Centerline File](http://geohub.lacity.org/datasets/d3cd48afaacd4913b923fd98c6591276_36)
- [Node/Intersection File](http://geohub.lacity.org/datasets/0372aa1fb42a4e29adb9caadcfb210bb_9)
- This script uses the cursor functions within ArcPy, but you could reformat it to match the way your data is stored.

### Steps

1. The BOE Intersection file was automatically generated based on the endpoints of each of the line segments in the centerline file. This means that there will be points at dead-ends and cul-de-sacs, which do not function in the same way that other intersections function (with conflicting traffic movements), so we began by removing any points that contained 'D/E' as a value for the "TO_ST" field.
2. There are also a number of intersections with no values in the "TO_ST" or "FROM_ST," which 
3. Finally, we wanted to create a subset for those intersections that involved a freeway. Most of the time these will be intersections where a freeway ramp meets a surface street. These are fine. In some cases, however, there are "ghost" intersections: intersections that only exist in the ArcMap's 2D plane and don't actually exist in reality. This is the case for nearly all overpasses. We want to keep the ramp intersections and remove the "ghost" intersections as part of the cleaning. 

### Process Diagram

![Centerline Cleaning Process Diagram](https://github.com/black-tea/VisionZero/blob/master/CenterlineCleaning/ESRI_BuildDatabase.png)
