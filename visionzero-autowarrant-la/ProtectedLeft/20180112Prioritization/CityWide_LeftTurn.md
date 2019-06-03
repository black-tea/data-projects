Prioritizing Safety Improvements at LADOT
=========================================

Last year I wrote about how we at LADOT had developed a more systematic
approach to evaluating intersections for funding applications.
Previously, engineers had only been able to do bottom-up queries, that
is, look at all the data for each potential intersection, manually scrub
it for qualifying collisions, and then create summaries for each
intersection. For our effort, we created a top-down, global query among
all intersections in Los Angeles, a process explained much more
thoroughly
[here](https://black-tea.github.io/projects/2017/01/10/Data-Driven-Funding-Applications.html).

Although we initially developed that approach to optimize our funding
applications (Highway Safety Improvement Program / Metro ExpressLanes),
we’ve since broadened our use of it to help prioritize where we install
our own funded infrastructure. This example will cover protected
left-turn phasing, but we’ve also used this global query process for
several different infrastructure prioritization exercises.

As we’ve broadened our use of that approach over the past year and a
half, I’ve also become much more proficient in making these queries, and
discovered new ways to do them better. My initial solution to the
problem included a series of python for loops that fairly closely mimics
the flowchart on the project page. This past year, I much more
streamline approach to this data querying, using better tools such as
Wickam’s `dplyr` (also obviously shifting from a python-based to R-based
workflow).

### Pre-Project Data Processing

Before embarking on this project, there is one critical pre-processing
step: each of the collisions needs to be assigned to specific
intersection ID. For the intitial funding application effort, I did this
through a python script, which you can view on my [github
page](https://github.com/black-tea/VisionZero/tree/master/CenterlineCleaning).
We now have a contractor, [RoadSafe GIS](https://roadsafegis.com/),
performing this service for us. However it happens, each collision needs
to end up with an ID tag for the nearest intersection.

Once you receive the list of candidate locations for prioritizing
(unless it is a global search), you will also need to make sure that the
list includes the ID as well, for joining to the collision table.

### Data Inputs

For these prioritization efforts, I typically just need three different
tables:

-   `switrs_los_angeles.csv` The collision table, which will include the
    basic information about the collision such as the date, the severity
    of the collision, whether there was alcohol involved, and (after you
    finish the pre-processing step) the ID of the nearest intersection.
    I’ve uploaded a copy of the latest five years of collision data to
    the
    [GeoHub](http://geohub.lacity.org/datasets/a6e14c3de891457a87a1ff7159c2a5e6_0),
    the City’s GeoSpatial Open Data portal.

-   `party_los_angeles.csv` The party table, since it includes the
    direction of travel for each party and the movement directly
    preceding the location.

-   A list of candidate locations for upgrades (we are not always doing
    a citywide seach - in this case I was prioritizing a list of 58
    intersections for improved phasing).

For interpreting the data within the collision and party table, we’ve
uploaded the SWITRS codebook
[here](http://visionzero.lacity.org/wp-content/uploads/2016/09/SWITRS_codebook.pdf).
We can begin by loading these data:

    # Load Libraries
    library(tidyverse)

    ## -- Attaching packages --------------------------------------------------------------- tidyverse 1.2.1 --

    ## v ggplot2 2.2.1.9000     v purrr   0.2.4     
    ## v tibble  1.4.1          v dplyr   0.7.4     
    ## v tidyr   0.7.2          v stringr 1.2.0     
    ## v readr   1.1.1          v forcats 0.2.0

    ## -- Conflicts ------------------------------------------------------------------ tidyverse_conflicts() --
    ## x dplyr::filter() masks stats::filter()
    ## x dplyr::lag()    masks stats::lag()

    # Import Data
    collisions <- read.csv('collisions_los_angeles.csv',
                           header = TRUE,
                           stringsAsFactors = FALSE)
    parties <- read.csv('party_los_angeles.csv',
                        header = TRUE,
                        stringsAsFactors = FALSE)
    candidate_int <- read.csv('Vision Zero Signals - LT Crash Analysis.csv',
                              header = TRUE,
                              stringsAsFactors = FALSE)

### Protected L-Turn Criteria

-   Filter to latest 5 years (already complete for these data)

-   Collision involves a Left or U-Turn

-   No alcohol was involved in the collision

That is it! Pretty simple. In addition to the above criteria, I will
separate out the collision counts by direction, since that is how we
will be evaluating the protected phasing candidates.

### Clean & Filter the data

Here I perform some basic filters on each of the data sets and subset
for only the variables I am interested in. Very straightforward `dplyr`
work.

    # Clean and Reformat Data Tables
    candidate_int <- candidate_int %>%
      select(Int, Primary.Street, X.Street, Phasing.Type)
      
    collisions <- collisions %>%
      # select only relevant variables
      select(case_id, accident_year, collision_severity, alcohol_involved, int_id) %>%
      # remove any collisions where alcohol was involved
      filter(alcohol_involved != 'Y') %>%
      # only intersted in collision history at candidate locations
      filter(int_id %in% candidate_int$Int) %>%
      # also remove collisions without an assigned intersection
      filter(!is.na(int_id)) %>%
      # drop alcohol_involved variable after filter
      select(-alcohol_involved)

    parties <- parties %>%
      # Select only relevant variables
      select(case_id, party_number, party_type, dir_of_travel, move_pre_acc) %>%
      # Filter for parties that are in the collision table
      filter(case_id %in% collisions$case_id) %>%
      # also filter out parties that do not have a direction of travel
      filter(dir_of_travel %in% c('E','N','S','W')) %>%
      # Join to collision table to get collision_severity
      left_join(collisions) %>%
      # Group by Case ID,
      group_by(case_id) %>%
      # Select only those case IDs where there was at least one person making a 'U' or 'L' turn
      filter(any(move_pre_acc %in% c('E','F'))) %>%
      # Select only collisions involving at least two parties
      filter(n() >= 2) %>%
      ungroup() 

    ## Joining, by = "case_id"

The parties table is the one we are ultimately interested in. After
we’ve joined the collision table to the party table (and completed all
the necessary cleaning), we can take a look:

    glimpse(parties)

    ## Observations: 1,926
    ## Variables: 8
    ## $ case_id            <chr> "6275418", "5606216", "7138013", "6504095",...
    ## $ party_number       <int> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2...
    ## $ party_type         <chr> "1", "1", "1", "1", "1", "1", "1", "1", "1"...
    ## $ dir_of_travel      <chr> "E", "W", "S", "S", "N", "S", "E", "N", "E"...
    ## $ move_pre_acc       <chr> "E", "F", "E", "E", "E", "E", "E", "E", "E"...
    ## $ accident_year      <int> 2013, 2012, 2015, 2014, 2015, 2014, 2014, 2...
    ## $ collision_severity <int> 4, 0, 3, 3, 4, 3, 3, 4, 4, 4, 2, 3, 3, 0, 4...
    ## $ int_id             <int> 96407, 106643, 131554, 110127, 138221, 1382...

### Generate Summary Counts

Once we have all the appropriate filters, it is quite easy to summarize
the data and create a table that includes these counts. For this
exercise, for example, I created three different tables for
prioritization:

-   Count of collisions where people were Killed or Severely Injured
    (KSI)

-   Count of collisions where there was at least someone who complained
    of pain (excluding property-damange-only collisions)

-   Count of all collisions

<!-- -->

    # Generate summary count tables
    ksi_count <- parties %>%
      # Filter for KSIs
      filter(collision_severity %in% c(1,2)) %>%
      # Filter by movement again
      filter(move_pre_acc %in% c('E','F')) %>%
      group_by(int_id, dir_of_travel) %>%
      summarise(ksi.ct = n())

    inj_count <- parties %>%
      # Filter for all injuries (excluding collision_severity = 0)
      filter(collision_severity %in% c(1,2,3,4)) %>%
      filter(move_pre_acc %in% c('E','F')) %>%
      group_by(int_id, dir_of_travel) %>%
      summarise(inj.ct = n()) 

    col_count <- parties %>%
      filter(move_pre_acc %in% c('E','F')) %>%
      group_by(int_id, dir_of_travel) %>%
      summarise(col.ct = n()) 

Once we have all three tables, we can merge them into one giant table
for a summary. However, first I am going to reshape the tables using the
`spread` function from `tidyr` which takes ‘long’ tables (long in this
case from breaking out every intersection by direction) and converting
them to ‘wide’ tables (in this case, converting the directions to be
columns). Each value will be the count for that interesction ID and
direction.

    # Generate summary count tables
    ksi_summary <- ksi_count %>%
      spread(dir_of_travel, ksi.ct, fill = NA, convert = FALSE) %>%
      rename(ksi.E = 'E',
             ksi.N = 'N',
             ksi.S = 'S',
             ksi.W = 'W')

    inj_summary <- inj_count %>%
      spread(dir_of_travel, inj.ct, fill = NA, convert = FALSE) %>%
      rename(inj.E = 'E',
             inj.N = 'N',
             inj.S = 'S',
             inj.W = 'W')

    col_summary <- col_count %>%
      spread(dir_of_travel, col.ct, fill = NA, convert = FALSE) %>%
      rename(col.E = 'E',
             col.N = 'N',
             col.S = 'S',
             col.W = 'W')

Now we can merge all three ‘tables’ into one really wide table.

    summary <- ksi_summary %>%
      full_join(inj_summary) %>%
      full_join(col_summary) 

    ## Joining, by = "int_id"
    ## Joining, by = "int_id"

And take a glimpse of the final output

    print(summary)

    ## # A tibble: 57 x 13
    ## # Groups: int_id [?]
    ##    int_id ksi.E ksi.N ksi.S ksi.W inj.E inj.N inj.S inj.W col.E col.N
    ##     <int> <int> <int> <int> <int> <int> <int> <int> <int> <int> <int>
    ##  1  96010    NA    NA     1    NA     2     3     6     1     2     3
    ##  2  96370    NA     1    NA    NA     3     9     8     1     5    11
    ##  3  96407    NA    NA     2    NA     3     8    13     3     5     9
    ##  4  99928    NA    NA     1    NA     2     2     1     1     2     3
    ##  5 100373     1    NA    NA    NA     2    NA     2     1     2    NA
    ##  6 106686    NA     1     1     1     6     6    10    11     6     6
    ##  7 106837     1    NA    NA    NA     4     1     8     5     4     1
    ##  8 109525    NA    NA    NA     1     5     3     2     6     6     3
    ##  9 110084    NA     1    NA    NA     3     5     2     4     5     6
    ## 10 113863    NA    NA    NA     1     2    13     7     7     2    13
    ## # ... with 47 more rows, and 2 more variables: col.S <int>, col.W <int>
