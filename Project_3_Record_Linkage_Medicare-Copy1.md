# Record linkage

Python's record linkage toolkit is a powerful technique used to merge multiple large datasets together. 

It is really useful when there is no common ID/ unique key or identifier to join the tables on, or when we're facin messy data (e.g. many values have typos or different spellings). 

When there's no common key to join tables on, you might want to merge two datasets based on name and address details for example (for employees, customers, instiutions etc) in order to extract useful insights from a combination of the data in a few tables. The name and/or address may match perfectly but often there isn't an exact match and so you'll need to be able to match records based on imperfect data (such as typos, spelling inconsistencies etc.). 

This is also a useful method to uncover duplicates and remove duplicate records from the joint master table.

At a high level, the record linkage workflow involves the following steps:

1. Cleaning /pre-processing the data - done with the generic fucntion recordlinkage.preprocessing.clean() and used to standardize and clean text data such as transforming all records to lower case or removing brackets, spaces and symbols from names, phone numbers etc. 

2. Indexing - Generating pairs of potentially matching records from both tables.

3. Comparing the pairs by calculating their similarity.

4. Classifying - Scoring the pairs based on similarity levels.

5. Evaluating - Filtering out pairs which don't match or which have low similarity scores and extracting matching records.

5. Linking the tables based on matching records.

The record linkage toolkit documentation with helpful datasets and code examples, can be found here: https://recordlinkage.readthedocs.io/en/latest/about.html




# The data


In this example, I'll set out how to link tables by using the recordlinkage package. 

The recordlinkage package essentially helps us:

- identify matching records in another table by calculating the similarity between multiple pairs of string records; and 
- join two datasets into one clean master dataset that we can perform calculations on in order to draw business insights.

To demonstrate the record linkage technique I'll use U.S. hospital data in two separate datasets which we'll be matching and linking based on hospital name and address (assuming there's no common key to join the tables on).

US hospital data is a good candidate for this exercise because many hospitals have similar names across different cities (e.g. "Saint Lukes", "Saint Mary", "Community Hospital"). Also, addresses can be ambiguous because, in urban areas, hospitals can occupy several city blocks.  Because there are tens of thousands of medical facilities in the US, the data's scale is quite large, which makes the problem even more challenging.

We'll be looking at two data sets:

1. "Hospital general information" - a data set that contains basic details such as hospital number, name, address, State, hospital type and ownership information. This table can be found at:https://data.medicare.gov/Hospital-Compare/Hospital-General-Information/xubh-q36u and has 5,314 rows and 29 columns.

2. "Inpatient Medicare Charge Data" - a dataset that contains hospital basic information (name, address), together with the total number of discharges and various Medicare payments received by that hospital (in relation to patients treated for a specific heart medical condition). The data can be found at:https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Inpatient2016 and has 197,283 rows and 12 columns.

Note: Joining the two datasets would enable further analysis on the payment data per hospital (set out at the end of this exercise to demonstrate what analysis could be performed on the joined data) but this exercise will mainly focus on the record linkage technique, data cleaning and joining techniques. Let's get started!


# 1. Pre-processing the data

The record linkage indexing module is basically used to make pairs of records. These pairs are called "candidate links" or "candidate matches".

We'll start by:
- importing the toolkit, 
- creating dataframes from our tables, and 
- creating an indexing object to store our candidate mathces.


```python
#import recordlinkage

import pandas as pd
import recordlinkage

#read the data

hospitals = pd.read_csv('Hospital_General_Information.csv')
hospital_charges = pd.read_csv('Medicare_Provider_Charge_Inpatient_DRGALL_FY2016 2.csv')
        
```

    /Users/racheldulberg/opt/anaconda3/lib/python3.8/site-packages/IPython/core/interactiveshell.py:3071: DtypeWarning: Columns (8) have mixed types.Specify dtype option on import or set low_memory=False.
      has_raised = await self.run_ast_nodes(code_ast.body, cell_name,



```python
#check the header for each dataframe

hospitals.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Facility ID</th>
      <th>Facility Name</th>
      <th>Address</th>
      <th>City</th>
      <th>State</th>
      <th>ZIP Code</th>
      <th>County Name</th>
      <th>Phone Number</th>
      <th>Hospital Type</th>
      <th>Hospital Ownership</th>
      <th>...</th>
      <th>Readmission national comparison footnote</th>
      <th>Patient experience national comparison</th>
      <th>Patient experience national comparison footnote</th>
      <th>Effectiveness of care national comparison</th>
      <th>Effectiveness of care national comparison footnote</th>
      <th>Timeliness of care national comparison</th>
      <th>Timeliness of care national comparison footnote</th>
      <th>Efficient use of medical imaging national comparison</th>
      <th>Efficient use of medical imaging national comparison footnote</th>
      <th>Location</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>044022</td>
      <td>CONWAY BEHAVIORAL HEALTH</td>
      <td>2255 STURGIS ROAD</td>
      <td>CONWAY</td>
      <td>AR</td>
      <td>72034</td>
      <td>FAULKNER</td>
      <td>(501) 205-0011</td>
      <td>Psychiatric</td>
      <td>Proprietary</td>
      <td>...</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>054154</td>
      <td>SAN JOSE BEHAVIORAL HEALTH</td>
      <td>455 SILICON VALLEY BOULEVARD</td>
      <td>SAN JOSE</td>
      <td>CA</td>
      <td>95138</td>
      <td>SANTA CLARA</td>
      <td>(669) 234-5959</td>
      <td>Psychiatric</td>
      <td>Proprietary</td>
      <td>...</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>POINT (-121.77291599999998 37.246742)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>121300</td>
      <td>KAUAI VETERANS MEMORIAL HOSPITAL</td>
      <td>4643 WAIMEA CANYON DRIVE</td>
      <td>WAIMEA</td>
      <td>HI</td>
      <td>96796</td>
      <td>KAUAI</td>
      <td>(808) 338-9431</td>
      <td>Critical Access Hospitals</td>
      <td>Government - State</td>
      <td>...</td>
      <td>5.0</td>
      <td>Not Available</td>
      <td>16.0</td>
      <td>Same as the national average</td>
      <td>NaN</td>
      <td>Same as the national average</td>
      <td>NaN</td>
      <td>Not Available</td>
      <td>16.0</td>
      <td>POINT (-159.67005500000002 21.960079)</td>
    </tr>
    <tr>
      <th>3</th>
      <td>141328</td>
      <td>HARDIN COUNTY GENERAL HOSPITAL &amp; CLINIC</td>
      <td>FERRELL ROAD</td>
      <td>ROSICLARE</td>
      <td>IL</td>
      <td>62982</td>
      <td>HARDIN</td>
      <td>(618) 285-6634</td>
      <td>Critical Access Hospitals</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>NaN</td>
      <td>Not Available</td>
      <td>16.0</td>
      <td>Not Available</td>
      <td>16.0</td>
      <td>Not Available</td>
      <td>5.0</td>
      <td>Not Available</td>
      <td>5.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>154060</td>
      <td>PARK CENTER, INC</td>
      <td>1909 CAREW STREET</td>
      <td>FORT WAYNE</td>
      <td>IN</td>
      <td>46805</td>
      <td>ALLEN</td>
      <td>(260) 481-2700</td>
      <td>Psychiatric</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>Not Available</td>
      <td>19.0</td>
      <td>POINT (-85.108024 41.093791)</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 29 columns</p>
</div>




```python
#check the header for each dataframe

hospital_charges.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DRG Definition</th>
      <th>Provider Id</th>
      <th>Provider Name</th>
      <th>Provider Street Address</th>
      <th>Provider City</th>
      <th>Provider State</th>
      <th>Provider Zip Code</th>
      <th>Hospital Referral Region (HRR) Description</th>
      <th>Total Discharges</th>
      <th>Average Covered Charges</th>
      <th>Average Total Payments</th>
      <th>Average Medicare Payments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>10033</td>
      <td>UNIVERSITY OF ALABAMA HOSPITAL</td>
      <td>619 SOUTH 19TH STREET</td>
      <td>BIRMINGHAM</td>
      <td>AL</td>
      <td>35233</td>
      <td>AL - Birmingham</td>
      <td>13</td>
      <td>$1,016,806.46</td>
      <td>$296,937.00</td>
      <td>$150,139.69</td>
    </tr>
    <tr>
      <th>1</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>30103</td>
      <td>MAYO CLINIC HOSPITAL</td>
      <td>5777 EAST MAYO BOULEVARD</td>
      <td>PHOENIX</td>
      <td>AZ</td>
      <td>85054</td>
      <td>AZ - Phoenix</td>
      <td>26</td>
      <td>$443,387.54</td>
      <td>$215,059.54</td>
      <td>$163,889.31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>40114</td>
      <td>BAPTIST HEALTH MEDICAL CENTER-LITTLE ROCK</td>
      <td>9601 INTERSTATE 630, EXIT 7</td>
      <td>LITTLE ROCK</td>
      <td>AR</td>
      <td>72205</td>
      <td>AR - Little Rock</td>
      <td>33</td>
      <td>$711,472.00</td>
      <td>$180,315.55</td>
      <td>$145,192.61</td>
    </tr>
    <tr>
      <th>3</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>50025</td>
      <td>UC SAN DIEGO HEALTH HILLCREST - HILLCREST MED CTR</td>
      <td>200 WEST ARBOR DRIVE</td>
      <td>SAN DIEGO</td>
      <td>CA</td>
      <td>92103</td>
      <td>CA - San Diego</td>
      <td>17</td>
      <td>$796,343.82</td>
      <td>$299,244.41</td>
      <td>$270,131.59</td>
    </tr>
    <tr>
      <th>4</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>50100</td>
      <td>SHARP MEMORIAL HOSPITAL</td>
      <td>7901 FROST ST</td>
      <td>SAN DIEGO</td>
      <td>CA</td>
      <td>92123</td>
      <td>CA - San Diego</td>
      <td>13</td>
      <td>$1,434,651.46</td>
      <td>$239,537.46</td>
      <td>$215,205.00</td>
    </tr>
  </tbody>
</table>
</div>



It would also be helpful to check the data types in each data frame to ensure the types for names and addresses (including city, state and zipcodes) are aligned in both data frames. It looks like these values are all strings (noted below as 'object') except for the zipcode which is an integer in both tables. This all looks good.


```python
#output data types

hospitals.dtypes
```




    Facility ID                                                       object
    Facility Name                                                     object
    Address                                                           object
    City                                                              object
    State                                                             object
    ZIP Code                                                           int64
    County Name                                                       object
    Phone Number                                                      object
    Hospital Type                                                     object
    Hospital Ownership                                                object
    Emergency Services                                                object
    Meets criteria for promoting interoperability of EHRs             object
    Hospital overall rating                                           object
    Hospital overall rating footnote                                 float64
    Mortality national comparison                                     object
    Mortality national comparison footnote                           float64
    Safety of care national comparison                                object
    Safety of care national comparison footnote                      float64
    Readmission national comparison                                   object
    Readmission national comparison footnote                         float64
    Patient experience national comparison                            object
    Patient experience national comparison footnote                  float64
    Effectiveness of care national comparison                         object
    Effectiveness of care national comparison footnote               float64
    Timeliness of care national comparison                            object
    Timeliness of care national comparison footnote                  float64
    Efficient use of medical imaging national comparison              object
    Efficient use of medical imaging national comparison footnote    float64
    Location                                                          object
    dtype: object




```python
#check DF basic info (cols, num of rows, data types, memory usage etc)

hospitals.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 5314 entries, 0 to 5313
    Data columns (total 29 columns):
     #   Column                                                         Non-Null Count  Dtype  
    ---  ------                                                         --------------  -----  
     0   Facility ID                                                    5314 non-null   object 
     1   Facility Name                                                  5314 non-null   object 
     2   Address                                                        5314 non-null   object 
     3   City                                                           5314 non-null   object 
     4   State                                                          5314 non-null   object 
     5   ZIP Code                                                       5314 non-null   int64  
     6   County Name                                                    5314 non-null   object 
     7   Phone Number                                                   5314 non-null   object 
     8   Hospital Type                                                  5314 non-null   object 
     9   Hospital Ownership                                             5314 non-null   object 
     10  Emergency Services                                             5314 non-null   object 
     11  Meets criteria for promoting interoperability of EHRs          3763 non-null   object 
     12  Hospital overall rating                                        5314 non-null   object 
     13  Hospital overall rating footnote                               1814 non-null   float64
     14  Mortality national comparison                                  5314 non-null   object 
     15  Mortality national comparison footnote                         1983 non-null   float64
     16  Safety of care national comparison                             5314 non-null   object 
     17  Safety of care national comparison footnote                    2712 non-null   float64
     18  Readmission national comparison                                5314 non-null   object 
     19  Readmission national comparison footnote                       1596 non-null   float64
     20  Patient experience national comparison                         5314 non-null   object 
     21  Patient experience national comparison footnote                1965 non-null   float64
     22  Effectiveness of care national comparison                      5314 non-null   object 
     23  Effectiveness of care national comparison footnote             2043 non-null   float64
     24  Timeliness of care national comparison                         5314 non-null   object 
     25  Timeliness of care national comparison footnote                1483 non-null   float64
     26  Efficient use of medical imaging national comparison           5314 non-null   object 
     27  Efficient use of medical imaging national comparison footnote  2257 non-null   float64
     28  Location                                                       4910 non-null   object 
    dtypes: float64(8), int64(1), object(20)
    memory usage: 1.2+ MB



```python
hospital_charges.dtypes
```




    DRG Definition                                object
    Provider Id                                    int64
    Provider Name                                 object
    Provider Street Address                       object
    Provider City                                 object
    Provider State                                object
    Provider Zip Code                              int64
    Hospital Referral Region (HRR) Description    object
    Total Discharges                              object
    Average Covered Charges                       object
    Average Total Payments                        object
    Average Medicare Payments                     object
    dtype: object




```python
hospital_charges.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 197283 entries, 0 to 197282
    Data columns (total 12 columns):
     #   Column                                      Non-Null Count   Dtype 
    ---  ------                                      --------------   ----- 
     0   DRG Definition                              197283 non-null  object
     1   Provider Id                                 197283 non-null  int64 
     2   Provider Name                               197283 non-null  object
     3   Provider Street Address                     197283 non-null  object
     4   Provider City                               197283 non-null  object
     5   Provider State                              197283 non-null  object
     6   Provider Zip Code                           197283 non-null  int64 
     7   Hospital Referral Region (HRR) Description  197283 non-null  object
     8   Total Discharges                            197283 non-null  object
     9   Average Covered Charges                     197283 non-null  object
     10  Average Total Payments                      197283 non-null  object
     11  Average Medicare Payments                   197283 non-null  object
    dtypes: int64(2), object(10)
    memory usage: 18.1+ MB


# 2. Indexing & blocking - generating pairs of potentially matching records

It is pretty intuitive to compare each record in DataFrame A ('hospitals') with all records of DataFrame B ('hospital_charges'). We want to make record pairs - each record pair should contain one record of DF A and one record of DF B. The process of making record pairs is also called ‘indexing’. 

With the recordlinkage module, indexing is very easy. First, load the index.Index class. Note that if you call the .full method, this object generates a full index on a .index(...) call. In case of deduplication of a single dataframe, one dataframe is sufficient as argument.However, in this case, as we're comparing two DFs, we insert both as arguments as you can see below.

One of the most well known and efficient indexing methods used below is called "blocking". This method includes only record pairs that are identical on one or more stored attributes or variables of the person or entity (so, below we're blocking on the state, city and zipcode variables because we're probably only interested in hospitals that have an exact match on these attributes in both tables). The blocking method can be used in the recordlinkage module.

Because those variables have slightly different column names in each table, we need to state those column names exactly as they are and so we're using the "left_on" and "right_on" arguments to refer to the column names in each table.

As you can see below, it is possible to parse a list of columns names to block on multiple variables. Blocking on multiple variables will reduce the number of record pairs even further, and so we've done this here.

Note: Another indexing method is called 'Sorted Neighbourhood Indexing' (recordlinkage.index.SortedNeighbourhood). This method is handy when there are many misspellings in the strings used for indexing. We've not used this here as our data doesn't seem to have spelling issues but it's worth bearing this method in mind. See the documentation for details about sorted neighbourd indexing and there's a useful tutorial here: https://uwaterloo.ca/networks-lab/blog/post/sorted-neighbourhood-indexing-recordlinkage.



```python
#create indexing object

indexer = recordlinkage.Index()

#block on State, City & Zip Code in order to limit the number of comparisons and speed up the process

indexer.block(left_on=['State','City', 'ZIP Code'], right_on=['Provider State', 'Provider City', 'Provider Zip Code'])

#check how many match candidates there are

candidates = indexer.index(hospitals, hospital_charges)
print(len(candidates))

```

    268669


Indexing returns a pandas.MultiIndex with record pairs. Each record pair contains the index labels of two records, one from each table, as you can see below. As you can see above, it looks like we have over 268,000 potential matching candidates in our data (even after blocking for the state, city and zipcode variables).


```python
#Display the candidate pairs

print(candidates)
```

    MultiIndex([(   0,   3188),
                (   0,   6126),
                (   0,   7798),
                (   0,   9849),
                (   0,  11264),
                (   0,  22230),
                (   0,  23078),
                (   0,  24250),
                (   0,  26054),
                (   0,  29017),
                ...
                (5312, 158030),
                (5312, 159027),
                (5312, 161272),
                (5312, 164381),
                (5312, 166521),
                (5312, 169379),
                (5312, 174349),
                (5312, 182383),
                (5312, 185173),
                (5312, 190596)],
               length=268669)


# 3. Comparing record pairs/calculating similarity

We now need to classify the record pairs into "matching" and "distinct" (non-matching) pairs. The recordlinkage.Compare class and its methods can be used to compare records pairs. The Compare class has methods like "string", "exact" and "numeric" to initialise the comparing of the records. The "compute" method is then used to start the actual comparing.

Each record pair is a candidate match. To classify the candidate record pairs into matches and non-matches, we compare the records on all attributes both records have in common. 

In this example, it would make sense to look for:
- exact matches for the 'State' variable; and 
- similar matches for pairs of 'Name' and 'address' (as it's possible there will be typos or minor differences/inconsistencies in hospital names and addresses between two matching records).  

For the .string method, which computes the (partial) similarity between strings values, we can set a threshold value (commonly set as 0.85). All approximate string comparisons higher than or equal to this threshold will appear as "1". Otherwise, if the similarity is below the threshold, it will compute as "0".

Likewise for exact matches - no match will be calculated as '0' and exact match is '1'.


```python
#initialise index object
indexer = recordlinkage.Index()

#set up a block with relevant variables
indexer.block(left_on=['State','City', 'ZIP Code'], right_on=['Provider State', 'Provider City', 'Provider Zip Code'])

#generate candidate pairs
pairs=indexer.index(hospitals, hospital_charges)

#create a Compare object
compare = recordlinkage.Compare()

#find exact matches of State 
compare.exact('State', 'Provider State', label='State')

#find similar matches of hospital name and address
compare.string('Facility Name', 'Provider Name', threshold=0.85, label='Name')
compare.string('Address', 'Provider Street Address', threshold=0.85, label='Address')

#compute the comparison between the two data frames
results=compare.compute(pairs, hospitals, hospital_charges)

#display results
results
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>State</th>
      <th>Name</th>
      <th>Address</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">0</th>
      <th>3188</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>6126</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>7798</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>9849</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>11264</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>...</th>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th rowspan="5" valign="top">5312</th>
      <th>169379</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>174349</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>182383</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>185173</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>190596</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
<p>268669 rows × 3 columns</p>
</div>




```python
#verify the results
results.info()
```

    <class 'pandas.core.frame.DataFrame'>
    MultiIndex: 268669 entries, (0, 3188) to (5312, 190596)
    Data columns (total 3 columns):
     #   Column   Non-Null Count   Dtype  
    ---  ------   --------------   -----  
     0   State    268669 non-null  int64  
     1   Name     268669 non-null  float64
     2   Address  268669 non-null  float64
    dtypes: float64(2), int64(1)
    memory usage: 9.2 MB


# 4. Classifying - Scoring the pairs based on similarity levels

Note that the results of the comparison display multiple potential matches for each row index in the first table(hospitals) against a row index in the second table (hospital_charges). The similarity scores under each of the compared columns is either 0 (no match) or 1 (match) for each row (see below).

The probable matches we're after would likely be in the rows with the highest similarity score across all three columns/variables - ie those that have scored 1 for each of the compared columns and so match on State, Name and Address. The sum of the similarity scores (per row) should therefore be 3 (ie 1+1+1) for rows we are interested in as probable matches.


```python
#display comparison results
results
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>State</th>
      <th>Name</th>
      <th>Address</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">0</th>
      <th>3188</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>6126</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>7798</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>9849</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>11264</th>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>...</th>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th rowspan="5" valign="top">5312</th>
      <th>169379</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>174349</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>182383</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>185173</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>190596</th>
      <td>1</td>
      <td>1.0</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
<p>268669 rows × 3 columns</p>
</div>



# 5. Evaluating - Filtering out non matching pairs 

Given we have over 260,000 potential matches, it would be useful to calculate the number of records per score to get a high level view of how many "good" matches we have in the data. We can run the value_counts() function below to achieve that. This tells us there are 151,705 potential matches (and around 40,000 pairs that only match on 2 variables and 76,169 pairs that only match on 1 variable).


```python
#Calculate count of records per total row score
results.sum(axis=1).value_counts().sort_index(ascending=False)
```




    3.0    151705
    2.0     40795
    1.0     76169
    dtype: int64




```python
#get only probable matches
matches=results[results.sum(axis=1)>=3]

#check the classification worked
print(matches)
```

                 State  Name  Address
    11   2419        1   1.0      1.0
         2547        1   1.0      1.0
         2571        1   1.0      1.0
         3706        1   1.0      1.0
         7163        1   1.0      1.0
    ...            ...   ...      ...
    5312 169379      1   1.0      1.0
         174349      1   1.0      1.0
         182383      1   1.0      1.0
         185173      1   1.0      1.0
         190596      1   1.0      1.0
    
    [151705 rows x 3 columns]


We can check the matches by printing a record from each of the source tables based on the record indexes noted above. 

As you can see below, the records are a good match. This is also an example of recordlinkage matching multiple records from the hospital_charges table to a single record in the hospitals table. This makes sense as the number of rows in the hospital charges table is much larger than the number of rows in the hospitals table (so this is a one to many relathionship). For example, each record from the hospital_charges table matched with "Duke Health Raleigh Hospital" relates to a separate treatment at this hospital and shows different costs.


```python
#verify record details from DF A matches record details from DF B
hospitals.loc[11,:]
```




    Facility ID                                                                                                 340073
    Facility Name                                                                         DUKE HEALTH RALEIGH HOSPITAL
    Address                                                                                        3400 WAKE FOREST RD
    City                                                                                                       RALEIGH
    State                                                                                                           NC
    ZIP Code                                                                                                     27609
    County Name                                                                                                   WAKE
    Phone Number                                                                                        (919) 954-3000
    Hospital Type                                                                                 Acute Care Hospitals
    Hospital Ownership                                                                  Voluntary non-profit - Private
    Emergency Services                                                                                             Yes
    Meets criteria for promoting interoperability of EHRs                                                            Y
    Hospital overall rating                                                                                          5
    Hospital overall rating footnote                                                                               NaN
    Mortality national comparison                                                         Same as the national average
    Mortality national comparison footnote                                                                         NaN
    Safety of care national comparison                                                      Above the national average
    Safety of care national comparison footnote                                                                    NaN
    Readmission national comparison                                                         Above the national average
    Readmission national comparison footnote                                                                       NaN
    Patient experience national comparison                                                Same as the national average
    Patient experience national comparison footnote                                                                NaN
    Effectiveness of care national comparison                                             Same as the national average
    Effectiveness of care national comparison footnote                                                             NaN
    Timeliness of care national comparison                                                  Below the national average
    Timeliness of care national comparison footnote                                                                NaN
    Efficient use of medical imaging national comparison                                    Above the national average
    Efficient use of medical imaging national comparison footnote                                                  NaN
    Location                                                                              POINT (-78.619549 35.828998)
    Hospitals_Name_Lookup                                            DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...
    Hosp_Name_Lookup                                                 DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...
    Name: 11, dtype: object




```python
hospital_charges.loc[2419,:]
```




    DRG Definition                                027 - CRANIOTOMY & ENDOVASCULAR INTRACRANIAL P...
    Provider Id                                                                              340073
    Provider Name                                                      DUKE HEALTH RALEIGH HOSPITAL
    Provider Street Address                                                     3400 WAKE FOREST RD
    Provider City                                                                           RALEIGH
    Provider State                                                                               NC
    Provider Zip Code                                                                         27609
    Hospital Referral Region (HRR) Description                                         NC - Raleigh
    Total Discharges                                                                             19
    Average Covered Charges                                                              $82,522.37
    Average Total Payments                                                               $19,446.89
    Average Medicare Payments                                                            $10,607.53
    Charges_Name_Lookup                           DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...
    Name: 2419, dtype: object




```python
hospital_charges.loc[2547,:]
```




    DRG Definition                                029 - SPINAL PROCEDURES W CC OR SPINAL NEUROST...
    Provider Id                                                                              340073
    Provider Name                                                      DUKE HEALTH RALEIGH HOSPITAL
    Provider Street Address                                                     3400 WAKE FOREST RD
    Provider City                                                                           RALEIGH
    Provider State                                                                               NC
    Provider Zip Code                                                                         27609
    Hospital Referral Region (HRR) Description                                         NC - Raleigh
    Total Discharges                                                                             13
    Average Covered Charges                                                              $56,765.31
    Average Total Payments                                                               $17,527.00
    Average Medicare Payments                                                            $16,245.15
    Charges_Name_Lookup                           DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...
    Name: 2547, dtype: object




```python
hospital_charges.loc[2571,:]
```




    DRG Definition                                               030 - SPINAL PROCEDURES W/O CC/MCC
    Provider Id                                                                              340073
    Provider Name                                                      DUKE HEALTH RALEIGH HOSPITAL
    Provider Street Address                                                     3400 WAKE FOREST RD
    Provider City                                                                           RALEIGH
    Provider State                                                                               NC
    Provider Zip Code                                                                         27609
    Hospital Referral Region (HRR) Description                                         NC - Raleigh
    Total Discharges                                                                             16
    Average Covered Charges                                                              $81,039.75
    Average Total Payments                                                               $11,124.56
    Average Medicare Payments                                                             $9,733.56
    Charges_Name_Lookup                           DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...
    Name: 2571, dtype: object



# 6. Linking the tables based on matching records


```python
#get row indexes for those rows in the "hospital_charges" DF that are included in the "matches" DF as probable matches

hosp_charg_rows = matches.index.get_level_values(1)

print(hosp_charg_rows)
```

    Int64Index([  2419,   2547,   2571,   3706,   7163,   9079,  10704,  12206,
                 14211,  20921,
                ...
                158030, 159027, 161272, 164381, 166521, 169379, 174349, 182383,
                185173, 190596],
               dtype='int64', length=151705)



```python
#find the above matching rows in the source hospital_charges DF and extract them into a new DF

new_hospital_charges = hospital_charges[hospital_charges.index.isin(hosp_charg_rows)]
```


```python
#verify what the new DF looks like (check total row numbers match the length of the rows index above)

new_hospital_charges
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DRG Definition</th>
      <th>Provider Id</th>
      <th>Provider Name</th>
      <th>Provider Street Address</th>
      <th>Provider City</th>
      <th>Provider State</th>
      <th>Provider Zip Code</th>
      <th>Hospital Referral Region (HRR) Description</th>
      <th>Total Discharges</th>
      <th>Average Covered Charges</th>
      <th>Average Total Payments</th>
      <th>Average Medicare Payments</th>
      <th>Charges_Name_Lookup</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>10033</td>
      <td>UNIVERSITY OF ALABAMA HOSPITAL</td>
      <td>619 SOUTH 19TH STREET</td>
      <td>BIRMINGHAM</td>
      <td>AL</td>
      <td>35233</td>
      <td>AL - Birmingham</td>
      <td>13</td>
      <td>$1,016,806.46</td>
      <td>$296,937.00</td>
      <td>$150,139.69</td>
      <td>UNIVERSITY OF ALABAMA HOSPITAL_619 SOUTH 19TH ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>30103</td>
      <td>MAYO CLINIC HOSPITAL</td>
      <td>5777 EAST MAYO BOULEVARD</td>
      <td>PHOENIX</td>
      <td>AZ</td>
      <td>85054</td>
      <td>AZ - Phoenix</td>
      <td>26</td>
      <td>$443,387.54</td>
      <td>$215,059.54</td>
      <td>$163,889.31</td>
      <td>MAYO CLINIC HOSPITAL_5777 EAST MAYO BOULEVARD_...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>50025</td>
      <td>UC SAN DIEGO HEALTH HILLCREST - HILLCREST MED CTR</td>
      <td>200 WEST ARBOR DRIVE</td>
      <td>SAN DIEGO</td>
      <td>CA</td>
      <td>92103</td>
      <td>CA - San Diego</td>
      <td>17</td>
      <td>$796,343.82</td>
      <td>$299,244.41</td>
      <td>$270,131.59</td>
      <td>UC SAN DIEGO HEALTH HILLCREST - HILLCREST MED ...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>50100</td>
      <td>SHARP MEMORIAL HOSPITAL</td>
      <td>7901 FROST ST</td>
      <td>SAN DIEGO</td>
      <td>CA</td>
      <td>92123</td>
      <td>CA - San Diego</td>
      <td>13</td>
      <td>$1,434,651.46</td>
      <td>$239,537.46</td>
      <td>$215,205.00</td>
      <td>SHARP MEMORIAL HOSPITAL_7901 FROST ST_SAN DIEG...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>001 - HEART TRANSPLANT OR IMPLANT OF HEART ASS...</td>
      <td>50108</td>
      <td>SUTTER MEDICAL CENTER, SACRAMENTO</td>
      <td>2825 CAPITOL AVENUE</td>
      <td>SACRAMENTO</td>
      <td>CA</td>
      <td>95816</td>
      <td>CA - Sacramento</td>
      <td>11</td>
      <td>$846,688.27</td>
      <td>$259,930.18</td>
      <td>$257,317.55</td>
      <td>SUTTER MEDICAL CENTER, SACRAMENTO_2825 CAPITOL...</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>197276</th>
      <td>988 - NON-EXTENSIVE O.R. PROC UNRELATED TO PRI...</td>
      <td>520098</td>
      <td>UNIVERSITY OF WI  HOSPITALS &amp; CLINICS AUTHORITY</td>
      <td>600 HIGHLAND AVENUE</td>
      <td>MADISON</td>
      <td>WI</td>
      <td>53792</td>
      <td>WI - Madison</td>
      <td>21</td>
      <td>$42,186.62</td>
      <td>$18,017.57</td>
      <td>$12,236.29</td>
      <td>UNIVERSITY OF WI  HOSPITALS &amp; CLINICS AUTHORIT...</td>
    </tr>
    <tr>
      <th>197277</th>
      <td>988 - NON-EXTENSIVE O.R. PROC UNRELATED TO PRI...</td>
      <td>520138</td>
      <td>AURORA ST LUKES MEDICAL CENTER</td>
      <td>2900 W OKLAHOMA AVE</td>
      <td>MILWAUKEE</td>
      <td>WI</td>
      <td>53215</td>
      <td>WI - Milwaukee</td>
      <td>25</td>
      <td>$63,977.60</td>
      <td>$12,867.24</td>
      <td>$10,142.96</td>
      <td>AURORA ST LUKES MEDICAL CENTER_2900 W OKLAHOMA...</td>
    </tr>
    <tr>
      <th>197278</th>
      <td>988 - NON-EXTENSIVE O.R. PROC UNRELATED TO PRI...</td>
      <td>520177</td>
      <td>FROEDTERT MEMORIAL LUTHERAN HOSPITAL</td>
      <td>9200 W WISCONSIN AVE</td>
      <td>MILWAUKEE</td>
      <td>WI</td>
      <td>53226</td>
      <td>WI - Milwaukee</td>
      <td>15</td>
      <td>$63,582.80</td>
      <td>$22,718.87</td>
      <td>$17,057.87</td>
      <td>FROEDTERT MEMORIAL LUTHERAN HOSPITAL_9200 W WI...</td>
    </tr>
    <tr>
      <th>197281</th>
      <td>989 - NON-EXTENSIVE O.R. PROC UNRELATED TO PRI...</td>
      <td>220071</td>
      <td>MASSACHUSETTS GENERAL HOSPITAL</td>
      <td>55 FRUIT STREET</td>
      <td>BOSTON</td>
      <td>MA</td>
      <td>2114</td>
      <td>MA - Boston</td>
      <td>11</td>
      <td>$54,199.64</td>
      <td>$10,610.18</td>
      <td>$9,079.09</td>
      <td>MASSACHUSETTS GENERAL HOSPITAL_55 FRUIT STREET...</td>
    </tr>
    <tr>
      <th>197282</th>
      <td>989 - NON-EXTENSIVE O.R. PROC UNRELATED TO PRI...</td>
      <td>360180</td>
      <td>CLEVELAND CLINIC</td>
      <td>9500 EUCLID AVENUE</td>
      <td>CLEVELAND</td>
      <td>OH</td>
      <td>44195</td>
      <td>OH - Cleveland</td>
      <td>11</td>
      <td>$42,411.45</td>
      <td>$8,367.18</td>
      <td>$6,772.55</td>
      <td>CLEVELAND CLINIC_9500 EUCLID AVENUE_CLEVELAND_OH</td>
    </tr>
  </tbody>
</table>
<p>151705 rows × 13 columns</p>
</div>




```python
#merge the source 'hospitals' DF with the newly formed 'new_hospital_charges' DF

merge = hospitals.merge(new_hospital_charges, left_on='Facility Name', right_on="Provider Name")
```


```python
#Take a look at the merged table

merge.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Facility ID</th>
      <th>Facility Name</th>
      <th>Address</th>
      <th>City</th>
      <th>State</th>
      <th>ZIP Code</th>
      <th>County Name</th>
      <th>Phone Number</th>
      <th>Hospital Type</th>
      <th>Hospital Ownership</th>
      <th>...</th>
      <th>Provider Street Address</th>
      <th>Provider City</th>
      <th>Provider State</th>
      <th>Provider Zip Code</th>
      <th>Hospital Referral Region (HRR) Description</th>
      <th>Total Discharges</th>
      <th>Average Covered Charges</th>
      <th>Average Total Payments</th>
      <th>Average Medicare Payments</th>
      <th>Charges_Name_Lookup</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>340073</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>WAKE</td>
      <td>(919) 954-3000</td>
      <td>Acute Care Hospitals</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>NC - Raleigh</td>
      <td>19</td>
      <td>$82,522.37</td>
      <td>$19,446.89</td>
      <td>$10,607.53</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>340073</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>WAKE</td>
      <td>(919) 954-3000</td>
      <td>Acute Care Hospitals</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>NC - Raleigh</td>
      <td>13</td>
      <td>$56,765.31</td>
      <td>$17,527.00</td>
      <td>$16,245.15</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>340073</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>WAKE</td>
      <td>(919) 954-3000</td>
      <td>Acute Care Hospitals</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>NC - Raleigh</td>
      <td>16</td>
      <td>$81,039.75</td>
      <td>$11,124.56</td>
      <td>$9,733.56</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>340073</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>WAKE</td>
      <td>(919) 954-3000</td>
      <td>Acute Care Hospitals</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>NC - Raleigh</td>
      <td>17</td>
      <td>$28,659.47</td>
      <td>$6,326.71</td>
      <td>$5,206.71</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>340073</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>WAKE</td>
      <td>(919) 954-3000</td>
      <td>Acute Care Hospitals</td>
      <td>Voluntary non-profit - Private</td>
      <td>...</td>
      <td>3400 WAKE FOREST RD</td>
      <td>RALEIGH</td>
      <td>NC</td>
      <td>27609</td>
      <td>NC - Raleigh</td>
      <td>18</td>
      <td>$41,752.89</td>
      <td>$10,309.61</td>
      <td>$9,224.28</td>
      <td>DUKE HEALTH RALEIGH HOSPITAL_3400 WAKE FOREST ...</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 44 columns</p>
</div>




```python
#Verify the number of cols and rows in the merged DF

merge.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 168039 entries, 0 to 168038
    Data columns (total 44 columns):
     #   Column                                                         Non-Null Count   Dtype  
    ---  ------                                                         --------------   -----  
     0   Facility ID                                                    168039 non-null  object 
     1   Facility Name                                                  168039 non-null  object 
     2   Address                                                        168039 non-null  object 
     3   City                                                           168039 non-null  object 
     4   State                                                          168039 non-null  object 
     5   ZIP Code                                                       168039 non-null  int64  
     6   County Name                                                    168039 non-null  object 
     7   Phone Number                                                   168039 non-null  object 
     8   Hospital Type                                                  168039 non-null  object 
     9   Hospital Ownership                                             168039 non-null  object 
     10  Emergency Services                                             168039 non-null  object 
     11  Meets criteria for promoting interoperability of EHRs          154811 non-null  object 
     12  Hospital overall rating                                        168039 non-null  object 
     13  Hospital overall rating footnote                               2353 non-null    float64
     14  Mortality national comparison                                  168039 non-null  object 
     15  Mortality national comparison footnote                         3432 non-null    float64
     16  Safety of care national comparison                             168039 non-null  object 
     17  Safety of care national comparison footnote                    9655 non-null    float64
     18  Readmission national comparison                                168039 non-null  object 
     19  Readmission national comparison footnote                       2178 non-null    float64
     20  Patient experience national comparison                         168039 non-null  object 
     21  Patient experience national comparison footnote                3131 non-null    float64
     22  Effectiveness of care national comparison                      168039 non-null  object 
     23  Effectiveness of care national comparison footnote             5175 non-null    float64
     24  Timeliness of care national comparison                         168039 non-null  object 
     25  Timeliness of care national comparison footnote                3169 non-null    float64
     26  Efficient use of medical imaging national comparison           168039 non-null  object 
     27  Efficient use of medical imaging national comparison footnote  10105 non-null   float64
     28  Location                                                       157997 non-null  object 
     29  Hospitals_Name_Lookup                                          168039 non-null  object 
     30  Hosp_Name_Lookup                                               168039 non-null  object 
     31  DRG Definition                                                 168039 non-null  object 
     32  Provider Id                                                    168039 non-null  int64  
     33  Provider Name                                                  168039 non-null  object 
     34  Provider Street Address                                        168039 non-null  object 
     35  Provider City                                                  168039 non-null  object 
     36  Provider State                                                 168039 non-null  object 
     37  Provider Zip Code                                              168039 non-null  int64  
     38  Hospital Referral Region (HRR) Description                     168039 non-null  object 
     39  Total Discharges                                               168039 non-null  object 
     40  Average Covered Charges                                        168039 non-null  object 
     41  Average Total Payments                                         168039 non-null  object 
     42  Average Medicare Payments                                      168039 non-null  object 
     43  Charges_Name_Lookup                                            168039 non-null  object 
    dtypes: float64(8), int64(3), object(33)
    memory usage: 57.7+ MB


# 7. Cleaning the merged DF for further analysis


```python
#drop cols 10-29

merge.drop(merge.iloc[:, 10:30], inplace = True, axis = 1)
```


```python
#check the result after dropped cols

merge.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 168039 entries, 0 to 168038
    Data columns (total 24 columns):
     #   Column                                      Non-Null Count   Dtype 
    ---  ------                                      --------------   ----- 
     0   Facility ID                                 168039 non-null  object
     1   Facility Name                               168039 non-null  object
     2   Address                                     168039 non-null  object
     3   City                                        168039 non-null  object
     4   State                                       168039 non-null  object
     5   ZIP Code                                    168039 non-null  int64 
     6   County Name                                 168039 non-null  object
     7   Phone Number                                168039 non-null  object
     8   Hospital Type                               168039 non-null  object
     9   Hospital Ownership                          168039 non-null  object
     10  Hosp_Name_Lookup                            168039 non-null  object
     11  DRG Definition                              168039 non-null  object
     12  Provider Id                                 168039 non-null  int64 
     13  Provider Name                               168039 non-null  object
     14  Provider Street Address                     168039 non-null  object
     15  Provider City                               168039 non-null  object
     16  Provider State                              168039 non-null  object
     17  Provider Zip Code                           168039 non-null  int64 
     18  Hospital Referral Region (HRR) Description  168039 non-null  object
     19  Total Discharges                            168039 non-null  object
     20  Average Covered Charges                     168039 non-null  object
     21  Average Total Payments                      168039 non-null  object
     22  Average Medicare Payments                   168039 non-null  object
     23  Charges_Name_Lookup                         168039 non-null  object
    dtypes: int64(3), object(21)
    memory usage: 32.1+ MB



```python
#drop more cols (in range: index 12-16)

merge.drop(merge.iloc[:,12:17], inplace=True, axis=1)
```


```python
merge.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 168039 entries, 0 to 168038
    Data columns (total 19 columns):
     #   Column                                      Non-Null Count   Dtype 
    ---  ------                                      --------------   ----- 
     0   Facility ID                                 168039 non-null  object
     1   Facility Name                               168039 non-null  object
     2   Address                                     168039 non-null  object
     3   City                                        168039 non-null  object
     4   State                                       168039 non-null  object
     5   ZIP Code                                    168039 non-null  int64 
     6   County Name                                 168039 non-null  object
     7   Phone Number                                168039 non-null  object
     8   Hospital Type                               168039 non-null  object
     9   Hospital Ownership                          168039 non-null  object
     10  Hosp_Name_Lookup                            168039 non-null  object
     11  DRG Definition                              168039 non-null  object
     12  Provider Zip Code                           168039 non-null  int64 
     13  Hospital Referral Region (HRR) Description  168039 non-null  object
     14  Total Discharges                            168039 non-null  object
     15  Average Covered Charges                     168039 non-null  object
     16  Average Total Payments                      168039 non-null  object
     17  Average Medicare Payments                   168039 non-null  object
     18  Charges_Name_Lookup                         168039 non-null  object
    dtypes: int64(2), object(17)
    memory usage: 25.6+ MB



```python
#drop two more cols based on their index

merge.drop(merge.columns[[10,18]], inplace=True, axis=1)
```


```python
#check the remaining columns and their types

merge.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 168039 entries, 0 to 168038
    Data columns (total 17 columns):
     #   Column                                      Non-Null Count   Dtype 
    ---  ------                                      --------------   ----- 
     0   Facility ID                                 168039 non-null  object
     1   Facility Name                               168039 non-null  object
     2   Address                                     168039 non-null  object
     3   City                                        168039 non-null  object
     4   State                                       168039 non-null  object
     5   ZIP Code                                    168039 non-null  int64 
     6   County Name                                 168039 non-null  object
     7   Phone Number                                168039 non-null  object
     8   Hospital Type                               168039 non-null  object
     9   Hospital Ownership                          168039 non-null  object
     10  DRG Definition                              168039 non-null  object
     11  Provider Zip Code                           168039 non-null  int64 
     12  Hospital Referral Region (HRR) Description  168039 non-null  object
     13  Total Discharges                            168039 non-null  object
     14  Average Covered Charges                     168039 non-null  object
     15  Average Total Payments                      168039 non-null  object
     16  Average Medicare Payments                   168039 non-null  object
    dtypes: int64(2), object(15)
    memory usage: 23.1+ MB



```python
#select certain columns (13-16) to check for cleaning tasks

merge.iloc[0:10, 13:]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Total Discharges</th>
      <th>Average Covered Charges</th>
      <th>Average Total Payments</th>
      <th>Average Medicare Payments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>19</td>
      <td>$82,522.37</td>
      <td>$19,446.89</td>
      <td>$10,607.53</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13</td>
      <td>$56,765.31</td>
      <td>$17,527.00</td>
      <td>$16,245.15</td>
    </tr>
    <tr>
      <th>2</th>
      <td>16</td>
      <td>$81,039.75</td>
      <td>$11,124.56</td>
      <td>$9,733.56</td>
    </tr>
    <tr>
      <th>3</th>
      <td>17</td>
      <td>$28,659.47</td>
      <td>$6,326.71</td>
      <td>$5,206.71</td>
    </tr>
    <tr>
      <th>4</th>
      <td>18</td>
      <td>$41,752.89</td>
      <td>$10,309.61</td>
      <td>$9,224.28</td>
    </tr>
    <tr>
      <th>5</th>
      <td>56</td>
      <td>$30,248.18</td>
      <td>$6,337.61</td>
      <td>$5,091.21</td>
    </tr>
    <tr>
      <th>6</th>
      <td>13</td>
      <td>$20,348.08</td>
      <td>$4,454.00</td>
      <td>$3,490.92</td>
    </tr>
    <tr>
      <th>7</th>
      <td>16</td>
      <td>$29,012.31</td>
      <td>$4,489.69</td>
      <td>$3,465.69</td>
    </tr>
    <tr>
      <th>8</th>
      <td>13</td>
      <td>$22,167.46</td>
      <td>$5,811.38</td>
      <td>$4,551.54</td>
    </tr>
    <tr>
      <th>9</th>
      <td>37</td>
      <td>$53,026.89</td>
      <td>$15,232.86</td>
      <td>$13,373.73</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Remove $ from columns 13-16

merge['Average Covered Charges']=merge['Average Covered Charges'].str.strip('$')
merge['Average Total Payments']=merge['Average Total Payments'].str.strip('$')
merge['Average Medicare Payments']=merge['Average Medicare Payments'].str.strip('$')

#verify that $ sign is stripped

merge['Average Covered Charges'].head()
merge['Average Total Payments'].head()
merge['Average Medicare Payments'].head()
```




    0    82,522.37
    1    56,765.31
    2    81,039.75
    3    28,659.47
    4    41,752.89
    Name: Average Covered Charges, dtype: object




```python
#remove commas

merge['Average Covered Charges'] = merge['Average Covered Charges'].str.replace(",", '')

#convert to float

merge['Average Covered Charges'] = merge['Average Covered Charges'].astype('float')

#verify the conversion to float was sucessful

assert merge['Average Covered Charges'].dtype == 'float'
```


```python
#Also verify by looking at all current data types for all cols

merge.dtypes
```




    Facility ID                                    object
    Facility Name                                  object
    Address                                        object
    City                                           object
    State                                          object
    ZIP Code                                        int64
    County Name                                    object
    Phone Number                                   object
    Hospital Type                                  object
    Hospital Ownership                             object
    DRG Definition                                 object
    Provider Zip Code                               int64
    Hospital Referral Region (HRR) Description     object
    Total Discharges                               object
    Average Covered Charges                       float64
    Average Total Payments                         object
    Average Medicare Payments                      object
    dtype: object




```python
#remove commas from two other columns

merge['Average Total Payments'] = merge['Average Total Payments'].str.replace(',', '')
merge['Average Medicare Payments'] = merge['Average Medicare Payments'].str.replace(',', '')
```


```python
#convert other cols to float in the same way

merge['Average Total Payments'] = merge['Average Total Payments'].astype('float')
merge['Average Medicare Payments'] = merge['Average Medicare Payments'].astype('float')
```


```python
#verify the conversion worked

assert merge['Average Total Payments'].dtype == 'float'
assert merge['Average Medicare Payments'].dtype == 'float'
```


```python
#validate data types for all cols again

merge.dtypes
```




    Facility ID                                    object
    Facility Name                                  object
    Address                                        object
    City                                           object
    State                                          object
    ZIP Code                                        int64
    County Name                                    object
    Phone Number                                   object
    Hospital Type                                  object
    Hospital Ownership                             object
    DRG Definition                                 object
    Provider Zip Code                               int64
    Hospital Referral Region (HRR) Description     object
    Total Discharges                               object
    Average Covered Charges                       float64
    Average Total Payments                        float64
    Average Medicare Payments                     float64
    dtype: object




```python
#check for duplicates

duplicates = merge.duplicated()

merge[duplicates]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Facility ID</th>
      <th>Facility Name</th>
      <th>Address</th>
      <th>City</th>
      <th>State</th>
      <th>ZIP Code</th>
      <th>County Name</th>
      <th>Phone Number</th>
      <th>Hospital Type</th>
      <th>Hospital Ownership</th>
      <th>DRG Definition</th>
      <th>Provider Zip Code</th>
      <th>Hospital Referral Region (HRR) Description</th>
      <th>Total Discharges</th>
      <th>Average Covered Charges</th>
      <th>Average Total Payments</th>
      <th>Average Medicare Payments</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



# 8. Analysing the data

Now that the merged table has been trimmed down and cleaned we can explore the data to extract insights. Below are a few examples of various calculations that can be run on the data to check average, total, min and max hospital charges across the entire data or per hospital. 


```python
#what's the total avg covered charges for all hospitals?

merge['Average Covered Charges'].sum()
```




    9829154595.540003




```python
#run summary stats on the average covered charges col

merge['Average Covered Charges'].describe()
```




    count    1.680390e+05
    mean     5.849329e+04
    std      7.056925e+04
    min      1.520800e+03
    25%      2.287776e+04
    50%      3.831556e+04
    75%      6.763867e+04
    max      3.104374e+06
    Name: Average Covered Charges, dtype: float64




```python
#calculate high level stats for each hospital

medicare_payments = merge.groupby(['Facility Name']).agg({'Average Medicare Payments':['sum', 'mean', 'max', 'min']})

medicare_payments = medicare_payments.reset_index()

medicare_payments
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th>Facility Name</th>
      <th colspan="4" halign="left">Average Medicare Payments</th>
    </tr>
    <tr>
      <th></th>
      <th></th>
      <th>sum</th>
      <th>mean</th>
      <th>max</th>
      <th>min</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ABBEVILLE GENERAL HOSPITAL</td>
      <td>93261.06</td>
      <td>7771.755000</td>
      <td>13680.22</td>
      <td>3957.74</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ABBOTT NORTHWESTERN HOSPITAL</td>
      <td>3622915.28</td>
      <td>14491.661120</td>
      <td>190761.96</td>
      <td>2943.35</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ABILENE REGIONAL MEDICAL CENTER</td>
      <td>318766.17</td>
      <td>8854.615833</td>
      <td>24886.93</td>
      <td>2416.85</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ABINGTON MEMORIAL HOSPITAL</td>
      <td>2278969.35</td>
      <td>11568.372335</td>
      <td>126298.26</td>
      <td>2788.19</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ABRAZO CENTRAL CAMPUS</td>
      <td>206858.14</td>
      <td>10342.907000</td>
      <td>37351.33</td>
      <td>4561.81</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2293</th>
      <td>YALOBUSHA GENERAL HOSPITAL</td>
      <td>39907.18</td>
      <td>4434.131111</td>
      <td>6600.83</td>
      <td>3631.79</td>
    </tr>
    <tr>
      <th>2294</th>
      <td>YAVAPAI REGIONAL MEDICAL CENTER</td>
      <td>726185.89</td>
      <td>8965.257901</td>
      <td>31013.22</td>
      <td>2401.85</td>
    </tr>
    <tr>
      <th>2295</th>
      <td>YORK HOSPITAL</td>
      <td>5437014.82</td>
      <td>11519.099195</td>
      <td>100556.24</td>
      <td>2240.36</td>
    </tr>
    <tr>
      <th>2296</th>
      <td>YUKON KUSKOKWIM DELTA REG HOSPITAL</td>
      <td>78751.06</td>
      <td>13125.176667</td>
      <td>14938.06</td>
      <td>11509.09</td>
    </tr>
    <tr>
      <th>2297</th>
      <td>YUMA REGIONAL MEDICAL CENTER</td>
      <td>1830666.32</td>
      <td>14414.695433</td>
      <td>78799.25</td>
      <td>3038.75</td>
    </tr>
  </tbody>
</table>
<p>2298 rows × 5 columns</p>
</div>




```python
#sort table values by sum of edicare payments (highest to lowest)

medicare_payments.sort_values(by=('Average Medicare Payments', 'sum'), ascending=False)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th>Facility Name</th>
      <th colspan="4" halign="left">Average Medicare Payments</th>
    </tr>
    <tr>
      <th></th>
      <th></th>
      <th>sum</th>
      <th>mean</th>
      <th>max</th>
      <th>min</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>663</th>
      <td>GOOD SAMARITAN HOSPITAL</td>
      <td>31991925.96</td>
      <td>12545.853318</td>
      <td>97936.62</td>
      <td>2185.72</td>
    </tr>
    <tr>
      <th>1886</th>
      <td>ST JOSEPH HOSPITAL</td>
      <td>18661488.84</td>
      <td>11187.942950</td>
      <td>80598.62</td>
      <td>2385.97</td>
    </tr>
    <tr>
      <th>1887</th>
      <td>ST JOSEPH MEDICAL CENTER</td>
      <td>18456169.70</td>
      <td>11051.598623</td>
      <td>55926.82</td>
      <td>2036.30</td>
    </tr>
    <tr>
      <th>785</th>
      <td>HOLY CROSS HOSPITAL</td>
      <td>15733761.60</td>
      <td>11501.287719</td>
      <td>139360.25</td>
      <td>2297.64</td>
    </tr>
    <tr>
      <th>1142</th>
      <td>MEMORIAL HOSPITAL</td>
      <td>15551205.24</td>
      <td>7902.035183</td>
      <td>35626.45</td>
      <td>2327.44</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1743</th>
      <td>SEYMOUR HOSPITAL</td>
      <td>5309.36</td>
      <td>5309.360000</td>
      <td>5309.36</td>
      <td>5309.36</td>
    </tr>
    <tr>
      <th>1746</th>
      <td>SHARE MEDICAL CENTER</td>
      <td>5104.27</td>
      <td>5104.270000</td>
      <td>5104.27</td>
      <td>5104.27</td>
    </tr>
    <tr>
      <th>490</th>
      <td>DOUGLAS GARDENS HOSPITAL</td>
      <td>3028.11</td>
      <td>3028.110000</td>
      <td>3028.11</td>
      <td>3028.11</td>
    </tr>
    <tr>
      <th>2270</th>
      <td>WILMINGTON TREATMENT CENTER</td>
      <td>2885.45</td>
      <td>2885.450000</td>
      <td>2885.45</td>
      <td>2885.45</td>
    </tr>
    <tr>
      <th>2079</th>
      <td>TRUSTPOINT HOSPITAL</td>
      <td>2715.06</td>
      <td>2715.060000</td>
      <td>2715.06</td>
      <td>2715.06</td>
    </tr>
  </tbody>
</table>
<p>2298 rows × 5 columns</p>
</div>




```python

```
