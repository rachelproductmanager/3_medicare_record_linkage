# 3. Medicare Record Linkage Project

Python's record linkage toolkit is a powerful technique used to merge multiple large datasets together.

It is really useful when there is no common ID/ unique key or identifier to join the tables on, or when we're dealing with messy data (e.g. many values have typos or different spellings).

In this project, I've set out how to link tables by using the Python recordlinkage package.

The recordlinkage package essentially helps us:

1. identify matching records in another table by calculating the similarity between multiple pairs of string records; and
2. join two datasets into one clean "master" dataset that we can perform calculations on in order to draw business insights.

To demonstrate the record linkage technique I've used U.S. hospital data from two separate datasets, which I've matched and linked based on hospital name and address (assuming there's no common key to join the tables on).

US hospital data is a good candidate for this exercise because many hospitals have similar names across different cities (e.g. "Saint Lukes", "Saint Mary", "Community Hospital"). Also, addresses can be ambiguous because, in urban areas, hospitals can occupy several city blocks. Because there are tens of thousands of medical facilities in the US, the data's scale is quite large, which makes the problem even more challenging.

We'll be looking at two data sets:

1. "Hospital general information" - a data set that contains basic details such as hospital number, name, address, State, hospital type and ownership information. This table can be found at:https://data.medicare.gov/Hospital-Compare/Hospital-General-Information/xubh-q36u and has 5,314 rows and 29 columns.

2. "Inpatient Medicare Charge Data" - a dataset that contains hospital basic information (name, address), together with the total number of discharges and various Medicare payments received by that hospital (in relation to patients treated for a specific heart medical condition). The data can be found at:https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Inpatient2016 and has 197,283 rows and 12 columns.

This project demonstrates the record linkage workflow:

1. Cleaning /pre-processing the data - done with the generic fucntion recordlinkage.preprocessing.clean() and used to standardize and clean text data such as transforming all records to lower case or removing brackets, spaces and symbols from names, phone numbers etc.

2. Indexing - Generating pairs of potentially matching records from both tables.

3. Comparing the pairs by calculating their similarity.

4. Classifying - Scoring the pairs based on similarity levels.

5. Evaluating - Filtering out pairs which don't match or which have low similarity scores and extracting matching records.

6. Linking the tables based on matching records.

The record linkage toolkit documentation with helpful datasets and code examples, can be found here: https://recordlinkage.readthedocs.io/en/latest/about.html
