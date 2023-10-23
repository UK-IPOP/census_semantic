# Semantic Search-Powered US Census Data Extraction Tool
A tool that selects relevant US Census variables based on user-supplied free-text search terms. Multiple terms may be provided. Better documentation to come.

## Requirements
- US Census API key - freely available by request. See [The census API documentation.](https://www.census.gov/data/developers/data-sets.html)
- Python 3 recent enough to work with the following required packages:
    - duckdb
    - censusdata
    - us
    - dotenv
- County and state FIPS codes you're interested in

## Basic usage
1. Request a Census API key if you don't already have one
2. Download this repo and extract locally
3. Create a file in the repo directory/same directory as the Python scripts called ".env" with a valid census API key and some query parameters. The included example_env.txt could be used as a template. In that case, fill in your values and rename the file to ".env"
4. Select and download census data using "select_vars.py":
    ```
    python select_vars.py pattern [,pattern]*
    ```
Currently the script writes any hits to STDOUT.
 
## Citation
If you make use of this tool in your work, please use the following citation:
D. R. Harris and N. Seyedtalebi, "Extracting Semantics from Census-based Reference Data," 2021 IEEE 15th International Conference on Semantic Computing (ICSC), Laguna Hills, CA, USA, 2021, pp. 88-89, doi: 10.1109/ICSC50631.2021.00022.

Abstract: We present preliminary findings in extracting semantics from reference data generated by the United States Census Bureau. US Census reference data is based upon surveys designed to collect demographics and other socioeconomic factors by geographical regions. These data sets contain thousands of variables; this complexity makes the reference data difficult to learn, query, and integrate into analyses. Researchers often avoid working directly with US Census reference data and instead work with census-derived extracts capturing a much smaller subset of records. We propose to use natural language processing to extract the semantics of census-based reference data and to map census variables to known ontologies. This semantic processing reduces the large volume of variables into more manageable sets of conceptual variables that can be organized by meaning and semantic type.