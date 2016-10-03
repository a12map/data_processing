# A12 data processing

These scripts process GTFS data provided by Prague Public Transport Company. They compute a rough distance matrix from each station to each station. The matrix is to be used in the WEB application that visualises the arrival times on a map.

## To use:
1. Run `pip install -r requirements.txt` (ideally in a virtualenv)
2. Run `./process_data.py <GTFS_data_folder> <output_folder>`
3. Run `./process_data.py <times_data> <locations_data> <output_folder>` for both day and night data

