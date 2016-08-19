# wikicount
Wikipedia pageview

This tool computes the top 25 wikipedia articles for the given day and hour by total pageviews for each unique domain.
To launch the script provide at least one date & hour in the command line using the format:
"%d/%m/%y %H"
You can also spicify a timerange by passing two arguments.

Example:
python compute.py "1/1/16 0"
python compute.py "1/1/15 0" "1/1/16 0"
