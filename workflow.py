import sys
import nfelomarket_data

if sys.argv[1] == 'all':
    nfelomarket_data.run_line_update()

if sys.argv[1] == 'lines':
    nfelomarket_data.run_line_update()