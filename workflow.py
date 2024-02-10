import sys
import nfelomarket_data
from supabase import create_client

if sys.argv[1] == 'all':
    ## init client from secrets ##
    client = create_client(
        sys.argv[2],
        sys.argv[3]
    )
    nfelomarket_data.run_line_update(client)

if sys.argv[1] == 'lines':
    ## init client from secrets ##
    client = create_client(
        sys.argv[2],
        sys.argv[3]
    )
    nfelomarket_data.run_line_update(client)