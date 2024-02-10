#################################
## Wrappers for functions that ##
## build the csvs              ##
#################################
import os
import pandas as pd
import pathlib
import nfelodcm as dcm
from dotenv import load_dotenv
from supabase import create_client

from .Constructors import (
    update_lines
)

## try to load the dot env and init supabase client for local runs ##
try:
    env_path = '{0}/.env'.format(
        pathlib.Path(__file__).parent.parent.resolve()
    )
    load_dotenv(env_path)
except Exception as e:
    print('Env variables could not be loaded. This only matters if running locally: /n    {0}'.format(e))

## load games ##
db = dcm.load(['games'])

## wrappers ##
def run_line_update(client=None):
    '''
    runs the update lines function
    '''
    if client is None:
        ## if client was not passed, init ##
        client = create_client(
            os.environ.get("SUPABASE_URL"),
            os.environ.get("SUPABASE_KEY")
        )
    ## run update ##
    update_lines(
        games=db['games'],
        supabase=client
    )
