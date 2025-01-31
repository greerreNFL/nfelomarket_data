import pandas as pd
import numpy
import pathlib

from ..Utilities import df_fill, tz_convert

def load_existing():
    '''
    loads the existing lines file
    '''
    lines_file_path = '{0}/Data/lines.csv'.format(
        pathlib.Path(__file__).parent.parent.parent.resolve()
    )
    ## try to load and return ##
    try:
        return pd.read_csv(lines_file_path, index_col=0)
    except:
        return None
    
def save_line_file(line_df):
    '''
    saves the lines file to the data folder
    '''
    lines_file_path = '{0}/Data/lines.csv'.format(
        pathlib.Path(__file__).parent.parent.parent.resolve()
    )
    ## try to save ##
    try:
        line_df.to_csv(
            lines_file_path
        )
    except Exception as e:
        raise Exception('IO ERROR: Could not save lines file. \n     {0}'.format(e))

def fetch_supabase(supabase, start, end):
    '''
    Fetches linestream data from supabase with
    a predefined start and end
    '''
    return supabase.table(
        'line-stream'
    ).select(
        'game_id', 'created_at', 'bookmaker', 'priority',
        'home_spread', 'home_spread_price', 'away_spread_price',
        'home_ml', 'away_ml',
        'total_line', 'over_price', 'under_price',
        'home_spread_tickets_pct', 'home_spread_money_pct',
        count="exact"  # Request the exact count of rows
    ).order(
        'created_at', desc=True
    ).range(start, end).execute()

def get_line_stream(supabase, games, limit=2000):
    '''
    gets the line stream data from supabase using pagination
    to handle larger data pulls
    '''
    ## state vars ##
    range_start = 0
    range_end = 1000 if limit > 1000 else limit
    total_records = None
    stop_index = limit ## where to stop pagination
    data = []
    ## pagination loop ##
    while True:
        ## fetch ##
        resp = fetch_supabase(supabase, range_start, range_end)
        ## if no data is returned, end ##
        if len(resp.data) == 0:
            break
        ## if initial request, update the total records
        if total_records is None:
            total_records = resp.count
            ## update stop record if necessary ##
            if total_records < limit:
                stop_index = total_records
        ## add the data ##
        data.extend(resp.data)
        ## update states ##
        range_start = range_end
        range_end = stop_index if range_end + 1000 > stop_index else range_end + 1000
        ## if range start is greater than range end, break ##
        if range_start >= stop_index:
            break
    ## get data ##
    df = pd.DataFrame(data)
    ## add kickoff times to create a check for lines that are post-kickoff
    kicks = games[['game_id', 'gameday', 'gametime']].copy()
    ## game day is YYYY-MM-DD, gametime is a string (HH:MM) that references an EST 24-hour clock ##
    ## create a datetime column with the kickoff time in UTC
    kicks['kickoff_time'] = (
        pd.to_datetime(kicks['gameday'] + ' ' + kicks['gametime'])    # parse date & time
        .dt.tz_localize('America/New_York')                           # localize to Eastern Time
        .dt.tz_convert('UTC')                                         # convert to UTC
    )
    ## ensure no duplicates ##
    kicks = kicks.groupby(['game_id']).head(1).copy()
    ## add to df ##
    df = pd.merge(
        df,
        kicks,
        on=['game_id'],
        how='left'
    )
    ## filter the linestream to only include lines before kickoff ##
    df = df[
        (df['created_at'] < df['kickoff_time']) |
        (pd.isnull(df['kickoff_time']))
    ].copy()
    ## return ##
    return df

def define_open_set(df):
    '''
    For a linestream, finds lines within 15 minutes of Tuesday midnight
    '''
    df_open = df.copy()
    ## get the last observation and assume it is close to kickoff ##
    df_open['created_at'] = pd.to_datetime(df_open['created_at'])
    df_open['created_at'] = df_open['created_at'].dt.tz_convert('US/Pacific')
    df_open['last_obs_ts'] = df_open.groupby(['game_id'])['created_at'].transform(lambda x: x.max())
    ## get all lines the tuesday morning before the game ##
    ## two pulls happen this day, one at 12:00am and one at 4:00am ##
    df_open = df_open[
        (df_open['created_at'].dt.dayofweek == 1) & 
        (df_open['created_at'].dt.time < pd.to_datetime('05:00:00').time()) &
        (df_open['created_at'] >= df_open['last_obs_ts'] - pd.Timedelta(days=7))
    ].copy()
    ## The preferences is first by time window, then by source priority ##
    ## To create a priority that also factors in time, add hours past midnight to
    ## priotity
    df_open['priority'] = (
        df_open['priority'] +
        (
            df_open['created_at'] -
            df_open['created_at'].dt.normalize()
        ).dt.total_seconds() / 3600.0
    )
    ## return ##
    return df_open

def define_last_set(df):
    '''
    For a linstream, finds lines within 15 minutes of the most recent line
    '''
    df_last = df.copy()
    ## get most recent ts ##
    df_last['created_at'] = pd.to_datetime(df_last['created_at'])
    df_last['created_at'] = df_last['created_at'].dt.tz_convert('US/Pacific')
    df_last['last_obs_ts'] = df_last.groupby(['game_id'])['created_at'].transform(lambda x: x.max())
    df_last = df_last[
        df_last['created_at'] >= 
        df_last['last_obs_ts'] - pd.Timedelta(minutes=15)
    ].copy()
    ## return ##
    return df_last

def return_sourced_set(game_id, df, line_set):
    '''
    Within a set of like line types, return the highest priority, non-null
    set with specified col names
    '''
    ## extract cols and condense, keeping only highest priority non-nan ##
    cols = list(line_set.keys())
    df_ = df[
        df['game_id'] == game_id
    ][cols + ['priority']].copy().dropna().sort_values(
        by=['priority'],
        ascending=[True]
    ).reset_index(drop=True)
    ## create output dict ##
    output = {}
    ## for each item in line_set, add the renamed value to the output ##
    ## if no records were nan-free, then return nan
    for k, v in line_set.items():
        output[v] = df_.iloc[0][k] if len(df_) > 0 else numpy.nan
    ## return ##
    return output

def get_game_ids(games, rebuild):
    '''
    returns the game id's for the current week, and the previous week
    '''
    ## define current week and season ##
    next_game = games[pd.isnull(games['result'])].copy().sort_values(
        by=['gameday'],
        ascending=[True]
    ).reset_index(drop=True).head(1)
    ## if the season is over, then no current week ##
    ## if there is a next game, update current first gameday ##
    current_week_first_gameday = '3000-01-01'
    if len(next_game) > 0:
        current_week = next_game.iloc[0]['week']
        current_season = next_game.iloc[0]['season']
        ## get the first gameday of the current week ##
        current_week_first_gameday = games[
            (games['week'] == current_week) &
            (games['season'] == current_season)
        ]['gameday'].min()
    ## get previous week ##
    prev_game = games[games['gameday'] < current_week_first_gameday].copy().sort_values(
        by=['gameday'],
        ascending=[False]
    ).reset_index(drop=True).head(1)
    prev_week = prev_game.iloc[0]['week']
    prev_season = prev_game.iloc[0]['season']
    ## get ids ##
    ## if no current games, return empty ##
    current_ids = []
    if len(next_game) > 0:
        current_ids = games[
            (games['week'] == current_week) &
            (games['season'] == current_season)
        ]['game_id'].unique().tolist()
    ## if rebuild, then get all ids from previous season, not just ##
    ## previous week ##
    if rebuild:
        prev_ids = games[   
            (games['season'] == prev_season)
        ]['game_id'].unique().tolist()
    else:
        prev_ids = games[
            (games['week'] == prev_week) &
            (games['season'] == prev_season)
        ]['game_id'].unique().tolist()
    ## return ids and ensure no duplicates ##
    return list(set(current_ids + prev_ids))

def get_structured_lines(games, supabase, rebuild):
    '''
    returns structured line records for the current week, and the last completed week (if in the same season)
    '''
    ## get game ids to structure data for ##
    ids = get_game_ids(games, rebuild)
    ## get lines ##
    line_stream = get_line_stream(supabase, games, 35000 if rebuild else 2000)
    ## get open and close ##
    open_stream = define_open_set(line_stream)
    last_stream = define_last_set(line_stream)
    ## init output struct ##
    output = []
    ## for each id, add the structured line-stream data ##
    for game_id in ids:
        ## init rec ##
        rec = {'game_id' : game_id}
        ## add meta from games ##
        meta = games[games['game_id']==game_id][[
            'season', 'week', 'home_team', 'away_team'
        ]].to_dict(orient='records')
        rec = rec | meta[0]
        ## add open spreads ##
        open_spreads = return_sourced_set(
            game_id,
            open_stream,
            {
                'home_spread' : 'home_spread_open',
                'home_spread_price' : 'home_spread_open_price',
                'away_spread_price' : 'away_spread_open_price',
                'bookmaker' : 'home_spread_open_source',
                'created_at' : 'home_spread_open_timestamp'
            }
        )
        rec = rec | open_spreads
        ## add closing spreads ##
        last_spreads = return_sourced_set(
            game_id,
            last_stream,
            {
                'home_spread' : 'home_spread_last',
                'home_spread_price' : 'home_spread_last_price',
                'away_spread_price' : 'away_spread_last_price',
                'bookmaker' : 'home_spread_last_source',
                'created_at' : 'home_spread_last_timestamp'
            }
        )
        rec = rec | last_spreads
        ## add pct ##
        pcts = return_sourced_set(
            game_id,
            last_stream,
            {
                'home_spread_tickets_pct' : 'home_spread_tickets_pct',
                'home_spread_money_pct' : 'home_spread_money_pct',
                'bookmaker' : 'home_spread_pcts_source',
                'created_at' : 'home_spread_pct_timestamp' 
            }
        )
        rec = rec | pcts
        ## add ml open ##
        ml_open = return_sourced_set(
            game_id,
            open_stream,
            {
                'home_ml' : 'home_ml_open',
                'away_ml' : 'away_ml_open',
                'bookmaker' : 'ml_open_source',
                'created_at' : 'ml_open_timestamp'
            }
        )
        rec = rec | ml_open
        ## add ml close
        ml_last = return_sourced_set(
            game_id,
            last_stream,
            {
                'home_ml' : 'home_ml_last',
                'away_ml' : 'away_ml_last',
                'bookmaker' : 'ml_last_source',
                'created_at' : 'ml_last_timestamp'
            }
        )
        rec = rec | ml_last
        ## total open ##
        total_open = return_sourced_set(
            game_id,
            open_stream,
            {
                'total_line' : 'total_line_open',
                'under_price' : 'under_price_open',
                'over_price' : 'over_price_open',
                'bookmaker' : 'total_line_open_source',
                'created_at' : 'total_line_open_timestamp'
            }
        )
        rec = rec | total_open
        ## total last ##
        total_last = return_sourced_set(
            game_id,
            last_stream,
            {
                'total_line' : 'total_line_last',
                'under_price' : 'under_price_last',
                'over_price' : 'over_price_last',
                'bookmaker' : 'total_line_last_source',
                'created_at' : 'total_line_last_timestamp'
            }
        )
        rec = rec | total_last
        ## append rec ##
        output.append(rec)
    ## return ##
    return pd.DataFrame(output)

def update_lines(games, supabase, rebuild=False):
    '''
    updates the lines file
    '''
    ## get the existing ##
    existing = load_existing()
    ## get the new structured data
    updates = get_structured_lines(games, supabase, rebuild)
    ## merge ##
    if existing is None:
        ## sort ##
        updates = updates.sort_values(
            by=['season', 'week'],
            ascending=[True, True]
        ).reset_index(drop=True)
        save_line_file(updates)
    else:
        ## ensure timezones are consistent
        updates = tz_convert(updates)
        existing = tz_convert(existing)
        ## fill the existing with new values
        updated = df_fill(
            new=updates,
            target=existing,
            join_cols=['game_id', 'season', 'week', 'home_team', 'away_team']
        )
        updated = updated.sort_values(
            by=['season', 'week'],
            ascending=[True, True]
        ).reset_index(drop=True)
        save_line_file(updated)