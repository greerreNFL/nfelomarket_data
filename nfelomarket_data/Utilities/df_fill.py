import pandas as pd

def df_fill(
        new:pd.DataFrame,
        target:pd.DataFrame,
        join_cols:list
    ):
    '''
    Fills a target df with new data, using the target
    table as a backup for missing data

    Params:
    - new: a dataframe with new data
    - target: a dataframe to be filled and extended
    - join_cols: the columns used to join the tables
    '''
    ## structure and states ##
    target_cols = target.columns.to_list()
    new_cols = new.columns.to_list()
    overlap_cols = []
    ## populate columns that overlap in the two dfs ##
    for col in target_cols:
        if col in new_cols and col not in join_cols:
            overlap_cols.append(col)
    ## rename ##
    new = new.rename(columns={x : '{0}_x'.format(x) for x in overlap_cols})
    target = target.rename(columns={x : '{0}_y'.format(x) for x in overlap_cols})
    ## join ##
    filled = pd.merge(
        target,
        new,
        on=join_cols,
        how='outer'
    )
    ## fill ##
    for col in overlap_cols:
        ## create a col that uses the new data unless its null, then use the old ##
        filled[col] = filled['{0}_x'.format(col)].combine_first(filled['{0}_y'.format(col)])
    ## drop excess cols ##
    filled = filled.drop(columns=['{0}_x'.format(x) for x in overlap_cols])
    filled = filled.drop(columns=['{0}_y'.format(x) for x in overlap_cols])
    ## return the filled df with the original columns ##
    return filled[target_cols].copy()
