import pandas as pd

def equal(s1: pd.Series, s2: pd.Series, sort=False) -> bool:
    if sort:
        s1_copy, s2_copy = s1.sort_values().reset_index(drop=True), s2.sort_values().reset_index(drop=True)
        return s1_copy.equals(s2_copy)
    return s1.reset_index(drop=True).equals(s2.reset_index(drop=True))

def make_columns_unique(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def regularize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = make_columns_unique(df)
    numeric_cols = df.select_dtypes(include='number').columns
    for col in numeric_cols:
        df[col] = df[col].astype('float')
    return df.astype(str).apply(lambda r: r.str.lower(), axis=1)


def compare_results(df_true: pd.DataFrame, df_pred: pd.DataFrame) -> bool:
    '''
    This function does not require the same names of columns, so 
    It also might get a false positive when there are multiple columns with the same values, possibly in different order
    '''

    # fail early: the amount of rows should be the same. predictions are allowed to have more columns than true df
    if df_true.shape[0] != df_pred.shape[0]:
        return False
    if df_true.shape[1] > df_pred.shape[1]:
        return False
    
    df_true = regularize_df(df_true)
    df_pred = regularize_df(df_pred)

    # trying to find a column by which to sort rows
    # try to find a column that would be equal (regardless of order) the first column from the df_true
    # if this fails, return false

    col_map = None # this should be list of 2 elems: [df_true.col1, df_pred.col2] - to map the columns that has matching values

    # find a columns from df_pred that has no duplicates:

    non_duplicate_col = None
    for column in df_true.columns:
        # Check if the column has no duplicates
        if not df_true[column].duplicated().any():
            non_duplicate_col = column
            break
    if non_duplicate_col == None:
        raise ValueError("Couldn't find a column without duplicates in df_true. Please fix teh query or the dataset.")

    for col_pred in df_pred.columns:
        if equal(df_true[non_duplicate_col], df_pred[col_pred], sort=True):
            col_map = [non_duplicate_col, col_pred]
            break
    #print('equal_to_first', equal_to_first)
    if col_map is None:
        return False

    # sorting using the found map
    df_true = df_true.sort_values(col_map[0])
    df_pred = df_pred.sort_values(col_map[1])
    
    # now compare all the columns. every column from df_true should be present in df_pred
    for col_true in df_true.columns:
        has_match = False
        for col_pred in df_pred.columns:
            if equal(df_true[col_true], df_pred[col_pred]):
                has_match = True
                break
        if not has_match:
            return False

    return True