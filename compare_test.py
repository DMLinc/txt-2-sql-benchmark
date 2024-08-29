import pandas as pd
from compare import compare_results

def print_case(true_df, pred_df, res_exp):
    res = compare_results(true_df, pred_df)
    if res_exp != res:
        print('df_true')
        print(true_df)
        print('df_pred')
        print(pred_df)
        print('expected result:', res_exp)
        print('got:', compare_results(true_df, pred_df))
        print('='*20)
    # if exp_res != res:
    #     print('INCORRECT!!!!')
    # print('='*20)


if __name__ == '__main__':
    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8},
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':None, 'b':None, 'c': 7}
    ])
    df_pred = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':None, 'b':None, 'c': 7},
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'a':2, 'b':4, 'c': 8},
        {'a':5, 'b':6, 'c': 7}
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)


    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'a':2, 'c':4, 'b': 8},
        {'a':5, 'c':6, 'b': 7}
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'bb':2, 'cc':4, 'aa': 8},
        {'bb':5, 'cc':6, 'aa': 7}
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)


    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'bb':2, 'cc':4, 'aa': 8, 'dd': 9},
        {'bb':5, 'cc':6, 'aa': 7, 'dd': 10}
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'bb':2, 'cc':4, 'aa': 8},
    ])
    exp_res = True
    print_case(df_true, df_pred, exp_res)


    df_true = pd.DataFrame([
        {'a':5, 'b':6},
        {'a':2, 'b':4}
    ])
    df_pred = pd.DataFrame([
        {'aa':4, 'bb':6},
        {'aa':2, 'bb':5}
    ])
    exp_res = False
    print_case(df_true, df_pred, exp_res)

    ##########################

    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':5, 'c': 8},
    ])
    exp_res = False
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'a':2, 'b':5, 'c': 8},
        {'a':5, 'b':6, 'c': 7}
    ])
    exp_res = False
    print_case(df_true, df_pred, exp_res)


    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'a':2, 'c':5, 'b': 8},
        {'a':5, 'c':6, 'b': 7}
    ])
    exp_res = False
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':5, 'b':6, 'c': 7},
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'bb':2, 'cc':5, 'aa': 8},
        {'bb':5, 'cc':6, 'aa': 7}
    ])
    exp_res = False
    print_case(df_true, df_pred, exp_res)

    df_true = pd.DataFrame([
        {'a':2, 'b':4, 'c': 8}
    ])
    df_pred = pd.DataFrame([
        {'bb':2, 'cc':5, 'aa': 8},
    ])
    exp_res = False
    print_case(df_true, df_pred, exp_res)