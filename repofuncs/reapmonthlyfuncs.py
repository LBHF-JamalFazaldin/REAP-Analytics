import pandas as pd

def calculate_pct(df, categories, value):
    df['rolling_12m_sum'] = (
        df
        .groupby(categories)[value]
        .rolling(window=12, min_periods=12)
        .sum()
        .reset_index(level=list(range(len(categories ))), drop=True)
    )

    df[r'% change 12 month average'] = df.groupby(categories)['rolling_12m_sum'].pct_change(periods=12)

    df[r'% change same month last year'] = df.groupby(categories + ['month'])[value].pct_change(periods=1)

    return df