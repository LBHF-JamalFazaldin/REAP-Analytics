import pandas as pd
from pandas.tseries.offsets import MonthBegin, DateOffset
import calendar
import numpy as np

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

def compute_dynamic_change(df):
    df = df.copy()
    # Detect year columns dynamically (those with "(Apr-...)" in them)
    year_cols = [c for c in df.columns if "(" in c]
    if len(year_cols) < 2:
        raise ValueError("Not enough year columns to compute change")
 
    # Ensure chronological order (previous, current)
    year_cols = sorted(year_cols)  
    prev_col, curr_col = year_cols[-2], year_cols[-1]
 
    changes = []
    changes_str = []
    for idx, row in df.iterrows():
        v_prev, v_curr = row[prev_col], row[curr_col]
        change_str = None

        try:
            # Case 1: percentage metrics (values between 0 and 1)
            if isinstance(v_prev, (int, float)) and isinstance(v_curr, (int, float)) and 0 <= v_prev <= 1 and 0 <= v_curr <= 1:
                change = v_curr - v_prev   # percentage point change
                change_str = change * 100
                change_str = f'{change_str:+.2f}% pp'
            # Case 2: normal relative % change
            elif isinstance(v_prev, (int, float)) and v_prev != 0:
                change = (v_curr - v_prev) / v_prev
                change_str = change * 100
                change_str = f'{change_str:+.2f}%'
            else:
                change = np.nan
        except Exception:
            change = np.nan

        if change_str is None:
            change_str = change
        
        changes.append(change)
        changes_str.append(change_str)
 
    df["% Change"] = changes_str
    df["RAG"] = changes

    df.drop(columns=year_cols, inplace=True)

    return df

def obtain_relevant_dates(df, date):
    latest_date = pd.to_datetime(df[date]).max()
    first_of_current_month = latest_date.replace(day=1)
    prev_full_month = first_of_current_month - MonthBegin(1)
    same_month_last_year = prev_full_month - DateOffset(years=1)
    prev_full_month = prev_full_month.strftime('%Y-%m')
    same_month_last_year = same_month_last_year.strftime('%Y-%m')
    prev_full_month_name = calendar.month_abbr[latest_date.month-1]
    month_year = f'{prev_full_month_name}-{(first_of_current_month - MonthBegin(1)).year}'

    date_dic = {
        "latest_date" : latest_date,
        "first_of_current_month" : first_of_current_month,
        "same_month_last_year" : same_month_last_year,
        "prev_full_month" : prev_full_month,
        "prev_full_month_name" : prev_full_month_name,
        "month_year" : month_year
    }

    return date_dic