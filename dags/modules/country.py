import pandas as pd

def view_percent():
    # import different country codes
    codes = pd.read_csv("/opt/airflow/dags/modules/country_codes.csv")

    # import youtube views for my channel and calculate percentage viewed
    views = pd.read_csv("/opt/airflow/dags/modules/youtube_views.csv")
    views = views.iloc[1: , :]
    views = views[views.Views != 0] # removing countries with no views
    views = views[views.Geography != 'US'] # pulling US already
    views["percent"] = views['Watch time (hours)'] / views['Watch time (hours)'].sum()

    # merge dataframes for final dataframe
    percent = views.merge(codes, how='left', left_on='Geography', right_on='code')
    percent = percent[['country','percent']]
    
    # return the dataframe
    return percent