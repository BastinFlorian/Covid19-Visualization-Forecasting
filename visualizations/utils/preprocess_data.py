import pandas as pd 
import os  

"""
This python files preprocess the John Hopkins Covid data 
"""

DIR_NAME = "..\..\csse_covid_19_data\csse_covid_19_daily_reports"
DIR_Name= "https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports"

def import_data(dir_name):
    col_to_rename = {"Country/Region":"Country_Region",
                     "Province/State":"Province_State",
                     "Last Update":"Last_Update",
                     "Long_":"Longitude",
                     "Lat":"Latitude"
                    }
    df_cols = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed','Deaths', 'Recovered','Active']
    df = pd.DataFrame(columns = df_cols)
    for file_name in os.listdir(dir_name):
        if file_name.endswith(".csv"):
            tmp = pd.read_csv(os.path.join(dir_name, file_name)).rename(columns=col_to_rename)
            tmp["Date"] = file_name[-14:-4]
            df = pd.concat([df, tmp], 
                               axis=0, 
                               join='outer')
            
    return df


def preprocess_data(df):  
    df.Deaths = df.Deaths.fillna(0)
    df.Confirmed = df.Confirmed.fillna(0)
    df.Recovered = df.Recovered.fillna(0)
    df.Active = df.Active.fillna(0)
    df.Province_State = df.Province_State.fillna("")  

    df.Last_Update = pd.to_datetime(pd.to_datetime(df.Last_Update).map(lambda x: x.strftime('%Y-%m-%d'))) 
    df = df.drop_duplicates(subset=["Province_State","Country_Region","Date"], keep="first")
    
    return df


def clean_country_names(df):
    country_names_mistakes = {
                            "Mainland China":"China",
                            "Viet Nam":"Vietnam", 
                            "Taiwan*":"Taiwan", 
                            "Hong Kong SAR":"Hong Kong,", 
                            "Gambia, The":"Gambia",
                            "Guinea-Bissau":"Guinea", 
                            "Czechia":"Czech Republic",
                            "Bahamas The":"Bahamas",
                            "Korea, South":"South Korea"
                             }
    df.Country_Region = df.Country_Region.replace(country_names_mistakes)
        
    return df
        
    
def get_per_country_data(df):
    return df.groupby(["Country_Region","Date"])["Confirmed","Deaths","Recovered"].sum().reset_index()


def get_specific_countries(df_per_country, list_of_countries):
    return df_per_country[df_per_country.Country_Region.isin(list_of_countries)]


def clean_data(dir_name=DIR_NAME):
    df = import_data(dir_name)
    df = clean_country_names(preprocess_data(df))
    df_per_country = get_per_country_data(df)
    return df_per_country


def get_european_data(dir_name=DIR_NAME):
    return get_specific_countries(clean_data(dir_name), ["France","Germany","Italy","Spain","United Kingdom"])

if __name__ == "__main__":
    print(clean_data())

