import pandas as pd 
import os  
import warnings
import json
warnings.filterwarnings('ignore')

"""
This python files preprocess the John Hopkins Covid data 
"""

DIR_NAME = "../../csse_covid_19_data\csse_covid_19_daily_reports"
DIR_NAME_ZIPCODE_DATA = '../../additional_data'

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
    return df.groupby(["Country_Region","Date"]).apply(sum).loc[:,["Confirmed","Deaths","Recovered"]].reset_index()


def get_specific_countries(df_per_country, list_of_countries):
    return df_per_country[df_per_country.Country_Region.isin(list_of_countries)]


def clean_data(dir_name=DIR_NAME):
    df = import_data(dir_name)
    df = clean_country_names(preprocess_data(df))
    return df


def get_european_data(dir_name=DIR_NAME):
    return get_specific_countries(clean_data(dir_name), ["France","Germany","Italy","Spain","United Kingdom"])

def get_states_data(dir_name=DIR_NAME, 
                    dir_name_zipcode_data=DIR_NAME_ZIPCODE_DATA):

    df = clean_data(dir_name)
    df_usa = df[df.Country_Region == "US"]
    
    city_to_code = pd.read_csv(os.path.join(dir_name_zipcode_data, "us_city_to_code.csv"))
    city_to_code = city_to_code.loc[:,["city","state_abbr"]].set_index("city").to_dict()["state_abbr"]
    
    with open(os.path.join(dir_name_zipcode_data, "us_state_to_code.json")) as json_file:
        province_to_code = dict([[v,k] for k,v in json.load(json_file).items()])
    
    # get state code using province_state name 
    df_usa["Code"] = [el.split(",")[-1].strip() for el in df_usa.Province_State]
    df_usa["Code"] = df_usa["Code"].replace(city_to_code)
    df_usa["Code"] = df_usa["Code"].replace(province_to_code)
    
    error = []
    for province in df_usa.Code:
        if len(province) > 2:
            error.append(province)
    error = list(set(error))

    df_usa = df_usa[~ df_usa.Code.isin(error)]

    code_province = pd.read_csv("https://raw.githubusercontent.com/scpike/us-state-county-zip/master/geo-data.csv")
    code_province = code_province.loc[:,["city","state_abbr"]].set_index("city").to_dict()["state_abbr"]
    df_usa = df_usa.groupby(["Code","Date"])["Confirmed"].sum().reset_index().sort_values(by="Date")
    
    return df_usa


if __name__ == "__main__":
    print(get_states_data().head())

