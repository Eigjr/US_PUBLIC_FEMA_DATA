import os
from sqlalchemy import create_engine as ce
from dotenv import load_dotenv
from cleaning_data import df


load_dotenv()

col_name = df.columns


# Creadential for database connection

db_name=os.getenv("name")
db_user=os.getenv("user")
db_host=os.getenv("host")
db_password=os.getenv("password")
db_port=os.getenv("port")



def star_schema(df, engine, schema_name):
    """
    Build star schema tables (dimensions + fact) from FEMA dataset.
    
    Load the dataset into postgres database and create the star schema tables.

    Parameters:
        df (pd.DataFrame): Input raw dataset.
    
    Returns:
        dict: Dictionary containing fact and dimension DataFrames.
    """

    Dim_applicant_details = df[
        ["Applicant_id", "Application_title", "Incident_type",
                            "County", "County_code", "State"]
                            ].drop_duplicates()

    Dim_disaster_details = df[
    ["Disaster_number", "Incident_type", "Declaration_date_Month", 
                                "Declaration_date_Year"]
                                ].drop_duplicates()
                
    Fact_funded_projects = df[
    ["Project_amount", "Federal_share_obligated", "Total_obligated", 
                        "Mitigation_amount","Incident_type", "Obligated_date_Month", 
                        "Obligated_date_Year", "State_number_code", "State_code", "State"]
                        ].drop_duplicates()

    Dim_project_details = df[
        ["Project_worksheet(pw)_number", "Project_size",
                        "Donation_control_center_(DCC)", "Incident_type"]
                        ].drop_duplicates()

    Dim_damage_details = df[
        ["Damage_category", "Damage_category_code", 
                    "Obligated_date_Month","Obligated_date_Year", "State_number_code"]
                    ].drop_duplicates()

    fema_tables = {
        "Dim_applicant_details": Dim_applicant_details,
        "Dim_project_details": Dim_project_details, 
        "Dim_disaster_details": Dim_disaster_details,
        "Dim_damage_details": Dim_damage_details,
        "Fact_funded_projects": Fact_funded_projects
    }
    
    for name, table in fema_tables.items():
        table.to_sql(name,engine, if_exists="replace", index=False, schema=schema_name)
        print(f"loaded {name} table into {schema_name} schema")

    return fema_tables

    # Create database connection


engine = ce (
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )

save_data_mode = star_schema(df, engine, schema_name='public')
print("Tables returned:", list(save_data_mode.keys()))
