import pandas as pd
import ast
from datetime import datetime

file_path = os.getenv(file)

read_file = pd.read_csv(file_path)

header_name = read_file['PublicAssistanceFundedProjectsDetails'][0]

file = ast.literal_eval(header_name)

# turning the list of dictionaries into dataframe

df = pd.DataFrame(file)
