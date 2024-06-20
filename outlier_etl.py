import os
import re
import dotenv
import logging
import yaml
from datetime import datetime
from pyredcap import REDCapProject, Outliers
from tests.integration.custom_rules import CustomRules

# %% SETUP
logging.basicConfig(level=logging.INFO)

# Load instructions files
with open('tests/integration/data/preprocessing.yaml') as file:
    preprocessing_steps = yaml.load(file, Loader=yaml.FullLoader)
with open('tests/integration/data/data_cleaning.yaml') as file:
    data_cleaning_steps = yaml.load(file, Loader=yaml.FullLoader)
# Credentials
dotenv.load_dotenv()
API_URL = os.getenv('API_URL')
API_TOKEN = os.getenv('TOKEN_PROSPECTIVO_2023')

# EXTRACT: Load project records
project = REDCapProject(API_URL, API_TOKEN)
metadata = project.get_metadata()
project.load_records(label_columns=[
    "doenca_0k_cid10", "doenca_lr_cid10", "doenca_sz_cid10",
    "doenca_0k_orpha", "doenca_lr_orpha", "doenca_sz_orpha",
    "doenca_0k_omim", "doenca_lr_omim", "doenca_sz_omim"
])

# TRANSFORM: Preprocessing and Data Cleaning
logging.info('Instantiating Preprocessing')
project.preprocess_forms(preprocessing_steps)
# Temporary fix: avoid false positives in Outliers for field_name 'peso_nascimento'
project.forms['identificacao']['peso_nascimento'] = (
    project.forms['identificacao']['peso_nascimento']
    .dropna().apply(lambda s: re.sub(r"(?<=\d)\.(?=\d{3}$)", "", s)))

# TRANSFORM: Generate outliers
custom_rules = CustomRules(project)
out = Outliers(project, custom_rules)
out.generate_outliers()

# %% LOAD: Save outliers_df
current_date: str = datetime.now().strftime("%d_%m_%Y")
out.outliers_df.to_csv(f'tests/integration/data/outliers_{current_date}.csv',
                       index=False)
