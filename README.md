# pyredcap
Python REDCap tools to improve analytics

# Table of contents
- [How to install](#how-to-install)
- [How to use](#how-to-use)
  - [REDCapProject class](#redcapproject-class)
  - [Preprocessing class](#preprocessing-class)
  - [DataCleaning class](#datacleaning-class)

# How to install

```bash
# Upgrade pip
python -m pip install --upgrade pip 
# Install from github source
pip install "git+https://github.com/RARAS-BR/pyredcap.git"
```

How to add in `requirements.txt` file:
```txt
pyredcap @ git+https://github.com/RARAS-BR/pyredcap
```

# How to use

## REDCapProject class

The ``REDCapProject`` class is the main class of the package, responsible for interacting with the REDCap API.
It allows you to load the project metadata, records, and export the data in a pandas DataFrame format.

This is a simple example of how to load your REDCap project:
```python
import os
import yaml
import dotenv
import logging
from pandas import DataFrame
from pyredcap import REDCapProject, Preprocessing, DataCleaning

logging.basicConfig(level=logging.INFO)

# Straightforward method (NOT RECMMENDED)
api_url = 'https://redcap.example.com/api/'
api_token = 'MySecretToken'

# Secure method (recommended)
dotenv.load_dotenv()
api_url = os.getenv('REDCAP_API_URL')
api_token = os.getenv('REDCAP_API_KEY')

# Create a project instance
project = REDCapProject(api_url, api_token)
metadata = project.get_metadata()

# Load project records
project.load_records()

# Records preview
print(project.df.head())
```

## Preprocessing class

The Preprocessing class is designed to streamline the project structure, ensuring data integrity by preserving 
all records and information, except missing datacodes. It offers a variety of functionalities, including:  
- Rename instruments
- Remove missing datacodes
- Aggregate columns
- Decode checkboxex
- Subset forms
- Create new form
- Merge forms

```python
# Load preprocessing steps
with open('preprocessing.yaml') as file:
    preprocessing_steps = yaml.load(file, Loader=yaml.FullLoader)

# Apply preprocessing steps
preprocessing = Preprocessing(
    redcap_project=project,
    instructions=preprocessing_steps
)

# Load preprocessed data
project_forms: dict[str, DataFrame] = preprocessing.forms
```

Below is an example of a YAML file that can be used to define the preprocessing steps:
```yaml
remove_missing_datacodes:
aggregate_columns:
  agg_map:
    - search_str: column_name_or_substring
      col_name: your_new_column_name
decode_checkbox:
subset_forms:
```

## DataCleaning class

The DataCleaning class serves as a comprehensive tool for data preparation prior to analysis. 
It enforces the correct data types, identifies and eliminates outliers, and applies a variety of custom functions 
for data validation. These functions supplement the validation capabilities of the REDCap application, 
addressing tasks that cannot be performed within REDCap itself.  
  
The following options are available:
- Remove incomplete forms
- Remap categorical labels
- Remap boolean labels
- Enforce data types (int, float, datetime)
- Split columns with multiple information
- Fix/Scale values
- Custom validations
- Replace "*_other*" columns
- Rename features
- Drop features

```python
# Load data cleaning steps
with open('data_cleaning.yaml') as file:
    data_cleaning_steps = yaml.load(file, Loader=yaml.FullLoader)

# Apply data cleaning steps for each form
for form_name, instructions in data_cleaning_steps.items():
    data_cleaning = DataCleaning(
        form_name=form_name,
        df=preprocessing.forms[form_name],
        metadata=metadata,
        instructions=instructions
    )
    project_forms[form_name]: DataFrame = data_cleaning.df
```

Below is an example of a YAML file that can be used to define the data cleaning steps:
```yaml
your_first_form_name:
  remove_incomplete_forms:
  remap_categorical_labels:
  enforce_dtype:
    dtype_map:
      Int64:
        - int_column_name
      datetime:
        - date_column_name
  rename_features:
    mapping:
      original_column_name: new_column_name

your_second_form_name:
  remove_incomplete_forms:
  remap_categorical_labels:
  remap_boolean_labels:
    columns:
      - boolean_column_name
  drop_features:
    columns:
      - column_name_to_drop
```