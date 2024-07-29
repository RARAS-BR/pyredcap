# PyRedcap
![Static Badge](https://img.shields.io/badge/version-0.1.1-1696d2?style=for-the-badge)
![Static Badge](https://img.shields.io/badge/build-passing-55b748?style=for-the-badge)
![Static Badge](https://img.shields.io/badge/lynt-9.1-55b748?style=for-the-badge)
![Static Badge](https://img.shields.io/badge/coverage-100%25-55b748?style=for-the-badge)

REDCap integration tools to improve data analysis and data quality.

# Table of contents

- [Installation](#installation)
- [ETL example](#etl-example)
- [Quick Start Guide](#quick-start-guide)
    - [REDCapProject class](#redcapproject-class)
    - [Preprocessing class](#preprocessing-class)
    - [DataCleaning class](#datacleaning-class)
    - [Outlier detection](#outlier-detection)

# Installation

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

# ETL example

This is the minimal setup to run all steps of the Extract, Transform and Load process:

```python
import os
import yaml
from pyredcap import REDCapProject

# Setup credentials
api_url = os.getenv('REDCAP_API_URL')
api_token = os.getenv('REDCAP_API_KEY')
# Load your instructions file, optional for preprocessing only
with open('preprocessing.yaml') as file:
    preprocessing_steps = yaml.load(file, Loader=yaml.FullLoader)
with open('data_cleaning.yaml') as file:
    data_cleaning_steps = yaml.load(file, Loader=yaml.FullLoader)
# Create a project instance
project = REDCapProject(api_url, api_token)

# EXTRACT: load raw data from REDCap API
project.load_records()

# TRANSFORM: Preprocessing and data cleaning steps
project.preprocess_forms(preprocessing_steps)
project.clean_data(data_cleaning_steps)

# LOAD: save each form as csv locally in the 'data' folder
project.to_csv(dir_path='data')
```

# Quick Start Guide

## REDCapProject class

The ``REDCapProject`` class is the main class of the package, responsible for interacting with the REDCap API.
It allows you to load the project metadata, records, and export the data in a pandas DataFrame format.

This is a simple example of how to load your REDCap project:

```python
import os
import dotenv
from pyredcap import REDCapProject

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

# Apply preprocessing steps with the desired instructions
project.preprocess_forms(preprocessing_steps)

# Access preprocessed data
project_forms: dict[str, DataFrame] = project.forms
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

Using `preprocess_forms` without any instructions is equivalent to the following steps:

```python
from pyredcap import Preprocessing

preprocessing = Preprocessing(
    redcap_project=project,
    instructions=preprocessing_steps
)
preprocessing.remove_missing_datacodes()
preprocessing.decode_checkbox()
preprocessing.subset_forms()
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
from pyredcap import DataCleaning

# Load data cleaning steps
with open('data_cleaning.yaml') as file:
    data_cleaning_steps = yaml.load(file, Loader=yaml.FullLoader)

# Straightforward method
project.clean_data(data_cleaning_steps)

# Or apply data cleaning steps individually
for form_name, instructions in data_cleaning_steps.items():
    data_cleaning = DataCleaning(
        form_name=form_name,
        df=preprocessing.forms[form_name],
        metadata=metadata,
        instructions=instructions
    )
    project_forms[form_name]: DataFrame = data_cleaning.df
```

:warning: This method doesn't work without providing the instructions steps.   
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

## Outlier detection

The outlier detection module is designed to calidate data which REDCap isn't able to.
It works with both data after preprocessing or data cleaning, but can't be used on raw data.  
Below is the minimal usage examle:

```python
from pyredcap import Outliers

# Create a project instance after the preprocessing steps or data cleaning
out = Outliers(project)

# Create outliers data frame as an attribute
out.generate_outliers()

# Preview of outliers data frame
out.outliers_df.head()

| record_id | data_access_group | instance | current_value | form_status | field_name     | reason                                      |
|-----------|-------------------|----------|---------------|-------------|----------------|---------------------------------------------|
| 1-130     | dag_a             | 1        | 3020-02-01    | complete    | diagnose_date  | Invalid data (datetime)                     |
| 1-42      | dag_a             |          | 1.70          | unverified  | height         | Value outside range (min: 80.0, max: 230.0) |
| 2-15      | dag_b             |          | 3.300         | complete    | birth_weigth   | Value outside range (min: 500)              |
| 5-39      | dag_e             | 2        | 2023-04-14    | incomplete  | interview_date | Value outside range (min: 2024-05-04)       |
| 1-29      | dag_a             |          | 1011043154    | complete    | social_id      | Invalid ID detected by validation algorithm |
```

This module also provides the capability to define custom outlier detection rules within a Python class. The following
example demonstrates this functionality by validating social security numbers (CPF). It checks for invalid CPF numbers
and identifies fields that previously allowed the insertion of missing data codes but are
now missing data. Every function should return a dictionary containing information about the field name, a list of
invalid records and the reason for the outlier.  

```python
from pyredcap import CustomRulesBase


class CustomRules(CustomRulesBase):

    def check_invalid_cpf(self) -> dict:
        cpf_df: Series = self.forms['identificacao'].set_index('record_id')['cpf'].dropna().copy()

        valid_cpf_mask = cpf_df.apply(self.th.validate_cpf, return_value=False)
        invalid_records = cpf_df[~valid_cpf_mask].index.tolist()
        return {
            'df': self.forms['identificacao'],
            'column': 'cpf',
            'invalid_records': invalid_records,
            'reason_desc': 'CPF invalidado pelo algoritmo de verificação'
        }

    def check_missing_cpf(self) -> dict:
        cpf_df: Series = self.forms['identificacao'].set_index('record_id')['cpf'].copy()

        invalid_records = cpf_df[cpf_df.isna()].index.tolist()
        return {
            'df': self.forms['identificacao'],
            'column': 'cpf',
            'invalid_records': invalid_records,
            'reason_desc': 'CPF em branco ou uso de missing data code'
        }

    
# Outlier detection with custom rules
custom_rules = CustomRules(project)
out = Outliers(project, custom_rules)
out.generate_outliers()
```
