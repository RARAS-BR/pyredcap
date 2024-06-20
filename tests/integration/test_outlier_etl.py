import os
import re
import copy
import dotenv
import logging
import yaml
import pytest
from pyredcap import REDCapProject, Outliers
from custom_rules import CustomRules

# SETUP
logging.basicConfig(level=logging.INFO)
DATA_PATH = 'tests/integration/data/'

# Load instructions files
with open(f'{DATA_PATH}preprocessing.yaml') as file:
    preprocessing_steps = yaml.load(file, Loader=yaml.FullLoader)
with open(f'{DATA_PATH}data_cleaning.yaml') as file:
    data_cleaning_steps = yaml.load(file, Loader=yaml.FullLoader)
# Credentials
dotenv.load_dotenv()
API_URL = os.getenv('API_URL')
API_TOKEN = os.getenv('TOKEN_PROSPECTIVO_2023')


@pytest.fixture(scope="module")
def raw_project():
    project = REDCapProject(API_URL, API_TOKEN)
    project.load_records(label_columns=[
        "doenca_0k_cid10", "doenca_lr_cid10", "doenca_sz_cid10",
        "doenca_0k_orpha", "doenca_lr_orpha", "doenca_sz_orpha",
        "doenca_0k_omim", "doenca_lr_omim", "doenca_sz_omim"
    ])
    return project


@pytest.fixture(scope="module")
def processed_project(raw_project):
    processed_project = copy.deepcopy(raw_project)
    processed_project.preprocess_forms(preprocessing_steps)
    # Temporary fix: avoid false positives in outlier detection
    processed_project.forms['identificacao']['peso_nascimento'] = (
        processed_project.forms['identificacao']['peso_nascimento']
        .dropna().apply(lambda s: re.sub(r"(?<=\d)\.(?=\d{3}$)", "", s)))
    return processed_project


@pytest.fixture(scope="module")
def clean_project(processed_project):
    clean_project = copy.deepcopy(processed_project)
    clean_project.clean_data(data_cleaning_steps)
    return clean_project


def test_preprocessing(raw_project):
    assert all([len(form) > 0 for form in raw_project.forms.values()])


def test_data_cleaning(processed_project):
    assert all([len(form) > 0 for form in processed_project.forms.values()])


def test_outliers_from_preprocess(processed_project):
    custom_rules = CustomRules(processed_project)
    out = Outliers(processed_project, custom_rules)
    out.generate_outliers()
    assert out.outliers_df is not None


# def test_outliers_from_cleaned(clean_project):
#     custom_rules = CustomRules(clean_project)
#     out = Outliers(clean_project, custom_rules)
#     out.generate_outliers()
#     assert out.outliers_df is not None


def test_save_outliers_from_preprocess(processed_project):
    custom_rules = CustomRules(processed_project)
    out = Outliers(processed_project, custom_rules)
    out.generate_outliers()
    out.outliers_df.to_csv(f'{DATA_PATH}outliers.csv', index=False)
    assert os.path.exists(f'{DATA_PATH}outliers.csv')
