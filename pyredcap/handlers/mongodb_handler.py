import logging
import pandas as pd
from pymongo import MongoClient
from pandas import DataFrame


def load_project(
        db_name: str,
        conn_url: str = 'mongodb://localhost:27017/'
) -> tuple[dict[str, DataFrame], dict[str, str]]:
    """
    Loads data from MongoDB.

    Parameters
    ----------
    db_name : str
        The name of the database.
    conn_url : str
        The connection URL to the MongoDB instance.

    Returns
    -------
    tuple[dict[str, DataFrame], dict[str, str]]
        A tuple containing two elements:
        1. A dictionary with the forms as keys and DataFrames as values.
        2. A dictionary with metadata information.
    """

    client = MongoClient(conn_url)
    db = client[db_name]
    logging.info('Connected to MongoDB: %s at %s', db_name, conn_url)

    forms = {}
    metadata = {}
    for collection in db.list_collection_names():
        logging.info('Loading collection: %s', collection)
        forms[collection] = list(db[collection].find({}, {"_id": 0}))  # Exclude _id
        if collection != '_metadata':
            forms[collection] = pd.DataFrame(forms[collection])
        else:
            metadata = forms['_metadata'][0]
            del forms['_metadata']

    client.close()

    return forms, metadata


def load_multiple_projects(
        db_names: list[str],
        project_names: list[str],
        form_names: list[str] = None,
        filter_valid_records: bool = True,
) -> dict[str, DataFrame]:
    if form_names is None:
        form_names = ['inclusao',
                      'identificacao',
                      'diagnostico',
                      'tratamento',
                      'internacao',
                      'comorbidade',
                      'seguimento',
                      'abep']
    data_frames = {form_name: pd.DataFrame() for form_name in form_names}

    for project_name, db_name in zip(project_names, db_names):
        logging.info('Project: %s', project_name)
        forms, _ = load_project(db_name)
        # Remove any form that is not in the list
        forms = {k: v for k, v in forms.items() if k in data_frames.keys()}

        valid_records: list[str] = []
        if filter_valid_records:
            id_mask = forms['identificacao']['record_id'].isin(forms['inclusao']['record_id'])
            valid_records = forms['identificacao'].loc[id_mask, 'record_id'].unique()

        map_records: bool = False
        import_map: dict = {}
        if 'record_id_importacao' in forms['inclusao'].columns:
            import_map = (
                forms['inclusao'][['record_id', 'record_id_importacao']]
                .dropna(subset='record_id_importacao')
                .set_index('record_id').to_dict()['record_id_importacao']
            )
            map_records = True

        for form_name, df in forms.items():
            # Filter records that are in the 'identificacao' form
            if filter_valid_records:
                record_mask = df['record_id'].isin(valid_records)
                df = df[record_mask].reset_index(drop=True)
            # Add project name to the DataFrame
            df.insert(0, 'estudo_id', project_name)
            # Map record_id with older projects
            if map_records:
                df.insert(1, 'unique_id', df['record_id'].replace(import_map))
            else:
                df.insert(1, 'unique_id', df['record_id'])
            data_frames[form_name] = pd.concat([data_frames[form_name], df], axis='rows', ignore_index=True)

    return data_frames
