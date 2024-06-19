import logging
from typing import Literal

import numpy as np
import pandas as pd
from pandas import DataFrame


class Outliers:

    def __init__(
            self,
            forms: dict[str, DataFrame],
            codebook: DataFrame
    ):
        self.forms = forms
        self.validation_df = None
        self.cols_to_validate: dict = {}
        self.outliers_df = pd.DataFrame(
            columns=['record_id', 'redcap_data_access_group',
                     'redcap_repeat_instance', 'field_name', 'current_value',
                     'form_status', 'reason'])
        self._instance_validation_df(codebook)

    @staticmethod
    def _fix_min_max_dtype(
            df,
            type_value: list[str],
            conversion_func: callable,
    ) -> DataFrame:
        mask = df['type'].isin(type_value)
        df.loc[mask, ['min', 'max']] = df.loc[mask, ['min', 'max']].apply(
            conversion_func)
        return df

    @staticmethod
    def _get_field_names_from_type(
            df: DataFrame,
            type_value: list[str]
    ) -> list:
        mask = df['type'].isin(type_value)
        return df[mask]['field_name'].tolist()

    def _instance_validation_df(
            self,
            codebook: DataFrame
    ) -> None:
        # Use the codebook as starting point for the validation
        df = codebook.copy()
        # Select only text fields and exclude read-only fields
        field_type_mask = df['field_type'] == 'text'
        field_annot_mask = df['field_annotation'].str.contains(r'@READONLY')
        validation_columns = ['field_name', 'form_name', 'text_validation_type_or_show_slider_number',
                              'text_validation_min', 'text_validation_max', 'required_field', 'branching_logic']
        df = df[field_type_mask & ~field_annot_mask][validation_columns]
        # Fill empty values
        df.replace({'': np.nan}, inplace=True)
        # Rename to cleaner names
        df.rename(columns={
            'text_validation_type_or_show_slider_number': 'type',
            'text_validation_min': 'min',
            'text_validation_max': 'max',
        }, inplace=True)
        # Drop fields with no step to validate
        df.dropna(subset=['type', 'min', 'max', 'required_field', 'branching_logic'],
                  how='all',
                  inplace=True)
        # Drop 'resp_' columns
        df = df[~df['field_name'].str.contains(r'^resp_')]
        # Reset index
        df.reset_index(drop=True, inplace=True)

        # Setup numeric columns
        df = self._fix_min_max_dtype(df, ['number', 'integer'], pd.to_numeric)
        # Setup date columns
        df = self._fix_min_max_dtype(df, ['date_mdy', 'date_dmy'], pd.to_datetime)
        # Setup required_field
        df['required_field'] = df['required_field'].replace({'y': True}).fillna(False)
        field_required_mask = df['required_field'] == True
        # Setup Branching logic fields
        field_branching_mask = df['branching_logic'].notna()

        # Select only the columns that will be used for validation
        self.cols_to_validate['numeric_cols'] = self._get_field_names_from_type(df, ['number', 'integer'])
        self.cols_to_validate['date_cols'] = self._get_field_names_from_type(df, ['date_dmy'])
        self.cols_to_validate['required_cols']: list = df[field_required_mask]['field_name'].tolist()
        self.cols_to_validate['branching_cols']: list = df[field_branching_mask]['field_name'].tolist()

        self.validation_df = df

        # TODO: Apply preprocessing changes in codebook (Temporary fix)
        # Rename modified instruments
        self.validation_df['form_name'] = self.validation_df['form_name'].replace(
            {'dados_demograficos': 'identificacao'})
        # Rename modified columns
        self.validation_df['field_name'] = self.validation_df['field_name'].replace({'ss_1': 'sintomas'})

    def update_outliers_df(self, outliers: DataFrame) -> None:
        if not outliers.empty:
            self.outliers_df = pd.concat([self.outliers_df, outliers], ignore_index=True)

    def check_required_fields(self, column: str) -> list:
        if 'redcap_repeat_instance' in self.validation_df.columns:
            return (self.validation_df.loc[self.validation_df[column].isna(), ['record_id', 'redcap_repeat_instance']]
                    .apply(tuple, axis=1).tolist())
        return self.validation_df.loc[self.validation_df[column].isna(), 'record_id'].tolist()

    def process_invalid_records(
            self,
            df: DataFrame,
            column: str,
            invalid_records: list[str] | list[tuple[str, int]],
            reason_desc: str
    ) -> None:
        if not invalid_records:
            return None

        # Check if list is with str
        if isinstance(invalid_records[0], str):
            invalid_record_mask = df['record_id'].isin(invalid_records)
        else:
            invalid_record_mask = df[['record_id', 'redcap_repeat_instance']].apply(tuple, axis=1).isin(invalid_records)

        complete_column: str = df.filter(regex='_complete$').columns[-1]
        cols_subset: list = ['record_id', 'redcap_data_access_group', 'redcap_repeat_instance',
                             column, complete_column]
        if 'redcap_repeat_instance' not in df.columns:
            df['redcap_repeat_instance'] = np.nan

        form_outliers: DataFrame = df[invalid_record_mask][cols_subset]
        form_outliers['field_name'] = column
        form_outliers['reason'] = reason_desc
        form_outliers.rename(columns={column: 'current_value'}, inplace=True)
        form_outliers.rename(columns={complete_column: 'form_status'}, inplace=True)

        logging.info('Field %s: extracted %s invalid records', column, len(invalid_records))
        self.update_outliers_df(form_outliers)

    def check_dtype(
            self,
            df: DataFrame,
            column: str,
            data_type: Literal['numeric', 'date']
    ) -> None:
        if data_type == 'numeric':
            is_valid_type = pd.to_numeric(df[column].dropna(), errors='coerce').notna()
        elif data_type == 'date':
            is_valid_type = pd.to_datetime(df[column].dropna(), errors='coerce').notna()
        else:
            raise ValueError("data_type must be either 'numeric' or 'date'")

        # Fill missing indexes with True
        is_valid_type = is_valid_type.reindex(df.index, fill_value=True)

        # Return records that failed to convert
        invalid_records = df.loc[~is_valid_type, 'record_id'].tolist()

        if invalid_records:
            self.process_invalid_records(df, column, invalid_records,
                                         f'Invalid data type ({data_type})')

    def check_range(
            self,
            df: DataFrame,
            column: str,
            data_type: Literal['numeric', 'date'],
            min_value: str = None,
            max_value: str = None
    ) -> None:
        # Check if at least one of the values is not NaN
        if pd.notna(min_value) or pd.notna(max_value):
            if data_type == 'numeric':
                series = pd.to_numeric(df[column], errors='coerce').dropna()
            elif data_type == 'date':
                series = pd.to_datetime(df[column], errors='coerce').dropna()
            else:
                raise ValueError("data_type must be either 'numeric' or 'date'")

            if pd.notna(min_value) and pd.notna(max_value):
                mask = series.between(min_value, max_value)
            elif pd.notna(min_value):
                mask = series >= min_value
            elif pd.notna(max_value):
                mask = series <= max_value
            else:
                raise ValueError('No range provided')

            # Reindex with True values
            mask = mask.reindex(df.index, fill_value=True)

            invalid_records = df.loc[~mask, 'record_id'].tolist()
            if invalid_records:
                self.process_invalid_records(df, column, invalid_records,
                                             f'Value outside of range ({data_type})')

    def generate_outliers(self, filter_incomplete: bool = True) -> None:

        # General outlier detection
        for form_name, form in self.forms.items():
            form_name_mask: bool = self.validation_df['form_name'] == form_name
            # Iterate through all fields to be validated in this form
            for column in self.validation_df[form_name_mask]['field_name']:
                field_name_mask: bool = self.validation_df['field_name'] == column
                field_info: dict = self.validation_df[field_name_mask].to_dict(orient='records')[0]

                # Check by field type
                if column in self.cols_to_validate['numeric_cols']:
                    self.check_dtype(form, column, 'numeric')
                    self.check_range(form, column, 'numeric', field_info['min'], field_info['max'])
                elif column in self.cols_to_validate['date_cols']:
                    self.check_dtype(form, column, 'date')
                    self.check_range(form, column, 'date', field_info['min'], field_info['max'])

        # CUSTOM OUTLIER DETECTIONS GO HERE
        # self.custom_diag_comorb_cid()

        # Format data frame and filter incomplete records
        self.outliers_df['form_status'] = self.outliers_df['form_status'].replace({
            0: 'incomplete',
            1: 'unverified',
            2: 'complete'
        })
        self.outliers_df.rename(columns={
            'redcap_repeat_instance': 'instance',
        }, inplace=True)
        self.outliers_df['instance'] = self.outliers_df['instance'].astype('Int64')
        if filter_incomplete:
            self.outliers_df = self.outliers_df[self.outliers_df['form_status'] == 'complete']


# TODO: Custom rules class
# from pyredcap.redcap_project import REDCapProject
# class CustomRules:
#
#     def __init__(self, redcap_project: REDCapProject):
#         self.forms: dict[str, DataFrame] = redcap_project.forms
#         self.codebook: DataFrame = redcap_project.codebook
#
#     # USER DEFINE CUSTOM METHODS BELOW
#     def custom_diag_comorb_cid(self) -> None:
#         diag_comorb_df = pd.merge(
#             self.forms['diagnostico'],
#             self.forms['comorbidade'],
#             on=['record_id']
#         )
#         diag_comorb_df['cid10_diagnostico'] = diag_comorb_df['doenca_cid10'].dropna().apply(
#             lambda x: x.split(' - ')[1].strip())
#         diag_equal_comorb_mask = diag_comorb_df['cid10_diagnostico'] == diag_comorb_df['cid10_comorbidade']
#         invalid_records = diag_comorb_df[diag_equal_comorb_mask]['record_id'].tolist()
#         if invalid_records:
#             self.process_invalid_records(
#                 self.forms['comorbidade'], 'cid10_comorbidade', invalid_records,
#                 'CID10 Diagnóstico igual ao CID10 comorbidade')
