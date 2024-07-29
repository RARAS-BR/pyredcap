import pandas as pd
from pandas import Series

from pyredcap import CustomRulesBase


class CustomRules(CustomRulesBase):

    def diag_cid_equals_comorb_cid(self) -> dict:
        diag_comorb_df = pd.merge(
            self.forms['diagnostico'],
            self.forms['comorbidade'],
            on=['record_id']
        )
        diag_comorb_df['cid10_diagnostico'] = diag_comorb_df['doenca_cid10'].dropna().apply(
            lambda x: x.split(' - ')[1].strip())
        diag_equal_comorb_mask = diag_comorb_df['cid10_diagnostico'] == diag_comorb_df['cid10_comorbidade']
        invalid_records = diag_comorb_df[diag_equal_comorb_mask]['record_id'].tolist()

        return {
            'df': self.forms['comorbidade'],
            'column': 'cid10_comorbidade',
            'form_name': 'comorbidade',
            'invalid_records': invalid_records,
            'reason_desc': 'CID10 Diagnóstico igual ao CID10 comorbidade'
        }

    def check_invalid_cpf(self) -> dict:
        cpf_df: Series = self.forms['identificacao'].set_index('record_id')['cpf'].dropna().copy()

        valid_cpf_mask = cpf_df.apply(self.th.validate_cpf, return_value=False)
        invalid_records = cpf_df[~valid_cpf_mask].index.tolist()
        return {
            'df': self.forms['identificacao'],
            'column': 'cpf',
            'form_name': 'identificacao',
            'invalid_records': invalid_records,
            'reason_desc': 'CPF invalidado pelo algoritmo de verificação'
        }

    def check_missing_cpf(self) -> dict:
        cpf_df: Series = self.forms['identificacao'].set_index('record_id')['cpf'].copy()

        invalid_records = cpf_df[cpf_df.isna()].index.tolist()
        return {
            'df': self.forms['identificacao'],
            'column': 'cpf',
            'form_name': 'identificacao',
            'invalid_records': invalid_records,
            'reason_desc': 'CPF em branco ou uso de missing data code'
        }
