import re
import logging
from typing import Union

import numpy as np
from pandas import DataFrame


class TransformerHandler:
    """
    A class used to handle data transformations.

    ...

    Methods
    -------
    run_instructions(transformer: any, instructions: dict[str, any]) -> None
        Run instructions for a transformer object.
    replace_label(x, mapping: dict) -> any
        Replace values in a Series according to a mapping dictionary.
    replace_array_item(index: int, value: any, df: DataFrame, other_column: str, other_value: str) -> any
        Replace an item in an array if it contains the other_value.
    extract_ids_and_descriptions(x: str, pattern: str) -> Union[list[str], float]
        Extract IDs and descriptions from a string based on a pattern.
    validate_cpf(cpf: str, return_value: bool = True)
        Algorithm to validate CPF numbers.
    validate_cns(cns: str, return_value: bool = True)
        Algorithm to validate CNS numbers.
    """

    @staticmethod
    def run_instructions(transformer: any, instructions: dict[str, any]) -> None:
        """
        Run instructions for a transformer object.

        Parameters
        ----------
        transformer : any
            The transformer object to be used.
        instructions : dict[str, any]
            A dictionary containing the instructions to be executed.
            The keys are the method names and the values are the parameters to be passed to the method.

        Returns
        -------
        None
        """
        for method_name, params in instructions.items():
            method = getattr(transformer, method_name, None)

            if method is None or not callable(method):
                class_name = transformer.__class__.__name__
                raise ValueError(f"{method_name} is not a valid method of the {class_name} class")

            if params is None:
                method()
            elif isinstance(params, list):
                method(*params)
            elif isinstance(params, dict):
                method(**params)
            else:
                raise ValueError(f"Invalid parameters for method {method_name}")

    @staticmethod
    def replace_label(x, mapping: dict) -> any:
        """Replace values in a Series according to a mapping dictionary."""
        try:
            # Case when the column has been transformed by the decode_checkbox method
            if isinstance(x, (np.ndarray, list)):
                return [mapping[i] for i in x]
            # General case
            return mapping[x]
        except KeyError:
            return x

    @staticmethod
    def replace_array_item(
            index: int,
            value: any,
            df: DataFrame,
            other_column: str,
            other_value: str
    ) -> any:
        """Replace an item in an array if it contains the other_value."""
        if isinstance(value, (list, np.ndarray)):
            # Replace array item only if it contains the other_value
            return [df.loc[index, other_column] if item.lower() == other_value else item for item in value]

        return value

    @staticmethod
    def extract_ids_and_descriptions(x: str, pattern: str) -> Union[list[str], float]:
        """ Extract IDs and descriptions from a string based on a pattern."""
        # Remove multiple whitespaces before applying the pattern
        x = re.sub(r'\s+', ' ', x)
        # Find all matches of the pattern in the string
        matches = re.findall(pattern, x)
        # If matches are found, join them and extract the description
        if matches:
            extracted_id = ' - '.join(matches)
            # Find the last match in the string
            last_match = matches[-1]
            # Find the position of the last match
            last_match_position = x.rfind(last_match)
            # Extract the description based on the position of the last match
            extracted_description = x[last_match_position + len(last_match) + 3:].strip()
        else:
            logging.warning('No matches found for %s.', x)
            return np.nan

        return [extracted_id, extracted_description]

    @staticmethod
    def validate_cpf(cpf: str, return_value: bool = True):
        """Algorithm to validate CPF numbers."""
        false = np.nan if return_value else False

        # Cast to list and remove non-digit characters
        cpf = [int(i) for i in str(cpf) if i.isdigit()]

        # Check if the CPF has the correct number of digits
        if len(cpf) != 11:
            return false

        # Calculate the first verification digit
        product_sum = sum(a * b for a, b in zip(cpf[:9], range(10, 1, -1)))
        expected_digit = (product_sum * 10 % 11) % 10
        if cpf[9] != expected_digit:
            return false

        # Calculate the second verification digit
        product_sum = sum(a * b for a, b in zip(cpf[:10], range(11, 1, -1)))
        expected_digit = (product_sum * 10 % 11) % 10
        if cpf[10] != expected_digit:
            return false

        # Valid CPF
        return ''.join(str(i) for i in cpf) if return_value else True

    @staticmethod
    def validate_cns(cns: str, return_value: bool = True):
        """
        Algorithm to validate CNS numbers.
        source: https://integracao.esusab.ufsc.br/v211/docs/algoritmo_CNS.html
        """
        false = np.nan if return_value else False
        true = cns if return_value else True

        # Check size
        if len(cns.strip()) != 15:
            return false

        # Routine for numbers starting in 1 or 2
        if cns[0] in ['1', '2']:

            pis = cns[:11]
            soma = sum((int(pis[i]) * (15 - i)) for i in range(11))
            resto = soma % 11
            dv = 11 - resto if resto != 0 else 0

            if dv == 10:
                soma = sum((int(pis[i]) * (15 - i)) for i in range(11)) + 2
                resto = soma % 11
                dv = 11 - resto if resto != 0 else 0
                resultado = pis + "001" + str(int(dv))
            else:
                resultado = pis + "000" + str(int(dv))

            if cns == resultado:
                return true
            return false

        # Routine for numbers starting in 7,8 or 9
        if cns[0] in ['7', '8', '9']:

            soma = sum((int(cns[i]) * (15 - i)) for i in range(15))
            resto = soma % 11

            if resto == 0:
                return true
            return false
        return false

    @staticmethod
    def process_invalid_records(
            df: DataFrame,
            column: str,
            form_name: str,
            invalid_records: list[str] | list[tuple[str, int]],
            reason_desc: str
    ) -> DataFrame:
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
        form_outliers['form_name'] = form_name
        form_outliers['reason'] = reason_desc
        form_outliers.rename(columns={column: 'current_value'}, inplace=True)
        form_outliers.rename(columns={complete_column: 'form_status'}, inplace=True)

        logging.info('Field %s: extracted %s invalid records', column, len(invalid_records))
        return form_outliers
