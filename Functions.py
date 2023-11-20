import os
import re
import pandas as pd

def keep_eliminate_matches_from_datasets(dataframe_to_transform: pd.DataFrame,
                                         column_from_transform_df: str,
                                         string_to_compare_with: list|str|tuple|pd.DataFrame,
                                         keep_matching: bool = True) -> pd.DataFrame:
    '''
    This function takes in one Pandas DataFrame,
    and compare exact matching string from :string_to_compare_with:.
    Then keep or eliminate rows that matches.
    Essentially a Left Outer Join or Inner Join Operations.

    :dataframe_to_transform:
    Pandas DataFrame to perform elimination. \n
    :dataframe_to_compare:
    Pandas DataFrame to compare with. \n
    :string_to_compare_with:
    Any stirng to compare with. Dtype: pd.DataFrame, list, string, tuple. \n
    :keep_matching:
    If TRUE, DataFrame keeps rows WITH matching terms;
    if FALSE, DataFrame keeps rows WITHOUT matching terms. \n
    :return: Pandas DataFrame
    '''
    if keep_matching is True:
        return dataframe_to_transform[dataframe_to_transform[column_from_transform_df
                                                             ].isin(string_to_compare_with)]
    return dataframe_to_transform[~dataframe_to_transform[column_from_transform_df
                                                          ].isin(string_to_compare_with)]

def create_email_domain_column(dataframe: pd.DataFrame,
                               column_containing_email: str,
                               new_column_name: str = "email_domain",
                               axis: int = 0) -> pd.DataFrame:
    '''
    This function takes in a Pandas DataFrame,
    And extract email domain from DataFrame column containing email addresses.

    :dataframe:
    Pandas DataFrame intent to perform transformation. \n
    :column_containing_email:
    Column header containing email addresses. \n
    :new_column_name:
    Name for the newly create column, defaut is "email_domain" if left empty. \n
    :return:
    DataFrame with new column containing email domain
    starting 1 element after "@" from :column_containing_email:.
    Return empty string for the cell,
    if value from :column_containing_email: does not contain element "@",
    '''
    stage_dataframe = dataframe.copy()
    stage_dataframe[new_column_name] = stage_dataframe[column_containing_email].apply(
        lambda x: x.split('@')[1] if isinstance(x, str) else '', axis= axis)
    return stage_dataframe

def extract_www_company_domain(strings_to_be_extracted: str|list(str)) -> str:
    '''
    This function takes in a str or a list of strings,
    And extractes company domain from a website.

    :strings_to_be_extracted:
    String or a list of strings to be extracted. \n
    :return:
    String or list of string between 'www.';'https://';'http://' and '.com'.
    '''
    if pd.isna(strings_to_be_extracted):
        return ' '
    if 'www.' in strings_to_be_extracted:
        start_string_search = strings_to_be_extracted.find('www.') + len('www.')
    elif 'https://' in strings_to_be_extracted:
        start_string_search = strings_to_be_extracted.find('https://') + len('https://')
    elif 'http://' in strings_to_be_extracted:
        start_string_search = strings_to_be_extracted.find('http://') +len('http://')
    else:
        start_string_search = 0
    end_string_search = strings_to_be_extracted.find('.com', start_string_search)
    if end_string_search == -1:
        return ' '
    return strings_to_be_extracted[start_string_search:end_string_search]

def lowercase_and_strip_punctuation(strings_to_be_cleaned: str|list(str)) -> str:
    '''
    This function takes in string or list of string,
    And apply lowercase to all elements,
    As well as stripping out all punctuations.

    :strings_to_be_cleaned:
    a string or list of strings to be filtered. \n
    :return:
    a string or list of strings free of puntuations and in lower cases.
    '''
    if not isinstance(strings_to_be_cleaned, str):
        strings_to_be_cleaned = str(strings_to_be_cleaned)
    # Strip all punctuation and lower all cases
    cleaned_string = re.sub(r'[^\w\s] \n', '', strings_to_be_cleaned)
    cleaned_string = cleaned_string.lower()
    return cleaned_string

def find_keywords(dataframe: pd.DataFrame, keyword_list: list(str),
                  target_column: str) -> list(str):
    '''
    This function find keyword from a keyword list

    :dataframe: Pandas DataFrame intent to perform transformation. \n
    :return: a list of result if there is a exact match from the keyword list,
    returns empty list if there is no match.
    '''
    appeared_keywords = [keyword for keyword in keyword_list
                         if keyword in dataframe[target_column]]
    return appeared_keywords

def map_category(dataframe: pd.DataFrame, category_dictionary: dict[str: [str]],
                 target_column: str) -> list(str):
    '''
    This function will find diction value from target column

    :dataframe:
    Pandas DataFrame intent to perform transformation. \n
    :return:
    a list of category if there is a exact match from :the category_dictionary:'s values,
    returns [None] if there is no match.
    '''
    categories = [category for category, keywords in category_dictionary.items()
                  if any(keyword in dataframe[target_column]
                         for keyword in keywords)]
    return categories if categories else [None]

def loop_through_data_files(direct_folder_path: str) -> list(pd.DataFrame):
    '''
    This function takes in the direct_folder_path and read all the csv and xlsx files.
    Then load each files into a Pandas DataFrame.
    If file has more than one sheet, each sheet will be loaded into individual DataFrame.

    :return:
    a list of DataFrame objects
    '''
    dataframe_list = []
    for filename in os.listdir(direct_folder_path):
        full_path = os.path.join(direct_folder_path, filename)
        print(f"Now loading {filename}")

        if filename.endswith('.csv'):
            dataframe = pd.read_csv(full_path)
            dataframe_list.append(dataframe)
        elif filename.endswith('.xlsx'):
            xls_file = pd.ExcelFile(full_path)
            sheets = xls_file.sheet_names
            if len(sheets) == 1:
                dataframe = pd.read_excel(full_path)
            else:
                print(f'{filename} has multiple sheets.')
                for sheet_name in sheets:
                    print(f'now working on {sheet_name}.')
                    dataframe = pd.read_excel(full_path, sheet_name= sheet_name)
                    dataframe_list.append(dataframe)
        print(f'Finished working on {filename}')
    print("End")
    return dataframe_list
