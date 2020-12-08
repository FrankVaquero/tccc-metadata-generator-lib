import pandas as pd

standard_fields_list = ['dct.accessRights', 'dct.accrualMethod', 'dct.accrualPeriodicity', 'dct.audience', 'dct.mediator', 'dct.created', 'dct.description', 'dct.format', 'dct.identifier', 'dct.language', 'dct.licence', 'dct.relation', 'dct.spatial', 'dct.subject', 'dct.temporal', 'dct.title', 'opi.mediatorContact', 'opi.dataFilePath', 'opi.dataDictionaryPath', 'dct.publisher', 'opi.publisherContact']

standard_fields = dict(zip(standard_fields_list, ['' for i in standard_fields_list]))


def generate_metadata(blob_path:str, defined_fields:dict, data_description_dictionary:dict, spark, infer_dtypes_from_file:bool=True):
    """
    This function generates two data frames associated to a file stored in Azure Blob Storage, the first one is the metadata file with information regarding the specific description of the file, like audience, format and publisher; the second one is the data dictionary, that has the description of each of the columns in the file, along with their corresponding data type, if it is a required field and if it is prumary key.
    
    The function is intended to help with the metadata generation for the processed files created in any of the projects involving OPI Analytics and TCCC. It assumes that we are using Azure Blob storage to keep our files and uses spark to read the corresponding file to which the metadata and data dictionary belongs to.

    Inputs
    -------------------------------------------------------
    
    - blob_path: path to the file to which the generated files corresponds to.
    
    - defined_fields: dictionary containing definition of the fields for the metadata.
    
    - data_description_dictionary: dictionary with data describing the columns in the file. 
        - The expected format of this field is: {'column_name':{'dtype':data type of the column, 'description': the column description, 'primary_key': is the column part of the primary key?, 'requiered': is the field requiered?}}

    - spark: a SparkSession instance.
    
    - infer_dtypes_from_file: if true, read the file from the blob path using Spark and infer the data types from the schema.

    Returns 
    -------------------------------------------------------

    - metadata: pandas.DataFrame
    - data_dictionary: pandas:DataFrame

    """

    for key in data_description_dictionary.keys():
        input_keys = sorted(list(data_description_dictionary[key].keys()))
        expected_keys = sorted(['dtype', 'description', 'primary_key', 'required'])
        if  input_keys != expected_keys:
            missing = list(set(expected_keys).difference(set(input_keys)))
            print('The following keys {} are missing in the field {}'.format(missing, key))
            raise ValueError('The fields "dtype", "description", "primary_key" and "required" are needed for every key in the description dictionary.')

    required_metadata_fields = ['dct.format']
    
    for field in required_metadata_fields: 
        if field not in defined_fields:
            print('Requiered field "{}" is missing.'.format(field))
            raise ValueError

    fields_difference = set(list(defined_fields.keys())).difference(set(standard_fields_list))

    if len(fields_difference)>0:
        print('\nWARNING: There are names outside the Standard Fields for the Metadata.')
        print('######## Dropping leftover fields:', fields_difference)
        for key in fields_difference:
            defined_fields.pop(key)

    standard_fields.update(defined_fields)

    standard_fields['opi.dataFilePath'] = blob_path

################################ METADATA #########################################

    metadata = pd.DataFrame.from_dict(standard_fields, orient='index', columns=['value']).reset_index()
    metadata.rename(columns={'index':'term'}, inplace=True)

############################## DATA DICTIONARY ####################################

    aux_description_data = pd.DataFrame.from_dict(data_description_dictionary).T.reset_index()
    aux_description_data.rename(columns={'index':'column'}, inplace=True)

    if infer_dtypes_from_file:
        file_format = defined_fields['dct.format']
        if file_format == "parquet":
            data_dictionary = spark.read.format("parquet").load(blob_path)
        elif file_format == "csv":
            data_dictionary = spark.read.format("csv").option("header", "true").load(blob_path)
        
        data_dictionary = pd.DataFrame(data_dictionary.dtypes, columns=['column', 'dtype'])
        
        data_dictionary = data_dictionary.merge(aux_description_data, on='column', how='left', suffixes=['', '_aux'])

        if 'dtype_aux' in data_dictionary.columns:
            data_dictionary.loc[data_dictionary.dtype_aux.notnull(), 'dtype'] = data_dictionary.loc[data_dictionary.dtype_aux.notnull(), 'dtype_aux']
            data_dictionary.drop('dtype_aux', axis=1, inplace=True)
    else:
        data_dictionary = aux_description_data

    return metadata, data_dictionary