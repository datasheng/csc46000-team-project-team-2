from src.Extract.main import compile_extracted_data

"""
This file will serve as the main ETL orchestrator, 
calling the necessary functions from the Extract, Transform, and Load modules.
The data will then be passed up to the root main.py 
because that root main will be the entry point for the entire application!

"""



def compile_ETL_data(api_1='api_1', api_2='api_2') -> dict:
    # Call the compile_extracted_data function with the appropriate API keys
    data = compile_extracted_data(api_1_key=api_1, api_2_key=api_2)
    return data