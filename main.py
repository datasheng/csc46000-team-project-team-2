#The code here will pull in the connections to the API and leverage the ETL modules in the src directory.
from src.main import compile_ETL_data
from config import api_keys


def main() -> None:
    print(api_keys['finnhub'])
    etl_data = compile_ETL_data(api_keys["finnhub"], api_keys["yahoo_fin"])
    print("ETL Data Compiled:", etl_data)



if __name__ == "__main__":
    main()