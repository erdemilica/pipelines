import dlt
from dlt.sources.helpers import requests
import datetime


@dlt.source
def stockapi_source(api_secret_key=dlt.secrets.value):
    return stockapi_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {api_secret_key}"}
    return headers


@dlt.resource(write_disposition="append")
def stockapi_resource(api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)
    
    api_key = api_secret_key
    stock = 'AMZN' # Enter the ticker of the stock you want here
    n_news = 30 # Enter how much news you want to receive here
    current_date = datetime.date.today()
    start_date = current_date - datetime.timedelta(days=2) #From how many day ago you want to receive the news //currently from two days ago to current date
    end_date = current_date
    offset = 0
    
    url = f'https://eodhistoricaldata.com/api/news?api_token={api_key}&s={stock}&limit={n_news}&offset={offset}&from={start_date}&to={end_date}'
    
    response = requests.get(url)
    response.raise_for_status()
    yield response.json()

if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='stockapi', destination='duckdb', dataset_name='stockapi_data'
    )

    # print credentials by running the resource
    data = list(stockapi_resource())

    # print the data yielded from resource
    print(data)
    

    # run the pipeline with your parameters
    load_info = pipeline.run(stockapi_source())

    # pretty print the information on data that was loaded
    print(load_info)
