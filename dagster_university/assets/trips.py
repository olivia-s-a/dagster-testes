#Vou deixar as coisas fora da ordem que deveriam estar só por motivos de aprendizado:
# está em ordem do que é feito primeiro, e não de como deveria ser feito o código.
import requests
from . import constants
#3.4.A) Importar o 'asset'
from dagster import asset

#LESSON 3.4.: Defining your first asset

#Por enquanto é só uma função
def taxi_trips_file_amora() -> None: #indica que não vai retornar nada
    """
        The raw parquet files from taxi trips dataset. Sourced from the NYC Ope Data portal.
    """
#as 3 aspas indicam que é a descrição do asset (vai aparecer no Dagster UI)
    month_to_fetch = '2023-03'
    raw_trips = requests.get( #requests library-> requests.get: retorna um parquet do negócio que a gente quer pegar.
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    ) #ah, aqui é a construção do url junto da data q vc quer, igual o Bruno fez no do geosampa

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
    #construir o path e salvar o arquivo
    #e aí é stored em assets/constants.py

#3.4.B) Transformar a função em um asset
@asset #ASSET DECORATOR: indic aque vai produzir um asset
def taxi_trips_file_banana(
    #o nome da função é o nome do asset-> ASSET KEY
) -> None: 
    """
        The raw parquet files from taxi trips dataset. Sourced from the NYC Ope Data portal.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
#e esta função toda é que define como o asset será computado
#ANATOMIA DE UM ASSET:
#   ASSET DECORATOR
#   ASSET KEY
#   SET OF UPSTREAM ASSET DEPENDANCIES (ainda não entendi o que isto aqui é)
#   PYTHON FUNCTION

# ---------()------------------

#LESSON 3.6.: Asset materialization

@asset
def taxi_trips_file_carambola() -> None:
    """The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal."""
    month_to_fetch = "2023-03"
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
        #assets/constant.py
        # TAXI_TRIPS_TEMPLATE_FILE_PATH = data/raw/taxi_trips_2023-03.parquet
    ) as output_file:
        output_file.write(raw_trips.content)
        
        
        
        
        
# ---------()------------------


#LESSON 3.9.: Practice: Create  taxi_zone_file asset

@asset
def taxi_zone_file() ->  None:
    """(a)This asset will contain a unique identifier and name for each part of NYC as a distinct taxi zone.
        Resp.: The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """

    raw_zones = requests.get(
        'https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD'
        )
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)
# tá certinho, diva, parabéns <3
