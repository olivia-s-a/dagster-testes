from dagster import asset

import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import matplotlib.pyplot as plt #estou usando ele pq o mapbox e o px não tão funfando

import duckdb
import os

from . import constants

#LESSON 4.6.: Assets with in-memory computations

# a) Creating metrics using assets
@asset(
    deps=["taxi_trips_canjiquinha", "taxi_zones_dobradinha"]
)
def manhattan_stats_escondidinho() -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    conn_escondidinho = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn_escondidinho.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

#b) Making a map
@asset(
    deps=["manhattan_stats_escondidinho"],
)
def manhattan_map_feijoada() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    
    trips_by_zone.plot(
        column='num_trips', 
        ax=ax, 
        legend=True,
        cmap='inferno',
        edgecolor='black'
    )

    ax.set_title('Número de Viagens por Zona em Manhattan')
    ax.set_axis_off()

    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, bbox_inches='tight')
    plt.close()

# LESSON 4.7.: Practice: Create a trips_by_week asset
"""@asset(
    deps=["taxi_trips_canjiquinha"]
)
def trips_by_week_galinhada() -> None:
    taxi_trips = taxi_trips_canjiquinha()

    period #String; sunday da semana; aaaa-mm-dd
    num_trips 
    passenger_count"""