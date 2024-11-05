from dagster import asset

from datetime import datetime, timedelta
import pandas as pd
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
@asset(
    deps=["taxi_trips_canjiquinha"]
)
def trips_by_week_galinhada() -> None:
    conn_galinhada = duckdb.connect(os.getenv("DUCKDB_DATABASE"))

    current_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)

    result = pd.DataFrame()

    while current_date < end_date:
        current_date_str = current_date.strftime(constants.DATE_FORMAT)
        query = f"""
            select
                vendor_id, total_amount, trip_distance, passenger_count
            from trips
            where date_trunc('week', pickup_datetime) = date_trunc(
                'week', 
                '{current_date_str}'::date
            )
        """

        data_for_week = conn_galinhada.execute(query).fetch_df()

        aggregate = data_for_week.agg({
            "vendor_id": "count",
            "total_amount": "sum",
            "trip_distance": "sum",
            "passenger_count": "sum"
        }).rename({"vendor_id": "num_trips"}).to_frame().T # type: ignore

        aggregate["period"] = current_date

        result = pd.concat([result, aggregate])

        current_date += timedelta(days=7)

    # clean up the formatting of the dataframe
    result['num_trips'] = result['num_trips'].astype(int)
    result['passenger_count'] = result['passenger_count'].astype(int)
    result['total_amount'] = result['total_amount'].round(2).astype(float)
    result['trip_distance'] = result['trip_distance'].round(2).astype(float)
    result = result[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]
    result = result.sort_values(by="period")

    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False) 
