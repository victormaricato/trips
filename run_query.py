import geopandas as gpd
from sqlalchemy import create_engine

engine = create_engine('postgresql://pgis:1234@localhost:5432/geodatabase')

LEFT = -10
BOTTOM = -10
RIGHT = 50
TOP = 90


def main() -> None:
    result = gpd.GeoDataFrame.from_postgis(
        f"""
            with trips_in_bouding_box as (
                SELECT geometry, cast(datetime as date) as trip_date
                FROM trips
                WHERE trips.geometry && ST_MakeEnvelope({LEFT}, {BOTTOM}, {RIGHT}, {TOP}, 4326)
            ),
            trips_per_week as (
                SELECT extract(week from trip_date) as week, count(*) as trips, max(geometry) as geometry
                FROM trips_in_bouding_box
                GROUP BY 1
            )
            SELECT avg(trips) as avg_trips_per_week, max(geometry) as geometry
            FROM trips_per_week
        """,
        con=engine,
        geom_col='geometry',
    )
    avg_trips_per_week = result.iloc[0].avg_trips_per_week
    print(f"""
        Bounding Box: {LEFT}, {BOTTOM}, {RIGHT}, {TOP}
        Average weekly trips: {avg_trips_per_week}
        """
    )


if __name__ == '__main__':
    main()
