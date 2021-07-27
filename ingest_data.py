import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
from dask.diagnostics import ProgressBar
from sqlalchemy import create_engine

engine = create_engine('postgresql://pgis:1234@localhost:5432/geodatabase')


def main() -> None:
    df = dd.read_csv("trips.csv")
    preprocessed_df = preprocess(df)
    geo_df = dg.from_dask_dataframe(preprocessed_df)
    geo_df = geo_df.set_geometry(dg.points_from_xy(geo_df, 'origin_longitude', 'origin_latitude'))
    with ProgressBar():
        gpd.GeoDataFrame(geo_df.compute()).to_postgis("trips", engine, index=True)



def preprocess(df: dd.DataFrame) -> dd.DataFrame:
    origin_coordinates = _get_coordinates(df.origin_coord)
    destination_coordinates = _get_coordinates(df.destination_coord)
    df = df.assign(
        origin_latitude=origin_coordinates.map(lambda x: x[0]),
        origin_longitude=origin_coordinates.map(lambda x: x[1]),
    )
    df = df.assign(
        destination_latitude=destination_coordinates.map(lambda x: x[0]),
        destination_longitude=destination_coordinates.map(lambda x: x[1]),
    )
    df = df.drop("origin_coord", axis=1)
    df = df.drop("destination_coord", axis=1)
    return df


def _get_coordinates(coordinates_unparsed: dd.Series) -> dd.Series:
    splitted_origin_coord = coordinates_unparsed.str.split(" ")
    return splitted_origin_coord.apply(lambda x: (x[1][1:], x[2][:-1]))


if __name__ == "__main__":
    main()
