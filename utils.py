import numpy as np
import polars as pl


def compute_distance(df: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the trajectory length for each MMSI in the DataFrame.
    Use the Haversine formula for distance calculation.

    Parameters:
    - df: the input DataFrame containing Latitude, Longitude, MMSI, and Time.

    Returns:
    - the DataFrame with the total distance for each MMSI.
    """

    # process data adding columns of next position
    # in this way we have that each row is now a couple of consecutive points
    # on which we can compute the distance
    df_distance_all = (
        df.sort(["MMSI", "Time"])  # sort by MMSI and time
        .with_columns(
            pl.col("Latitude").shift(1).alias("Latitude_next"),
            pl.col("Longitude").shift(1).alias("Longitude_next"),
            pl.col("MMSI").shift(1).alias("MMSI_next"),
        )
        .filter(~pl.col("Latitude_next").is_null())  # drop first row
        .filter(
            pl.col("MMSI_next") == pl.col("MMSI")
        )  # drop last row for each MMSI (next point belongs to another MMSI)
    )

    # convert degrees to radians, to apply the haversine formula
    df_distance_all = df_distance_all.with_columns(
        [
            (pl.col(c) * np.pi / 180).alias(f"{c}_rad")
            for c in ["Latitude", "Longitude", "Latitude_next", "Longitude_next"]
        ]
    )

    haversine_formula = (
        2
        * 6371
        * np.arcsin(
            np.sqrt(
                np.sin((pl.col("Latitude_next_rad") - pl.col("Latitude_rad")) / 2) ** 2
                + np.cos(pl.col("Latitude_rad"))
                * np.cos(pl.col("Latitude_next_rad"))
                * np.sin((pl.col("Longitude_next_rad") - pl.col("Longitude_rad")) / 2)
                ** 2
            )
        )
    )

    # compute the distance in kilometers of each couple of point
    df_distance_all = df_distance_all.with_columns(
        haversine_formula.alias("distance_km")
    )

    # compute total distance for each MMSI
    df_distance_per_mmsi = df_distance_all.group_by(pl.col("MMSI")).agg(
        pl.sum("distance_km")
    )

    return df_distance_per_mmsi
