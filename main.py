import argparse
import logging
import os

import polars as pl
import yaml

from utils import compute_distance


def generate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, type=str)
    parser.add_argument("--output", default=".", type=str)
    return parser


def main():

    # read settings
    with open("settings.yaml", "r") as f:
        settings = yaml.safe_load(f)

    file_cargo = settings["file_cargo"]
    file_tanker = settings["file_tanker"]

    # set logging
    logging.basicConfig(
        level=settings["logging_level"],
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # read args
    args = generate_parser().parse_args()
    data_path = args.data
    output_path = args.output

    logger.debug("Reading data from csv files from %s", data_path)
    # read cargo data
    df_cargo = pl.read_csv(os.path.join(data_path, file_cargo))

    # read tanker data
    with open(os.path.join(data_path, file_tanker), "r") as f:
        content = f.read()
    modified_content = content.replace(";", ",")  # original file has both ; and ,
    bytes_csv = bytes(modified_content, "utf-8")
    df_tanker = pl.read_csv(bytes_csv)

    # rename columns of tanker data
    df_tanker.columns = ["Time", "Ship type", "MMSI", "Latitude", "Longitude"]

    # loops through both datasets
    for name, df in {"cargo": df_cargo, "tanker": df_tanker}.items():

        logger.debug("Processing %s data", name)
        # process data
        df = df.with_columns(pl.col("Time").str.to_datetime("%d/%m/%Y %H:%M:%S"))

        # filter invalid latitude
        df_filtered = df.filter(
            (pl.col("Latitude") <= 90) & (pl.col("Latitude") >= -90)
        )

        # compute number of ships (MMSI)
        unique_MMSI_original = df["MMSI"].n_unique()  # consider all the data
        unique_MMSI_filtered = df_filtered[
            "MMSI"
        ].n_unique()  # consider only valid data
        response_str = (
            f"{unique_MMSI_original} unique MMSI for original data \n"
            + f"{unique_MMSI_filtered} unique MMSI for filtered data"
        )
        logger.info(response_str)

        # write txt
        logger.debug(
            "Saving results to " + os.path.join(output_path, f"unique_mmsi_{name}.txt")
        )
        with open(os.path.join(output_path, f"unique_mmsi_{name}.txt"), "w") as f:
            f.write(response_str)

        # compute distance
        logger.debug("Computing distance")
        df_distance_per_mmsi = compute_distance(df_filtered)

        # compute number of trajectories shorter than 1km and longer than 10km
        short_trajectories = df_distance_per_mmsi.filter(
            pl.col("distance_km") < 1
        ).shape[0]
        long_trajectories = df_distance_per_mmsi.filter(
            pl.col("distance_km") > 10
        ).shape[0]
        response_str = (
            f"{short_trajectories} trajectories shorter than 1km \n"
            + f"{long_trajectories} trajectories longer than 10km"
        )
        logger.info(response_str)

        # write txt
        logger.debug(
            "Saving results to " + os.path.join(output_path, f"lengths_{name}.txt")
        )
        with open(os.path.join(output_path, f"lengths_{name}.txt"), "w") as f:
            f.write(response_str)


if __name__ == "__main__":
    main()
