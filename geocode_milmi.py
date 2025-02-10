import datetime
import pandera as pa
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries
from pandera.errors import SchemaError, SchemaErrors
import click
import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
import tomli

import censusgeocode as cg
import numpy as np

from milmi import (
    setup_logging, 
    db_engine, 
    metadata_engine,
)
from metadata_audit.capture import record_metadata


class EmployerGeo(pa.DataFrameModel):
    """
    This requires installing the 'pandera[geopandas]' extension.
    """
    id: str = pa.Field(nullable=False)
    date: datetime.date = pa.Field(nullable=False)
    match_type: str = pa.Field(nullable=True)
    block_geoid: str = pa.Field(nullable=True)
    geom: GeoSeries = pa.Field(nullable=True)

    class Config:  # type: ignore
        strict = True
        coerce = True

    @pa.check("block_geoid")
    def correct_len_block(cls, block_geoid: Series[str]) -> Series[bool]: 
        return block_geoid.str.len() == 15


logger = setup_logging()

table_name = "employers_geo"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


# Every loader script starts with a pydantic model -- This is both to
# validate the clean-up process output and to ensure that fields agree
# with the metadata provided to the metadata system.
@click.command()
@click.argument("edition_date")
@click.option("-m", "--metadata_only", is_flag=True, help="Skip uploading dataset.")
def main(edition_date, metadata_only):
    if metadata_only:
        logger.info("Metadata only was selected.")

    edition = metadata["tables"][table_name]["editions"][edition_date]

    # Get rows from the table to geocode

    # Pull all rows from the providers table that don't 
    # appear in the providers_geo table
    new_employers_q = text("""
    SELECT p.id, 
        p.address, 
        p.city, 
        p.state, 
        p.zip_code,
        p.year 
    FROM milmi.employers p
    LEFT JOIN milmi.employers_geo pg
        ON p.license_number = pg.license_number
        AND p.date = pg.date
    WHERE pg.license_number IS NULL;
    """)

    # Pull everything as a backup
    all_employers_q = text("""
    SELECT p.id, 
        p.address, 
        p.city, 
        p.state, 
        p.zip_code,
        p.year 
    FROM milmi.employers p;
    """)

    try:
        with db_engine.connect() as db:
            new_providers = pd.read_sql(new_employers_q, db)

    except ProgrammingError:
        # If this is the first time running the script, no table exists so
        # geocode everything.

        with db_engine.connect() as db:
            new_providers = pd.read_sql(all_employers_q, db)
    

    new_providers = new_providers.rename(columns={"license_number": "id", "address": "street", "zip_code": "zip"})

    responses = []

    calls = (len(new_providers) // 5000) + 1

    request_dfs = np.array_split(new_providers, calls)

    for i, r in enumerate(request_dfs):
        logger.info(f"Completing request {i + 1}/{calls}")
        response = cg.addressbatch(r.drop("date", axis=1).to_dict(orient="records"))

        resp = pd.DataFrame(response)

        responses.append(
            resp
            .merge(
                r[["id", "date"]], 
                on="id"
            )
        )

    geocoded = pd.concat(responses, axis=0)
    geocoded.to_csv("tmp_geocoded_20250210.csv")


    result = (
        geocoded
        .astype({
            "statefp": pd.Int64Dtype(), 
            "countyfp": pd.Int64Dtype(), 
            "tract": pd.Int64Dtype(), 
            "block": pd.Int64Dtype(),
        })
        .astype({
            "statefp": pd.StringDtype(), 
            "countyfp": pd.StringDtype(), 
            "tract": pd.StringDtype(), 
            "block": pd.StringDtype(),
        })
        .rename(columns={"matchtype": "match_type"})
        .rename(columns={
            "statefp": "__statefp", 
            "countyfp": "__countyfp", 
            "tract": "__tract", 
            "block": "__block",
        })
        .assign(
            statefp=lambda df: df["__statefp"].str.zfill(2),
            countyfp=lambda df: df["__countyfp"].str.zfill(3),
            tract=lambda df: df["__tract"].str.zfill(6),
            block=lambda df: df["__block"].str.zfill(4),
        )
        .assign(
            geom=lambda df: gpd.points_from_xy(df["lon"], df["lat"]),
            block_geoid=lambda df: df["statefp"] + df["countyfp"] + df["tract"] + df["block"],
        )
    )[[
        "id",
        "date",
        "match_type",
        "block_geoid",
        "geom",
    ]]

    gdf = gpd.GeoDataFrame(result, geometry="geom", crs="EPSG:4326")

    gdf.to_file("tmp_geocoded_20250114.geojson")

    logger.info(f"Cleaning {table_name} was successful validating schema.")

    # Validate
    try:
        validated = EmployerGeo.validate(gdf)
        logger.info(
            f"Validating {table_name} was successful. Recording metadata."
        )
    except (SchemaError, SchemaErrors) as e:
        logger.error(f"Validating {table_name} failed.", e)
        return -1

    with metadata_engine.connect() as db:
        logger.info("Connected to metadata schema.")

        record_metadata(
            EmployerGeo,
            __file__,
            table_name,
            metadata,
            edition_date,
            gdf,
            sessionmaker(bind=db)(),
            logger,
        )

        db.commit()
        logger.info("successfully recorded metadata")

    if not metadata_only:
        with db_engine.connect() as db:
            logger.info("Metadata recorded, pushing data to db.")

            validated.to_postgis(  # type: ignore
                table_name, db, index=False, schema="childcare", if_exists="append"
            )
    else:
        logger.info("Metadata only specified, so process complete.")


if __name__ == "__main__":
    main()