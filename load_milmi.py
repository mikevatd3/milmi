from datetime import date
import click
import pandas as pd
import pandera as pa
from pandera.typing import Series
from pandera.errors import SchemaError, SchemaErrors
import tomli

from milmi import setup_logging, db_engine, metadata_engine
from metadata_audit.capture import record_metadata
from sqlalchemy.orm import sessionmaker

logger = setup_logging()

table_name = "milmi_employers"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


class MILMIEmployers(pa.DataFrameModel):
    """
    The employer data from the Michigan Labor Market Information Page 
    """
    id: int = pa.Field()
    name: str = pa.Field()
    address: str = pa.Field()
    city: str = pa.Field()
    state: str = pa.Field()
    zip_code: str = pa.Field()
    zip_code_2: str = pa.Field()
    business_description: str = pa.Field()
    naics: str = pa.Field(coerce=True)
    employment_size: str = pa.Field()
    employment_size_range: str = pa.Field(nullable=True)
    employment_size_value: pd.Int64Dtype = pa.Field(coerce=True, nullable=True)
    year_established: pd.Int64Dtype = pa.Field(coerce=True, nullable=True)
    county: str = pa.Field(nullable=False)
    year: date = pa.Field()
    
    @pa.check("name")
    def no_over_filling__name(cls, name: Series[str]) -> bool:
        """
        The 100 is kind of arbitrary, but there shouldn't be more than a 
        couple missing business names.
        """
        return 0 < (name == "NAME NOT AVAILABLE").sum() < 100

    @pa.check("address")
    def no_over_filling__address(cls, address: Series[str]) -> bool:
        """
        More are missing addresses than names. About 2% missing from the 
        2025 file. Take this into account during geocoding!
        """
        return (address == "ADDRESS NOT AVAILABLE").sum() < (0.03 * len(address))

    @pa.check("employment_size")
    def no_over_filling__emp_size(cls, employment_size: Series[str]) -> bool:
        """
        Similar to address, but many more are missing ~10%
        """
        return (employment_size == "SIZE NOT AVAILABLE").sum() < (0.10 * len(employment_size))

    @pa.check("naics")
    def naics_6_digit(cls, naics: Series[str]) -> Series[bool]:
        """
        We're pulling the last two digits off of these (both zeros, not 
        standard naics, so let's make sure that in the end we end up with 
        the correct length code.
        """
        return naics.str.len() == 6


@click.command()
@click.argument("edition_date")
@click.option("-m", "--metadata_only", is_flag=True, help="Skip uploading dataset.")
def main(edition_date, metadata_only):
    if metadata_only:
        logger.info("Metadata only was selected.")

    edition = metadata["tables"][table_name]["editions"][edition_date]

    dated = date.fromisoformat(edition_date)

    county_dfs = []

    for county in [
        "Wayne",
        "Oakland",
        "Macomb",
        "Livingston",
        "Washtenaw",
        "St Clair",
        "Calhoun",
        "Monroe",
    ]:

        logger.info(f"Loading {county} worksheet.")
        county_df = (
            pd.read_excel(edition["raw_path"], sheet_name=county)
            .rename(columns={
                'Name': '__name',
                'Address': '__address',
                'City': 'city',
                'County': 'county',
                'State': 'state',
                'ZipCode': '__zip_code',
                'ZipC+': '__zip_code_2',
                'Business Description': 'business_description',
                'NAICS': '__naics',
                'Employment Size': '__employment_size',
                'Employment Size Range': 'employment_size_range',
                'Employment Size Value': 'employment_size_value',
                'Year Established': 'year_established',
            })
            .assign(
                name = lambda df: df["__name"].fillna("NAME NOT AVAILABLE"),
                address = lambda df: df["__address"].fillna("ADDRESS NOT AVAILABLE"),
                employment_size = lambda df: df["__employment_size"].fillna("SIZE NOT AVAILABLE"),
                naics = lambda df: df["__naics"].astype("str").str.slice(0, 6),
                zip_code = lambda df: df["__zip_code"].astype(pd.Int64Dtype()).astype(str),
                zip_code_2 = lambda df: df["__zip_code_2"].astype(pd.Int64Dtype()).astype(str),
                year = dated
            )
            .drop(
                [
                    "__name",
                    "__address",
                    "__employment_size",
                    "__naics", 
                    "__zip_code", 
                    "__zip_code_2"
                ], 
                axis=1
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

        county_dfs.append(county_df)
    
    result = pd.concat(county_dfs)

    logger.info(result["employment_size"].value_counts())

    logger.info(f"Cleaning {table_name} was successful validating schema.")

    # Validate
    try:
        validated = MILMIEmployers.validate(result)
        logger.info(
            f"Validating {table_name} was successful. Recording metadata."
        )
    except (SchemaError, SchemaErrors) as e:
        logger.error(f"Validating {table_name} failed.", e)
        return -1 # Don't continue if you can't validate!

    with metadata_engine.connect() as db:
        logger.info("Connected to metadata schema.")

        record_metadata(
            MILMIEmployers,
            __file__,
            table_name,
            metadata,
            edition_date,
            result,
            sessionmaker(bind=db)(),
            logger,
        )

        db.commit()
        logger.info("successfully recorded metadata")

    if not metadata_only:
        with db_engine.connect() as db:
            logger.info("Metadata recorded, pushing data to db.")

            validated.to_sql(  # type: ignore
                table_name, db, index=False, schema=metadata["schema"], if_exists="append"
            )
    else:
        logger.info("Metadata only specified, so process complete.")

if __name__ == "__main__":
    main()
