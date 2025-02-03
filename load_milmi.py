from datetime import date
import click
import pandas as pd
import pandera as pa
from pandera.errors import SchemaError, SchemaErrors
import tomli

from milmi import setup_logging, db_engine, metadata_engine
from metadata_audit.capture import record_metadata
from sqlalchemy.orm import sessionmaker

logger = setup_logging()

table_name = "milmi"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


class MILMIEmployers(pa.DataFrameModel):
    """
    The employer data from the Michigan Labor Market Information Page 
    """

    id: str = pa.Field()
    name: str = pa.Field()
    address: str = pa.Field()
    phone: str = pa.Field()
    website: str = pa.Field()
    business_description: str = pa.Field()
    num_employees: str = pa.Field()
    lat: str = pa.Field()
    lon: str = pa.Field()
    geometry: str = pa.Field()
    county : str = pa.Field()
    start: date = pa.Field()
    end: date = pa.Field()


@click.command()
@click.argument("edition_date")
@click.option("-m", "--metadata_only", is_flag=True, help="Skip uploading dataset.")
def main(edition_date, metadata_only):
    if metadata_only:
        logger.info("Metadata only was selected.")

    edition = metadata["tables"][table_name]["editions"][edition_date]

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
