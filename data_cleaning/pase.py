# build-in libs
import logging
from datetime import datetime

# installed libs
import pandas as pd


# Logging config
def setup_logger():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def add_dataset_information(df: pd.DataFrame):
    # Add db_created_at column
    today_datetime = datetime.now()
    today_datetime = today_datetime.replace(microsecond=0)
    today_datetime = today_datetime.isoformat()
    df["db_created_at"] = today_datetime

    # Add dataset_id column
    current_dataset_id = 0
    df["db_id"] = current_dataset_id + 1
    return df


def clean_pase_data(pase_df: pd.DataFrame):
    # * read PASE
    # pase_df = pd.read_csv("src/cruces_PASE_2025_012.csv", sep=",", encoding="utf-8")
    logging.info(
        f"PASE loaded data : rows {pase_df.shape[0]} columns {pase_df.shape[1]}"
    )

    # format columns
    pase_df["Importe"] = (
        pase_df["Importe"].replace("[$,]", "", regex=True).astype(float)
    )
    pase_df["Fecha"] = pd.to_datetime(pase_df["Fecha"], format="%d/%m/%Y")

    # sort values
    pase_df = pase_df.sort_values(
        by=["Fecha", "Hora", "No.Economico"], ascending=[True, True, True]
    )

    # convert columns to correct data types
    pase_df["Tag"] = pase_df["Tag"].str.strip()
    pase_df["No.Economico"] = pase_df["No.Economico"].astype(int)
    pase_df["Fecha"] = pd.to_datetime(pase_df["Fecha"], format="%Y-%m-%d")
    pase_df["Hora"] = pd.to_datetime(pase_df["Hora"], format="mixed").dt.time
    pase_df["Caseta"] = pase_df["Caseta"].str.strip()
    pase_df["Carril"] = pase_df["Carril"].str.strip()
    pase_df["Clase"] = pase_df["Clase"].astype(int)
    pase_df["Importe"] = pase_df["Importe"].astype(float)
    pase_df["Fecha Aplicacion"] = pd.to_datetime(
        pase_df["Fecha Aplicacion"], format="%d/%m/%Y"
    )
    pase_df["Hora Aplicacion"] = pd.to_datetime(
        pase_df["Hora Aplicacion"], format="mixed"
    ).dt.time
    pase_df["Consecar"] = pase_df["Consecar"].astype(int)

    # pase_df.to_csv("db/pase_cruces.csv", index=False)
    logging.info(
        f"PASE final dataframe : rows {pase_df.shape[0]} columns {pase_df.shape[1]}"
    )
    return pase_df


if __name__ == "__main__":
    setup_logger()
    clean_pase_data()
