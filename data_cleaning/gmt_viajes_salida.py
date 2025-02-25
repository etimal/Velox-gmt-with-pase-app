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


def clean_gmt_data(viajes_df: pd.DataFrame):
    # * read GM Transport
    # viajes_df = pd.read_excel("src/Viajes_por_unidad_2025_012.xlsx")
    logging.info(
        f"GM Transport loaded data : rows {viajes_df.shape[0]} columns {viajes_df.shape[1]}"
    )

    # remove spaces from column names
    viajes_df.columns = viajes_df.columns.str.strip()

    # rename columns names
    columns_to_rename = {"Viaje Docto.": "Viaje", "Tractocamión": "Unidad"}
    viajes_df = viajes_df.rename(columns=columns_to_rename)

    # * set datetime target column
    target_datetime_column = "Fecha y Hora de Salida"

    # remove rows with all values as NaN
    viajes_df = viajes_df.dropna(how="all")
    # remove rows if Fecha y Hora de Salida is NaN
    viajes_df = viajes_df.dropna(subset=[target_datetime_column])

    # convert Fecha y Hora de Salida to datetime
    viajes_df[target_datetime_column] = pd.to_datetime(
        viajes_df[target_datetime_column], format="%d/%m/%Y %H:%M:%S"
    )

    # sort by Fecha y Hora de Salida and Unidad
    viajes_df = viajes_df.sort_values(
        by=[target_datetime_column, "Unidad"], ascending=[True, True]
    )

    """ Following process is needed to get just Velox shipments """
    # get Viajes where Unidad contains "VELOX"
    numero_ma_part1 = viajes_df[viajes_df["Unidad"].str.contains("VELOX")]

    # get Viajes where Unidad is 3502
    numero_ma_part2 = viajes_df[viajes_df["Unidad"] == "3502"]

    # concatenate both dataframes
    numero_ma = pd.concat([numero_ma_part1, numero_ma_part2])
    numero_ma = numero_ma.drop_duplicates()
    numero_ma = numero_ma["Viaje"].unique()

    # filter rows with target numero_ma
    viajes_df = viajes_df[viajes_df["Viaje"].isin(numero_ma)]

    # group by Viaje and get min and max Fecha y Hora de Salida
    last_datetime_by_ship = viajes_df.groupby("Viaje").agg(
        {target_datetime_column: ["min", "max"]}
    )

    if len(last_datetime_by_ship) != len(numero_ma):
        logging.error(
            f"Amount of unique Viajes {len(numero_ma)} is different from last_datetime_by_ship {len(last_datetime_by_ship)}"
        )

    # add new columns to original df
    viajes_df["fecha_salida_ma_min"] = viajes_df["Viaje"].map(
        last_datetime_by_ship[target_datetime_column]["min"]
    )
    viajes_df["fecha_salida_ma_max"] = viajes_df["Viaje"].map(
        last_datetime_by_ship[target_datetime_column]["max"]
    )

    # filter Unidad with Velox ships
    viajes_part1 = viajes_df[viajes_df["Unidad"].str.contains("VELOX")]
    viajes_part2 = viajes_df[viajes_df["Unidad"] == "3502"]
    viajes_df = pd.concat([viajes_part1, viajes_part2])

    """ Continue with extracting and cleaning general data """
    # extract number from column 'Unidad'
    viajes_df["No.Economico"] = viajes_df["Unidad"].str.extract(r"(\d+)")

    # split Hora from Fecha Salida verifying follwing format '00:00:00'
    viajes_df["Hora Salida"] = viajes_df[target_datetime_column].dt.strftime("%H:%M:%S")
    # viajes_df["Hora Salida"] = pd.to_datetime(
    # viajes_df["Hora Salida"], format="%H:%M:%S"
    # ).dt.time

    # remove hours from Fecha Salida
    viajes_df["Fecha Salida"] = viajes_df[target_datetime_column].dt.strftime(
        "%d/%m/%Y"
    )
    viajes_df["Fecha Salida"] = pd.to_datetime(
        viajes_df["Fecha Salida"], format="%d/%m/%Y"
    )

    # reorder column position
    col_position_by_name = ["Fecha Salida", "Hora Salida"]
    viajes_df = viajes_df[
        col_position_by_name
        + [col for col in viajes_df.columns if col not in col_position_by_name]
    ]

    # convert columns to correct data types
    viajes_df["Fecha y Hora de Salida"] = pd.to_datetime(
        viajes_df["Fecha y Hora de Salida"], format="%d/%m/%Y %H:%M:%S"
    )
    viajes_df["fecha_salida_ma_min"] = pd.to_datetime(
        viajes_df["fecha_salida_ma_min"], format="%Y-%m-%d %H:%M:%S"
    )
    viajes_df["fecha_salida_ma_max"] = pd.to_datetime(
        viajes_df["fecha_salida_ma_max"], format="%Y-%m-%d %H:%M:%S"
    )
    viajes_df["No.Economico"] = viajes_df["No.Economico"].astype(int)

    # viajes_df = add_dataset_information(viajes_df)
    # viajes_df.to_csv("db/gmt_viajes_por_unidad.csv", index=False)
    logging.info(
        f"GM Transport dataframe : rows {viajes_df.shape[0]} columns {viajes_df.shape[1]}"
    )
    return viajes_df


if __name__ == "__main__":
    setup_logger()
    clean_gmt_data()
