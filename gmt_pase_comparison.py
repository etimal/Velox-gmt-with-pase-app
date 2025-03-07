import logging
from datetime import datetime

import numpy as np
import pandas as pd

# logging full config
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def comparison(viajes_unidad_df: pd.DataFrame, pase_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare GM Transport and PASE dataframes and return the result.
    """

    # * read GM Transport from db folder
    # viajes_unidad_df = pd.read_csv("db/gmt_viajes_por_unidad.csv")
    viajes_unidad_df.rename(columns={"Fecha Salida": "Fecha"}, inplace=True)
    logging.info(
        f"GM Transport data : rows {viajes_unidad_df.shape[0]} columns {viajes_unidad_df.shape[1]}"
    )

    # Fecha bigger than 2025-01-01
    datetime_parameter = "2025-01-01"
    datetime_parameter = pd.to_datetime(datetime_parameter)
    viajes_unidad_df = viajes_unidad_df[viajes_unidad_df["Fecha"] >= datetime_parameter]
    logging.info(
        f'Min Date : {viajes_unidad_df["Fecha"].min()} Max Date : {viajes_unidad_df["Fecha"].max()}'
    )

    # filtering columns
    # columns = ["Fecha", "Hora Salida", "Viaje", "Ruta", "No.Economico"]
    # viajes_unidad_df = viajes_unidad_df[columns]
    logging.info(
        f"GM Transport data : rows {viajes_unidad_df.shape[0]} columns {viajes_unidad_df.shape[1]}"
    )

    # * read PASE from db folder
    # pase_df = pd.read_csv("db/pase_cruces.csv")
    logging.info(
        f"PASE filtered data : rows {pase_df.shape[0]} columns {pase_df.shape[1]}"
    )

    # Fecha bigger than 2025-01-01
    pase_df = pase_df[pase_df["Fecha"] >= datetime_parameter]
    pase_df = pase_df[pase_df["Fecha"] <= viajes_unidad_df["Fecha"].max()]
    logging.info(
        f'Min Date : {pase_df["Fecha"].min()} Max Date : {pase_df["Fecha"].max()}'
    )

    # filtering columns
    # columns = ["Tag", "No.Economico", "Fecha", "Hora", "Caseta", "Carril", "Importe"]
    columns = [
        "Tag",
        "No.Economico",
        "Fecha",
        "Hora",
        "Caseta",
        "Carril",
        "Clase",
        "Importe",
        "Fecha Aplicacion",
        "Hora Aplicacion",
        "Consecar",
    ]
    pase_df = pase_df[columns]
    logging.info(f"PASE data : rows {pase_df.shape[0]} columns {pase_df.shape[1]}")

    # * verify columns types
    logging.info(f"GM Transport data types : {viajes_unidad_df.dtypes}")
    logging.info(f"PASE data types : {pase_df.dtypes}")

    # verify Num.Economico dtype is int
    if viajes_unidad_df["No.Economico"].dtype != "int64":
        viajes_unidad_df["No.Economico"] = viajes_unidad_df["No.Economico"].astype(int)
        logging.info(f"Num.Economico dtype is converted to int")
    if pase_df["No.Economico"].dtype != "int64":
        pase_df["No.Economico"] = pase_df["No.Economico"].astype(int)
        logging.info(f"Num.Economico dtype is converted to int")

    # * get unique No.Economico values
    num_economicos = viajes_unidad_df["No.Economico"].unique()
    # num_economicos = [2402]
    logging.info(f"No Economico values : {num_economicos}")
    logging.info(f"Amount of No Economico values : {len(num_economicos)}")

    records_df = pd.DataFrame()  # collect all dataframes for each No.Economico
    for num_econimico in num_economicos:
        # * filter by No.Economico
        target_viajes_unidad_df = viajes_unidad_df[
            viajes_unidad_df["No.Economico"] == num_econimico
        ]
        logging.info(
            f"GM Transport data : rows {target_viajes_unidad_df.shape[0]} columns {target_viajes_unidad_df.shape[1]} for No.Economico {num_econimico}"
        )

        target_pase_df = pase_df[pase_df["No.Economico"] == num_econimico]
        logging.info(
            f"PASE filtered data : rows {target_pase_df.shape[0]} columns {target_pase_df.shape[1]} for No.Economico {num_econimico}"
        )

        # * divide workflow if there are many deliveries
        viajes_por_fecha = (
            target_viajes_unidad_df.groupby(["Fecha"])["Viaje"]
            .count()
            .reset_index()
            .rename(columns={"Viaje": "total_viajes", "Fecha": "fecha"})
        )
        logging.info(f"Cantidad de fechas con viajes : {len(viajes_por_fecha)}")

        fechas_con_mas_de_un_viaje = viajes_por_fecha[
            viajes_por_fecha["total_viajes"] > 1
        ]
        logging.info(f"Fechas con mas de un viaje : {len(fechas_con_mas_de_un_viaje)}")
        pase_viajes_multiples_por_fecha = pd.DataFrame()
        if len(fechas_con_mas_de_un_viaje) > 0:
            pase_viajes_multiples_por_fecha = target_pase_df[
                target_pase_df["Fecha"].isin(fechas_con_mas_de_un_viaje["fecha"].values)
            ].copy()
            pase_viajes_multiples_por_fecha.reset_index(drop=True, inplace=True)
            pase_viajes_multiples_por_fecha["Viaje"] = None
            pase_viajes_multiples_por_fecha["fecha_salida_ma_min"] = None
            pase_viajes_multiples_por_fecha["Fecha y Hora de Salida"] = None

        fechas_unicos = viajes_por_fecha[viajes_por_fecha["total_viajes"] == 1]
        logging.info(f"Fechas con un solo viaje : {len(fechas_unicos)}")

        pase_viajes_unicos_por_fecha = pd.DataFrame()
        if len(fechas_unicos) > 0:
            pase_viajes_unicos_por_fecha = target_pase_df[
                target_pase_df["Fecha"].isin(fechas_unicos["fecha"].values)
            ].copy()
            pase_viajes_unicos_por_fecha.reset_index(drop=True, inplace=True)

            # * append GMT vlues to all PASE by Fecha
            viajes_unidad_values = target_viajes_unidad_df.groupby(["Fecha"])[
                "Viaje"
            ].min()
            viajes_unidad_values = viajes_unidad_values.reset_index().rename(
                columns={"Viaje": "Viaje"}
            )
            pase_viajes_unicos_por_fecha = pase_viajes_unicos_por_fecha.merge(
                viajes_unidad_values, on="Fecha", how="left"
            )

            # group by Viaje and get max Fecha y Hora de Salida
            min_datetime_by_ship = target_viajes_unidad_df.groupby("Fecha").agg(
                {"fecha_salida_ma_min": "max"}
            )
            max_datetime_by_ship = target_viajes_unidad_df.groupby("Fecha").agg(
                {"Fecha y Hora de Salida": "max"}
            )
            # add column to original df
            pase_viajes_unicos_por_fecha["fecha_salida_ma_min"] = (
                pase_viajes_unicos_por_fecha["Fecha"].map(
                    min_datetime_by_ship["fecha_salida_ma_min"]
                )
            )
            pase_viajes_unicos_por_fecha["Fecha y Hora de Salida"] = (
                pase_viajes_unicos_por_fecha["Fecha"].map(
                    max_datetime_by_ship["Fecha y Hora de Salida"]
                )
            )

        fechas_sin_viaje_asignado = target_pase_df[
            ~target_pase_df["Fecha"].isin(viajes_por_fecha["fecha"].values)
        ]
        if len(fechas_sin_viaje_asignado) > 0:
            logging.info(
                f"Fechas sin viaje asignado (valores desde PASE) : {fechas_sin_viaje_asignado['Fecha'].nunique()}"
            )

        # * assign Viaje to PASE for fechas with more than one Viaje
        if len(fechas_con_mas_de_un_viaje) > 0:
            hora_de_viajes = target_viajes_unidad_df[
                target_viajes_unidad_df["Fecha"].isin(
                    fechas_con_mas_de_un_viaje["fecha"].values
                )
            ].copy()
            hora_de_viajes = hora_de_viajes.groupby(
                ["Fecha", "Viaje", "fecha_salida_ma_min", "Fecha y Hora de Salida"]
            )["Hora Salida"].min()
            hora_de_viajes = hora_de_viajes.reset_index().rename(
                columns={"Hora Salida": "hora_min"}
            )
            hora_de_viajes.sort_values(
                by=["Fecha", "hora_min"], ascending=[True, True], inplace=True
            )

            # format hora_min as time object
            hora_de_viajes["hora_min"] = pd.to_datetime(
                hora_de_viajes["hora_min"], format="%H:%M:%S"
            ).dt.time

            hora_de_viajes["hora_max"] = hora_de_viajes["hora_min"].shift(-1)

            # add rank for Viaje by Fecha
            hora_de_viajes["fecha_rank"] = (
                hora_de_viajes.groupby("Fecha")["Viaje"].cumcount() + 1
            )
            hora_de_viajes["total_viajes"] = hora_de_viajes.groupby("Fecha")[
                "Viaje"
            ].transform("count")

            # remove last hour value of each date
            conditions = [
                hora_de_viajes["fecha_rank"] == hora_de_viajes["total_viajes"]
            ]
            choices = [None]
            hora_de_viajes["hora_max"] = np.select(
                conditions, choices, default=hora_de_viajes["hora_max"]
            )

            for fecha in fechas_con_mas_de_un_viaje["fecha"].values:
                target_horas_fecha = hora_de_viajes[hora_de_viajes["Fecha"] == fecha]
                logging.info(
                    f"add Viaje to PASE for Fecha : amount of Viajes is {target_horas_fecha.shape[0]}"
                )

                for row_index, row in target_horas_fecha.iterrows():
                    if row["hora_max"] != None:
                        conditions = [
                            (pase_viajes_multiples_por_fecha["Hora"] >= row["hora_min"])
                            & (
                                pase_viajes_multiples_por_fecha["Hora"]
                                < row["hora_max"]
                            )
                            & (pase_viajes_multiples_por_fecha["Fecha"] == row["Fecha"])
                        ]
                        choices = [row["Viaje"]]
                        pase_viajes_multiples_por_fecha["Viaje"] = np.select(
                            conditions,
                            choices,
                            default=pase_viajes_multiples_por_fecha["Viaje"],
                        )
                        choices = [row["fecha_salida_ma_min"]]
                        pase_viajes_multiples_por_fecha["fecha_salida_ma_min"] = (
                            np.select(
                                conditions,
                                choices,
                                default=pase_viajes_multiples_por_fecha[
                                    "fecha_salida_ma_min"
                                ],
                            )
                        )
                        choices = [row["Fecha y Hora de Salida"]]
                        pase_viajes_multiples_por_fecha["Fecha y Hora de Salida"] = (
                            np.select(
                                conditions,
                                choices,
                                default=pase_viajes_multiples_por_fecha[
                                    "Fecha y Hora de Salida"
                                ],
                            )
                        )

                    else:
                        conditions = [
                            (pase_viajes_multiples_por_fecha["Hora"] >= row["hora_min"])
                            & (pase_viajes_multiples_por_fecha["Fecha"] == row["Fecha"])
                        ]
                        choices = [row["Viaje"]]
                        pase_viajes_multiples_por_fecha["Viaje"] = np.select(
                            conditions,
                            choices,
                            default=pase_viajes_multiples_por_fecha["Viaje"],
                        )
                        choices = [row["fecha_salida_ma_min"]]
                        pase_viajes_multiples_por_fecha["fecha_salida_ma_min"] = (
                            np.select(
                                conditions,
                                choices,
                                default=pase_viajes_multiples_por_fecha[
                                    "fecha_salida_ma_min"
                                ],
                            )
                        )
                        choices = [row["Fecha y Hora de Salida"]]
                        pase_viajes_multiples_por_fecha["Fecha y Hora de Salida"] = (
                            np.select(
                                conditions,
                                choices,
                                default=pase_viajes_multiples_por_fecha[
                                    "Fecha y Hora de Salida"
                                ],
                            )
                        )
            logging.info(f"Addition of Viaje values to PASE is completed")

        # * Append PASE Results
        pase_con_num_viaje = pd.DataFrame()

        if len(pase_viajes_multiples_por_fecha) > 0:
            pase_con_num_viaje = pase_viajes_multiples_por_fecha
        if len(pase_viajes_unicos_por_fecha) > 0:
            if pase_con_num_viaje.empty:
                pase_con_num_viaje = pase_viajes_unicos_por_fecha
            else:
                pase_con_num_viaje = pd.concat(
                    [pase_con_num_viaje, pase_viajes_unicos_por_fecha]
                )
        if len(fechas_sin_viaje_asignado) > 0:
            if pase_con_num_viaje.empty:
                pase_con_num_viaje = fechas_sin_viaje_asignado
            else:
                pase_con_num_viaje = pd.concat(
                    [pase_con_num_viaje, fechas_sin_viaje_asignado]
                )

        # verify items amount
        if len(pase_con_num_viaje) == len(target_pase_df):
            logging.info(
                f"items comparison with PASE is correct: {len(target_pase_df)}"
            )
        else:
            logging.error(
                f"items comparison with PASE is incorrect: {len(pase_con_num_viaje)} vs {len(target_pase_df)}"
            )

        # sort values
        pase_con_num_viaje.sort_values(
            by=["Fecha", "Hora"], ascending=[True, True], inplace=True
        )

        """ start datetime verification """
        # * Convert columns to datetime
        # create GMT datetime column for fecha_salida_ma_min
        pase_con_num_viaje["gmt_datetime"] = pd.to_datetime(
            pase_con_num_viaje["Fecha y Hora de Salida"]
        )
        # create pase datetime  column
        # pase_con_num_viaje["pase_datetime"] = pd.to_datetime(
        #    pase_con_num_viaje["Fecha"] + " " + pase_con_num_viaje["Hora"]
        # )
        # concat Timestamp' and 'datetime.time' objects
        pase_con_num_viaje["pase_datetime"] = pd.to_datetime(
            pase_con_num_viaje["Fecha"].astype(str)
            + " "
            + pase_con_num_viaje["Hora"].astype(str)
        )

        # * Comparison rules
        # verify if pase datetime is smaller than GMT datetime
        pase_con_num_viaje["pase_vs_gmt"] = (
            pase_con_num_viaje["pase_datetime"] < pase_con_num_viaje["gmt_datetime"]
        )

        # * Apply comparison rules
        # remove viaje values if pase datetime is smaller than GMT datetime
        pase_con_num_viaje["Viaje"] = np.where(
            pase_con_num_viaje["pase_vs_gmt"] == True,
            None,
            pase_con_num_viaje["Viaje"],
        )
        # remove Fecha y Hora de Salida values if pase datetime is smaller than GMT datetime
        pase_con_num_viaje["Fecha y Hora de Salida"] = np.where(
            pase_con_num_viaje["pase_vs_gmt"] == True,
            None,
            pase_con_num_viaje["Fecha y Hora de Salida"],
        )

        # convert to datetime
        pase_con_num_viaje["Fecha y Hora de Salida"] = pd.to_datetime(
            pase_con_num_viaje["Fecha y Hora de Salida"]
        )

        #! Complete Viaje values for PASE based on Viajes Unidad Fecha
        viajes_con_inicio_y_fin = target_viajes_unidad_df.groupby(["Viaje"])[
            "Fecha y Hora de Salida"
        ].min()
        viajes_con_inicio_y_fin = viajes_con_inicio_y_fin.reset_index().rename(
            columns={"Fecha y Hora de Salida": "FechaInicio"}
        )
        viajes_con_inicio_y_fin.sort_values(
            by=["FechaInicio"], ascending=[True], inplace=True
        )
        viajes_con_inicio_y_fin["FechaFin"] = viajes_con_inicio_y_fin[
            "FechaInicio"
        ].shift(-1)

        for viaje_index, viaje_row in viajes_con_inicio_y_fin.iterrows():
            if viaje_row["FechaFin"] == None or pd.isna(viaje_row["FechaFin"]) == True:
                conditions = [
                    (pase_con_num_viaje["pase_datetime"] >= viaje_row["FechaInicio"])
                    & (pase_con_num_viaje["Viaje"].isna())
                ]
                choices = [viaje_row["Viaje"]]
                pase_con_num_viaje["Viaje"] = np.select(
                    conditions, choices, default=pase_con_num_viaje["Viaje"]
                )
            else:
                conditions = [
                    (pase_con_num_viaje["pase_datetime"] >= viaje_row["FechaInicio"])
                    & (pase_con_num_viaje["pase_datetime"] < viaje_row["FechaFin"])
                    & (pase_con_num_viaje["Viaje"].isna())
                ]
                choices = [viaje_row["Viaje"]]
                pase_con_num_viaje["Viaje"] = np.select(
                    conditions, choices, default=pase_con_num_viaje["Viaje"]
                )

        # * Shift viaje value if nombre de caseta is "LINCOLN" ############ Only for LINCOLN ############
        pase_con_num_viaje["viaje_shift"] = pase_con_num_viaje["Viaje"].shift(-1)
        conditions = [pase_con_num_viaje["Caseta"] == "LINCOLN"]
        choices = [pase_con_num_viaje["viaje_shift"]]
        pase_con_num_viaje["Viaje"] = np.select(
            conditions, choices, default=pase_con_num_viaje["Viaje"]
        )
        pase_con_num_viaje.drop(columns=["viaje_shift"], inplace=True)

        pase_con_num_viaje["fecha_salida_ma_min_shift"] = pase_con_num_viaje[
            "Fecha y Hora de Salida"
        ].shift(-1)
        conditions = [pase_con_num_viaje["Caseta"] == "LINCOLN"]
        choices = [pase_con_num_viaje["fecha_salida_ma_min_shift"]]
        pase_con_num_viaje["Fecha y Hora de Salida"] = np.select(
            conditions, choices, default=pase_con_num_viaje["Fecha y Hora de Salida"]
        )
        pase_con_num_viaje.drop(columns=["fecha_salida_ma_min_shift"], inplace=True)

        # * Complete fecha_salida based on previous value
        pase_con_num_viaje["fecha_salida_fill"] = pase_con_num_viaje[
            "Fecha y Hora de Salida"
        ].ffill()

        pase_con_num_viaje["Fecha y Hora de Salida"] = pase_con_num_viaje[
            "fecha_salida_fill"
        ]
        pase_con_num_viaje.drop(columns=["fecha_salida_fill"], inplace=True)

        # * Append GMT Rutas to PASE
        gmt_data_to_append = target_viajes_unidad_df[
            ["Viaje", "Ruta", "Fecha y Hora de Salida"]
        ].copy()

        # remove duplicates
        gmt_data_to_append.drop_duplicates(inplace=True)

        # convert to datetime
        pase_con_num_viaje["Fecha y Hora de Salida"] = pd.to_datetime(
            pase_con_num_viaje["Fecha y Hora de Salida"]
        )

        pase_con_num_viaje = pase_con_num_viaje.merge(
            gmt_data_to_append,
            on=["Fecha y Hora de Salida", "Viaje"],
            how="left",
            indicator=True,
        )

        # * add VELOX to No.Economico, if it begins with 2
        pase_con_num_viaje["No.Economico"] = pase_con_num_viaje["No.Economico"].astype(
            str
        )
        pase_con_num_viaje["No.Economico"] = np.where(
            pase_con_num_viaje["No.Economico"].astype(str).str.startswith("2"),
            "VELOX " + pase_con_num_viaje["No.Economico"].astype(str),
            pase_con_num_viaje["No.Economico"],
        )

        # remove columns
        pase_con_num_viaje.drop(
            columns=[
                "fecha_salida_ma_min",
                "_merge",
            ],
            inplace=True,
        )

        # reorder columns
        columns_sorting = [
            "Viaje",
            "Tag",
            "No.Economico",
            "Fecha",
            "Hora",
            "Caseta",
            "Carril",
            "Clase",
            "Importe",
            "Fecha Aplicacion",
            "Hora Aplicacion",
            "Consecar",
            "Fecha y Hora de Salida",
            "gmt_datetime",
            "pase_datetime",
            "Ruta",
            "pase_vs_gmt",
        ]
        pase_con_num_viaje = pase_con_num_viaje[columns_sorting]

        # verify items amount
        logging.info(
            f"Addition of GMT values to PASE is completed for no economico: {num_econimico}"
        )
        if len(pase_con_num_viaje) == len(target_pase_df):
            logging.info(
                f"GMT items comparison with PASE is correct: {len(target_pase_df)}"
            )
        else:
            logging.error(
                f"GMT items comparison with PASE is incorrect: {len(pase_con_num_viaje)} vs {len(target_pase_df)}"
            )

        # * Append to records_df
        if records_df.empty:
            records_df = pase_con_num_viaje
        else:
            records_df = pd.concat([records_df, pase_con_num_viaje])
        logging.info(
            f"current records df : rows {records_df.shape[0]} columns {records_df.shape[1]}"
        )

    # clean records_df columns
    records_df = records_df.drop(columns=["pase_vs_gmt", "gmt_datetime"])

    # * save results
    logging.info(
        f"Final Records df : rows {records_df.shape[0]} columns {records_df.shape[1]}"
    )
    logging.info(f"End of the process")
    return records_df


if __name__ == "__main__":
    comparison()
