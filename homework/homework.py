"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


"""
def clean_campaign_data():

En esta tarea se le pide que limpie los datos de una campaña de
marketing realizada por un banco, la cual tiene como fin la
recolección de datos de clientes para ofrecerls un préstamo.

La información recolectada se encuentra en la carpeta
files/input/ en varios archivos csv.zip comprimidos para ahorrar
espacio en disco.

Usted debe procesar directamente los archivos comprimidos (sin
descomprimirlos). Se desea partir la data en tres archivos csv
(sin comprimir): client.csv, campaign.csv y economics.csv.
Cada archivo debe tener las columnas indicadas.

Los tres archivos generados se almacenarán en la carpeta files/output/.

client.csv:
- client_id
- age
- job: se debe cambiar el "." por "" y el "-" por "_"
- marital
- education: se debe cambiar "." por "_" y "unknown" por pd.NA
- credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
- mortage: convertir a "yes" a 1 y cualquier otro valor a 0

campaign.csv:
- client_id
- number_contacts
- contact_duration
- previous_campaing_contacts
- previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
- campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
- last_contact_day: crear un valor con el formato "YYYY-MM-DD",
    combinando los campos "day" y "month" con el año 2022.

economics.csv:
- client_id
- const_price_idx
- eurobor_three_months



return


if __name__ == "__main__":
clean_campaign_data()


"""

import os
import pandas as pd
import zipfile
from datetime import datetime

def clean_campaign_data():

    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    zip_files = [file for file in os.listdir(input_dir) if file.endswith(".zip")]

    all_client_data = []
    all_campaign_data = []
    all_economics_data = []

    for zip_file in zip_files:
        zip_path = os.path.join(input_dir, zip_file)
        with zipfile.ZipFile(zip_path, 'r') as z:
            for csv_name in z.namelist():
                if csv_name.endswith(".csv"):
                    with z.open(csv_name) as csv_file:
                        df = pd.read_csv(csv_file)
                        print(f"Procesando archivo: {csv_name}")
                        print(f"Columnas detectadas: {df.columns.tolist()}")

                        # Procesar datos para client.csv
                        client_columns = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
                        if set(client_columns).issubset(df.columns):
                            client_df = df[client_columns].copy()
                            client_df["job"] = client_df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
                            client_df["education"] = client_df["education"].replace({
                                "unknown": pd.NA,
                                "university.degree": "university_degree",
                                "high.school": "high_school",
                                "basic.9y": "basic_9y",
                                "professional.course": "professional_course",
                                "basic.4y": "basic_4y",
                                "basic.6y": "basic_6y",
                                "illiterate": "illiterate"
                            })
                            client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
                            client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
                            all_client_data.append(client_df)
                        else:
                            missing = set(client_columns) - set(df.columns)
                            print(f"No se encontraron las columnas necesarias para client.csv: {missing}")

                        # Procesar datos para campaign.csv
                        campaign_columns = ["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]
                        if set(campaign_columns).issubset(df.columns):
                            campaign_df = df[campaign_columns].copy()
                            campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
                            campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
                            campaign_df["last_contact_date"] = campaign_df.apply(
                                lambda row: datetime.strptime(f"2022-{row['month']}-{int(row['day']):02d}", "%Y-%b-%d").strftime("%Y-%m-%d")
                                if pd.notnull(row["month"]) and pd.notnull(row["day"]) else None, axis=1
                            )
                            campaign_df.drop(columns=["day", "month"], inplace=True)
                            all_campaign_data.append(campaign_df)
                        else:
                            missing = set(campaign_columns) - set(df.columns)
                            print(f"No se encontraron las columnas necesarias para campaign.csv: {missing}")

                        # Procesar datos para economics.csv
                        economics_columns = ["client_id", "cons_price_idx", "euribor_three_months"]
                        if set(economics_columns).issubset(df.columns):
                            economics_df = df[economics_columns].copy()
                            all_economics_data.append(economics_df)
                        else:
                            missing = set(economics_columns) - set(df.columns)
                            print(f"No se encontraron las columnas necesarias para economics.csv: {missing}")

    # Guardar archivos CSV procesados
    if all_client_data:
        pd.concat(all_client_data, ignore_index=True).to_csv(os.path.join(output_dir, "client.csv"), index=False)
    else:
        print("No se generaron datos para client.csv")

    if all_campaign_data:
        pd.concat(all_campaign_data, ignore_index=True).to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    else:
        print("No se generaron datos para campaign.csv")

    if all_economics_data:
        pd.concat(all_economics_data, ignore_index=True).to_csv(os.path.join(output_dir, "economics.csv"), index=False)
    else:
        print("No se generaron datos para economics.csv")

if __name__ == "__main__":
    clean_campaign_data()


