
import polars as pl


def read_and_fix_csv(path):
    df = pl.read_csv(
		path,
		null_values=["-"],
		schema_overrides={
			# Only override columns that need to be read as string for fixing
			"PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": pl.Float64,
			"UMIDADE RELATIVA DO AR, HORARIA (%)": pl.Float64,
			"PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": pl.Utf8,
			"RADIACAO GLOBAL, HORARIA (Kj/m²)": pl.Float64,
			"VELOCIDADE DO VENTO, HORARIA (m/s)": pl.Float64,
			"DIR DO VENTO, HORARIA (graus)": pl.Float64,
			"INSOLACAO, DIARIA (horas)": pl.Float64,
			"PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, MAXIMA DIARIA (mB)": pl.Float64,
			"PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, MINIMA DIARIA (mB)": pl.Float64,
			"RADIACAO GLOBAL, MAXIMA DIARIA (Kj/m²)": pl.Float64,
			"RADIACAO GLOBAL, MINIMA DIARIA (Kj/m²)": pl.Float64,
			"UMIDADE RELATIVA DO AR, MAXIMA DIARIA (%)": pl.Float64,
			"UMIDADE RELATIVA DO AR, MINIMA DIARIA (%)": pl.Float64,
			"VELOCIDADE DO VENTO, MAXIMA DIARIA (m/s)": pl.Float64,
			"VELOCIDADE DO VENTO, MINIMA DIARIA (m/s)": pl.Float64,
			"DIR DO VENTO, MAXIMA DIARIA (graus)": pl.Float64,
			"DIR DO VENTO, MINIMA DIARIA (graus)": pl.Float64,
			"INSOLACAO, MAXIMA DIARIA (horas)": pl.Float64,
			"INSOLACAO, MINIMA DIARIA (horas)": pl.Float64,
			"TEMPERATURA DO AR, HORARIA (°C)": pl.Float64,
			"TEMPERATURA DO PONTO DE ORVALHO, HORARIA (°C)": pl.Float64,
			"TEMPERATURA MAXIMA, DIARIA (°C)": pl.Float64,
			"TEMPERATURA MINIMA, DIARIA (°C)": pl.Float64,
			"LATITUDE": pl.Float64,
			"LONGITUDE": pl.Float64,
			"ALTITUDE": pl.Float64
		}
	)
	# Standardize column names for hour and date
	rename_map = {
		"HORA (UTC)": "HORA_UTC",
		"Hora UTC": "HORA_UTC",
		"Data": "DATA",
		"DATA": "DATA"
	}
	# Only rename columns that exist in the DataFrame
	rename_dict = {k: v for k, v in rename_map.items() if k in df.columns}
	if rename_dict:
		df = df.rename(rename_dict)

	# Replace comma with dot and cast to float for relevant columns
	columns_to_fix = [
		"UMIDADE RELATIVA DO AR, HORARIA (%)",
		"PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
		"PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
	]
	for col in columns_to_fix:
		if col in df.columns:
			df = df.with_columns([
				pl.col(col)
				.str.replace(",", ".")
				.cast(pl.Float64)
				.alias(col)
			])
	return df

df_2018 = read_and_fix_csv("dados/2018.csv")
df_2019 = read_and_fix_csv("dados/2019.csv")
df_2020 = read_and_fix_csv("dados/2020.csv")
df_2021 = read_and_fix_csv("dados/2021.csv")
#df_2022 = read_and_fix_csv("dados/2022.csv")
#df_2023 = read_and_fix_csv("dados/2023.csv")
df_2024 = read_and_fix_csv("dados/2024.csv")