# Filtrer les vols car les vols du J qui sont déjà arrivés vont dans RAW avec une DATE_PHOTO à J
# Le lendemain donc à J+1, les vols de J seront aussi dans RAW mais avec une DATE_PHOTO à J+1. 
# Exemple
# INJECTION le 13/02 -> Vol terminé 9999 : flight_id= 20260213 DATE_PHOTO= 2026-02-13 dans RAW car RAW= J (=vol terminé) + J-1
# INJECTION le 14/02 -> Vol terminé 9999 : flight_id= 20260213 DATE_PHOTO= 2026-02-14 dans RAW car RAW= J (=vol terminé) + J-1
# Garder la DATE_PHOTO la plus récente si doublon dans RAW ou STAGING ?

# INJECTION le 13/02 en BACKFILL (du 08/01 au 10/01) -> Retirer tous les vols CANCELLED inclus dans plage BACKFILL dans SCHEDULED
# Filtrer avec date.now() pour ne garder que les vols en cours ?
# Donc si BACKFILL du 08/01 au 10/01, on retire tous les vols CANCELLED du 08/01 au 10/01 dans SCHEDULED car SCHEDULED doit contenir uniquement les vols à venir ou en cours, pas les vols déjà terminés ou annulés.

# VALIDER. Ne garder que les vols J-1 pour RAW et les vols J (même si déjà atterris) pour SCHEDULED. 
# Mettre des Tasksgroup

# Corriger les types : colonnes dates en datetime
# df["scheduled_departure"] = pd.to_datetime(df["scheduled_departure"])
# df["actual_departure"] = pd.to_datetime(df["actual_departure"])
# df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"])
# df["actual_arrival"] = pd.to_datetime(df["actual_arrival"])
# df["delay_minutes"] = pd.to_numeric(df["delay_minutes"], errors="coerce")

# Gestion des valeurs manquantes : imputer ou filtrer
# df["delay_minutes"] = df["delay_minutes"].fillna(0)
# df["is_cancelled"] = df["status"] == "CANCELLED"

# Suppression des doublons : si nécessaire
# df = df.drop_duplicates(subset=["flight_number", "scheduled_departure", "flight_id"]) : garder la DATE_PHOTO la plus récente si doublon dans RAW ou STAGING ?

# Suppression des colonnes inutiles
# df = df.drop(columns=["unnecessary_column1", "unnecessary_column2"])

# Colonnes dérivées : 
# df["is_delayed"] = df["delay_minutes"] > 15
# df["delay_category"] = pd.cut(df["delay_minutes"], bins=[-1, 0, 15, float("inf")], labels=["on_time", "minor_delay", "major_delay"])
# df["is_landed"] = df["actual_arrival"].notna().apply(lambda x: True if x else False)
# df["is_delay_minutes"] = (df["scheduled_departure"] - df["actual_departure"]) - (df["scheduled_arrival"] - df["actual_arrival"])

# Jointures : 
# IATA et Meteo