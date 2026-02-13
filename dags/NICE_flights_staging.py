# Filtrer les vols car les vols du J qui sont déjà arrivés vont dans RAW avec une DATE_PHOTO à J
# Le lendemain donc à J+1, les vols de J seront aussi dans RAW mais avec une DATE_PHOTO à J+1. 
# Exemple
# INJECTION le 13/02 -> Vol terminé 9999 : flight_id= 20260213 DATE_PHOTO= 2026-02-13 dans RAW car RAW= J (=vol terminé) + J-1
# INJECTION le 14/02 -> Vol terminé 9999 : flight_id= 20260213 DATE_PHOTO= 2026-02-14 dans RAW car RAW= J (=vol terminé) + J-1
# Garder la DATE_PHOTO la plus récente si doublon ?

# INJECTION le 13/02 en BACKFILL (du 08/01 au 10/01) -> Retirer tous les vols CANCELLED inclus dans plage BACKFILL dans SCHEDULED
# Filtrer avec date_du_jour ?

# Corriger les types : colonnes dates en datetime
# df["scheduled_departure"] = pd.to_datetime(df["scheduled_departure"])
# df["actual_departure"] = pd.to_datetime(df["actual_departure"])
# df["delay_minutes"] = pd.to_numeric(df["delay_minutes"], errors="coerce")

# Gestion des valeurs manquantes : imputer ou filtrer
# df["delay_minutes"] = df["delay_minutes"].fillna(0)
# df["is_cancelled"] = df["status"] == "CANCELLED"

# Suppression des doublons : si nécessaire
# df = df.drop_duplicates(subset=["flight_number", "scheduled_departure"])

# Colonnes dérivées : 
# df["is_delayed"] = df["delay_minutes"] > 15

# Jointures : 
# Meteo et IATA