import pandas as pd
import logging


def check_nulls(
    df: pd.DataFrame,
    columns: list = None,
    threshold_percent: float = None,
) -> None:
    """
    Analyse les valeurs NULL d’un DataFrame et log les résultats.

    Arguments :
    - df (pd.DataFrame) : DataFrame à analyser.
    - columns (list, optionnel) : Colonnes à vérifier. Si None → toutes les colonnes.
    - threshold_percent (float, optionnel) : Seuil max autorisé de NULL (%) avant alerte.
    """

    if df.empty:
        logging.warning("⚠️ DataFrame vide — aucun contrôle effectué.")
        return

    columns_to_check = columns or df.columns

    logging.info("🔎 Analyse des valeurs NULL en cours...")

    null_counts = df[columns_to_check].isna().sum()
    null_percent = (df[columns_to_check].isna().mean() * 100).round(2)

    for col in columns_to_check:
        logging.info(
            f"📊 {col} | NULL: {null_counts[col]} "
            f"({null_percent[col]}%)"
        )

        if threshold_percent is not None and null_percent[col] > threshold_percent:
            logging.error(
                f"🚨 Seuil dépassé pour {col} "
                f"({null_percent[col]}% > {threshold_percent}%)"
            )

    logging.info("✅ Contrôle des valeurs NULL terminé.")
