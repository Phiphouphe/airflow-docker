## ADR-001 — Service Webhook pour l'auto-healing

**Statut** : Accepté

**Contexte**  
En production, des services critiques comme FastAPI et Airflow peuvent tomber de manière inattendue. Sans mécanisme de remédiation automatique, une intervention manuelle est nécessaire pour redémarrer les containers — ce qui peut prendre du temps et impacter la disponibilité du système.

**Décision**  
Déployer un service `webhook` sur EC2 qui écoute les alertes Prometheus/Alertmanager et redémarre automatiquement les containers Docker concernés.

**Implémentation**  
Le service webhook expose un endpoint POST `/webhook` sur le port 9099. Il est configuré dans Alertmanager comme receiver :

```yaml
webhook_configs:
  - url: 'http://webhook:9099/webhook'
    send_resolved: true
```

Mapping alertes → containers :

| Alerte | Container redémarré |
|---|---|
| `AirflowDown` | `airflow-docker-airflow-apiserver-1` |
| `FastAPIDown` | `flight_api` |
| `DAGFailed` | aucun restart (warning uniquement) |

Le webhook ne déclenche aucune action pour 'DAGFailed' : la résilience sur les échecs de tâche est assurée en amont par les retries Airflow (configuration retries/retry_delay au niveau des tâches), qui expliquent les résolutions d'alerte observées sans intervention du webhook.

Le container webhook a accès au Docker socket de l'hôte (`/var/run/docker.sock`) pour piloter Docker directement.

**Justification**
- Auto-remédiation sans intervention manuelle — le système se répare tout seul
- Couplé à Alertmanager : l'alerte déclenche simultanément une notification email ET le restart automatique
- Solution légère (FastAPI + subprocess), sans dépendance externe
- `send_resolved: true` — notification envoyée aussi à la résolution, confirmant que le restart a fonctionné

**Validation en production**  
Une alerte `AirflowDown` s'est déclenchée en production. Alertmanager a simultanément envoyé un email de notification et notifié le webhook, qui a redémarré automatiquement le container Airflow sans intervention manuelle.

**Alternatives considérées**
- Intervention manuelle : temps de réaction variable, indisponibilité plus longue
- Watchtower : surveille les images Docker Hub, pas adapté au redémarrage sur alerte métier
- AWS Auto Recovery : nécessite une configuration EC2 dédiée, plus complexe

**Note**  
Ce service n'est pas un outil de déploiement continu (CD). Le déploiement en production est géré séparément via le pipeline GitHub Actions CD (workflow_dispatch).


## ADR-002 — Critère de sélection ML : F1-score plutôt qu'accuracy
**Statut** : Accepté

**Contexte**  
Le pipeline ML entraîne 4 modèles de classification à chaque run et sélectionne automatiquement le meilleur. Le critère initial était l'accuracy. Or le dataset est déséquilibré (47% de vols à l'heure, 53% en retard mineur ou majeur), ce qui rend l'accuracy trompeuse : un modèle prédisant "toujours à l'heure" obtiendrait déjà ~47% d'accuracy sans rien apprendre.

**Décision**  
Remplacer l'accuracy par le F1-score comme critère de sélection du meilleur modèle, avec un seuil minimum de 0.40 pour passer en Staging.

**Implémentation**  
Dans `MLTrainTask.py` :
```python
best = max(results, key=lambda x: x["f1"])
```
Métriques loggées dans MLflow à chaque run : `test_accuracy`, `test_recall`, `test_precision`, `test_f1`.

Seuil minimum configuré dans `ML_training_raw_flights.py` :
```python
staging_threshold=0.40
```

**Justification**
- Le F1-score équilibre recall et precision — il pénalise les faux négatifs (retards non détectés) et les faux positifs (fausses alertes)
- Un faux négatif (dire "à l'heure" quand le vol est en retard) est plus coûteux métier pour Laura qu'un faux positif
- L'accuracy favorisait LogisticRegression (0.79) alors que XGBoost et GradientBoosting ont un meilleur F1 (0.45-0.46)

**Résultats en production**  
Meilleur modèle actuel : GradientBoosting version 87, F1-score = 0.4569, au-dessus du seuil de 0.40.

**Alternatives considérées**
- Accuracy : trompeuse sur dataset déséquilibré, favorise les modèles qui prédisent "à l'heure" trop souvent
- Recall seul : pousserait le modèle à prédire "retard" systématiquement pour ne jamais en rater, au détriment de la precision


## ADR-003 — Retrait de la feature `status` du pipeline ML
**Statut** : Accepté

**Contexte**  
La feature `status` était initialement incluse dans les features d'entraînement. Elle contenait les valeurs finales des vols passés (`ON_TIME`, `ARRIVED`, `CANCELLED`) dans `analytics.raw_flights`. En production, `analytics.scheduled_flights` peut contenir des valeurs transitoires supplémentaires (`DELAYED_DEPARTURE`) que le modèle n'avait jamais vues à l'entraînement.

**Décision**  
Retirer `status` des features d'entraînement (`ML_training_raw_flights.py`) et d'inférence (`ML_predict_scheduled_flights.py`).

**Implémentation**  
Suppression de `"status"` dans les deux listes de features :
```python
features = [
    "origin_airport", "destination_airport", "departure_time_block",
    "day_of_week", "month", "dep_hour", "arr_hour", "is_cancelled",
    "precipitation_sum", "wind_speed_max", "wind_gusts_max",
    "weather_code", "temp_min",
]
```

**Justification**
- **Data leakage** : `status = ARRIVED` dans les données d'entraînement est fortement corrélé à `is_delayed` — le modèle apprenait une relation triviale et circulaire, pas un vrai pattern prédictif. Cela expliquait le F1 artificiellement élevé de 0.8555 observé sur l'ancienne version en Production (version 68)
- **Incohérence entraînement/inférence** : `DELAYED_DEPARTURE` apparaît uniquement dans les vols du jour et était ignoré silencieusement par `OneHotEncoder(handle_unknown="ignore")` — la feature n'apportait donc aucune information utile à l'inférence
- Après retrait, le F1 réel est de 0.45 — score honnête, sans biais

**Validation**  
La version 68 (avec `status`) a été archivée manuellement. La version 87 (sans `status`) est en Production avec F1=0.4569.

**Alternatives considérées**
- Garder `status` avec un mapping explicite des valeurs transitoires : complexité inutile, le problème de fond (statuts non disponibles à l'entraînement) reste entier


## ADR-004 — Pattern DELETE+INSERT pour le chargement en couche raw
**Statut** : Accepté

**Contexte**  
Les DAGs d'extraction tournent toutes les 2h. À chaque cycle, les mêmes vols peuvent être collectés avec des statuts mis à jour (un vol passe de `ON_TIME` à `DELAYED`). Il faut décider comment gérer ces mises à jour en base sans créer de doublons.

**Décision**  
Utiliser un pattern DELETE+INSERT dans `Parquet_to_snapshot2` : supprimer les données existantes pour la même `date_photo` et le même aéroport avant de réinsérer le nouveau batch.

**Implémentation**  
Dans `Parquet_to_snapshot2.py` :
```python
# Suppression des données du cycle courant
cursor.execute("""
    DELETE FROM {schema}.{table}
    WHERE date_photo = %s AND origin_airport = %s
""", (date_photo, airport))

# Réinsertion du batch complet
df.to_sql(table, engine, schema=schema, if_exists="append", index=False)
```

**Justification**
- **Idempotence** : si un DAG est relancé après échec, le résultat est identique — pas de doublons
- **Simplicité** : pas de logique UPSERT complexe avec gestion des conflits colonne par colonne
- **Cohérence** : le batch inséré reflète toujours l'état le plus récent de l'API pour ce cycle

**Limites identifiées**
- Absence de versioning intra-journalier : deux cycles du même jour ont la même `date_photo` (format YYYY-MM-DD) — la déduplication inter-cycles est gérée par `VersionSelector` en analytics
- Pas de point-in-time correctness : l'historique des états intermédiaires d'un vol au fil de la journée n'est pas conservé — perspective d'amélioration via snapshot historique (SCD Type 2)

**Alternatives considérées**
- UPSERT (`ON CONFLICT DO UPDATE`) : plus précis mais nécessite des contraintes de clé primaire déclarées en base, absentes dans les tables raw
- Append pur : crée des doublons à chaque cycle, nécessite une déduplication plus complexe en aval

