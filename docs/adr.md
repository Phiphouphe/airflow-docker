## ADR-008 — Service Webhook pour l'auto-healing

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
