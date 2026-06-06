# Blue/Green Deployment

## Concept

Le déploiement Blue/Green consiste à maintenir deux environnements identiques :
- **Blue** : version actuelle en production
- **Green** : nouvelle version déployée en parallèle

Le trafic bascule de Blue vers Green uniquement quand Green est validé.
En cas de problème, le rollback est instantané.

## Pourquoi ce pattern ?

- **Zéro interruption** lors des déploiements
- **Rollback instantané** en cas de problème
- **Tests en production** avant bascule du trafic

## Implémentation dans notre projet

Dans notre architecture actuelle, ce pattern est assuré par :

1. **Health checks** dans le CD pipeline — le déploiement valide que l'API
   répond sur `/health` avant de continuer
2. **restart: always** dans Docker Compose — les services redémarrent
   automatiquement en cas de crash
3. **Webhook Alertmanager** — redémarre automatiquement les services tombés

## Implémentation complète (architecture cible)

Pour une implémentation complète sur cette infrastructure :

1. Deux services dans `docker-compose.yaml` : `api_blue` et `api_green`
2. **Nginx** comme reverse proxy pour router le trafic
3. Script de bascule Blue → Green après validation
4. Rollback automatique si le health check échoue
```yaml
# Exemple d'architecture cible
services:
  api_blue:
    build: ./api
    container_name: flight_api_blue

  api_green:
    build: ./api
    container_name: flight_api_green

  nginx:
    image: nginx
    # Route vers blue ou green selon la config
```
