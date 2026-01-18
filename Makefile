# Makefile pour Docker

# Installe Docker (Ubuntu/Debian)
install:
	sudo apt update
	sudo apt install -y docker.io
	sudo systemctl enable --now docker
	sudo usermod -aG docker $$(whoami)
	@echo "Docker installé. Déconnecte-toi puis reconnecte-toi pour appliquer le groupe docker."

# Démarre Docker et liste les containers en cours d'exécution
restart:
	docker restart $$(docker ps -q)

# Arrête et relance Docker
create:
	docker compose down
	docker compose up -d

# Vérifie la version de Docker
version:
	docker --version

# Lance un container de test (Hello World)
run-test:
	docker run hello-world

# Supprime tous les containers et images inutilisés même ceux actifs
clean:
	docker rm -f $$(docker ps -a -q) || true
	docker rmi -f $$(docker images -q) || true

# Supprime uniquement les containers arrêtés et les images non utilisées
prune:
	docker container prune -f
	docker image prune -f

# Supprime tout : containers, images, volumes et réseaux inutilisés
prune-all:
	docker system prune -f
	docker volume prune -f

# Vérifie le stockage utilisé par Docker
docker-storage:
	docker system df -v