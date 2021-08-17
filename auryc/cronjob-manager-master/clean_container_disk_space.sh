sudo docker rmi -f $(docker images -aq)
sudo docker system prune -a
