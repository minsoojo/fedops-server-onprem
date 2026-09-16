ARG BASE_IMAGE=docker.io/library/python@sha256:da5aee29682d12a6649f51c8d6f15b87deb3e6c524b923c41d0cb3304d07c913
FROM ${BASE_IMAGE}
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /FedOps-Server/server_manager
COPY requirements.txt requirements.lock ./
RUN python -m pip install --no-cache-dir -r requirements.lock && python -m pip check && python -m pip freeze > /image-requirements.txt
COPY . .
EXPOSE 8000
CMD ["python3", "app.py"]
