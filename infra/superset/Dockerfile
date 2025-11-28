
FROM apache/superset:5.0.0

USER root

COPY requirements.txt /app/requirements.txt
COPY github_security_manager.py /app/github_security_manager.py

RUN chown -R superset:superset /app

USER superset:superset

WORKDIR /app

# Install packages using uv into the virtual environment
RUN . /app/.venv/bin/activate && uv pip install -r /app/requirements.txt

CMD ["/app/docker/entrypoints/run-server.sh"]