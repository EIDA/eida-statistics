FROM python:3.9-slim
RUN apt-get update && apt-get upgrade -y && apt-get install  -y --no-install-recommends libpq-dev gcc libc6-dev
RUN pip install --no-cache-dir gunicorn
WORKDIR /app
COPY requirements.txt .
COPY development.ini .
COPY setup.py .
COPY webservice ./webservice
RUN pip install -r requirements.txt
RUN pip install -e .
RUN pip install -e ".[dev]"
CMD ["/bin/bash", "-c", "gunicorn --bind 0.0.0.0:6543 --paste development.ini"]
