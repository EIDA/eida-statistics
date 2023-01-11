FROM python:3.9-slim
RUN apt-get update && apt-get upgrade -y && apt-get install  -y --no-install-recommends libpq-dev gcc libc6-dev
RUN pip install --no-cache-dir gunicorn
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY webservice/ .
CMD ["/bin/bash", "-c", "gunicorn --bind 0.0.0.0:5000 manage:app"]

