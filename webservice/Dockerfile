FROM python:3.9
RUN pip install --no-cache-dir gunicorn
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY webservice/ .
CMD ["/bin/bash", "-c", "gunicorn --bind 0.0.0.0:5000 manage:app"]

