FROM python:3.9-slim
RUN apt-get update && apt-get upgrade -y && apt-get install  -y --no-install-recommends libpq-dev gcc libc6-dev gpg git
RUN pip install --no-cache-dir gunicorn
WORKDIR /app
COPY requirements.txt .
COPY development.ini .
COPY setup.py .
RUN pip install -r requirements.txt
COPY ws_eidastats ./ws_eidastats
COPY gnupghome ./gnupghome
RUN pip install -e .
CMD ["/bin/bash", "-c", "gunicorn --bind 0.0.0.0:6543 --paste development.ini"]
