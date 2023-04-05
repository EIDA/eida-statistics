FROM python:3.9-slim
RUN apt-get update && apt-get upgrade -y && apt-get install  -y --no-install-recommends libpq-dev gcc libc6-dev gpg
RUN pip install --no-cache-dir gunicorn
WORKDIR /app
COPY requirements.txt .
COPY development.ini .
COPY setup.py .
RUN pip install -r requirements.txt
COPY ws_eidastats ./ws_eidastats
COPY gnupghome ./gnupghome
RUN pip install -e .
# Patch openapi3 waiting for PR #201 https://github.com/Pylons/pyramid_openapi3/pull/201
RUN apt-get install -y --no-install-recommends wget
RUN wget -O /usr/local/lib/python3.9/site-packages/pyramid_openapi3/__init__.py https://raw.githubusercontent.com/vpet98/pyramid_openapi3/aca022ab7caaac989593919b0cd07c97fdb4d094/pyramid_openapi3/__init__.py
CMD ["/bin/bash", "-c", "gunicorn --bind 0.0.0.0:6543 --paste development.ini"]
