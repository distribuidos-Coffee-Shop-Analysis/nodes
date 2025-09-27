FROM python:3.9.7-slim

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

# Copy files maintaining directory structure  
COPY main.py /main.py
COPY common/ /common/
COPY server/ /server/
COPY protocol/ /protocol/
COPY tests/ /tests/
COPY config.ini /config.ini

# Set Python path
ENV PYTHONPATH=/

RUN python -m unittest tests/test_common.py

ENTRYPOINT ["/bin/sh"]
