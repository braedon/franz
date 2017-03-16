FROM python:3.6-slim

WORKDIR /usr/src/app

COPY setup.py /usr/src/app/
RUN pip install .

COPY franz/*.py /usr/src/app/franz/
RUN pip install -e .

COPY LICENSE /usr/src/app/
COPY README.md /usr/src/app/

CMD ["bash"]
