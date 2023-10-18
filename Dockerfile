FROM python:3.8
WORKDIR /app
COPY ETL_Pipeline.py /app/
RUN pip install pandas sqlalchemy psycopg2
ENTRYPOINT ["python","ETL_Pipeline.py"]