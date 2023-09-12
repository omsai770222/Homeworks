FROM python:latest

COPY input.csv /app/
COPY output.csv /app/
COPY preprocess.py /app/


WORKDIR /app

RUN pip install pandas Flask

RUN chmod +x /app/preprocess.py

CMD ["python", "preprocess.py", "input.csv", "output.csv"]



