FROM python:3.12-slim
WORKDIR /app
COPY app /app
ENV PYTHONUNBUFFERED=1 DATA_DIR=/data
EXPOSE 8080 3338
CMD ["python3","server.py"]
