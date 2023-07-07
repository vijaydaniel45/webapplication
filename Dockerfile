FROM python:3.8
# show the stdout and stderr streams right in the command line instead of getting buffered.
ENV PYTHONUNBUFFERED 1
RUN mkdir /app
WORKDIR /app
COPY . .

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]