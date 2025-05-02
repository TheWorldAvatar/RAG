FROM python:3.13

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./*.py /code/
COPY ./*.yaml /code/
RUN mkdir /code/data
COPY ./data/*.json /code/data/
RUN mkdir /code/static
COPY ./static/ /code/static/
RUN mkdir /code/html_templates
COPY ./html_templates/ /code/html_templates/
RUN mkdir /code/prompt_templates
COPY ./prompt_templates/ /code/prompt_templates/

RUN mkdir /code/.embeddings
RUN mkdir /code/.vectorstore

EXPOSE 80
CMD ["fastapi", "run", "app.py", "--port", "80"]

