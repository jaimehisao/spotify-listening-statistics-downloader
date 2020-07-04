FROM python:3-alpine

USER 9000:9000

WORKDIR /usr/code/bot

COPY requirements.txt ./

RUN \
 sudo apk add --no-cache postgresql-libs && \
 sudo apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 sudo pip install spotipy && \
 sudo pip install python-dotenv && \
 sudo python3 -m pip install -r requirements.txt --no-cache-dir && \
 sudo apk --purge del .build-deps

COPY . .

CMD [ "python", "main.py" ]

