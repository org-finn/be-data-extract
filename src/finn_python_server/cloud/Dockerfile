FROM fnproject/python:3.9-dev as build-stage
WORKDIR /function

ADD requirements.txt /function/
RUN pip3 install --target /python/  --no-cache-dir -r requirements.txt
ADD . /function/

RUN python3 cleanup.py

# 더 이상 필요 없는 파일들을 정리합니다.
RUN rm -f requirements.txt
RUN rm -f cleanup.py

FROM fnproject/python:3.9
WORKDIR /function
COPY --from=build-stage /python /python
COPY --from=build-stage /function /function
ENV PYTHONPATH=/function:/python
ENTRYPOINT ["/python/bin/fdk", "/function/func.py", "handler"]