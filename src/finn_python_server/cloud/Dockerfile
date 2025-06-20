FROM fnproject/python:3.9-dev as build-stage
WORKDIR /function
ADD requirements.txt /function/
RUN pip3 install --target /python/  --no-cache-dir -r requirements.txt
ADD . /function/
RUN rm -fr __pycache__
RUN rm -fr .pip_cache
RUN rm -f requirements.txt
RUN find . -name "__pycache__" | xargs rm -fr
RUN find . -name ".pip_cache" | xargs rm -fr

FROM fnproject/python:3.9
WORKDIR /function
COPY --from=build-stage /python /python
COPY --from=build-stage /function /function
ENV PYTHONPATH=/function:/python
ENTRYPOINT ["/python/bin/fdk", "/function/func.py", "handler"]