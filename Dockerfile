ARG NA_PYVERSION="${NA_PYVERSION:-3.9}"
FROM python:${NA_PYVERSION}-slim

ARG NA_USER="${NA_USER:-guido}"
ARG TOX_DIR="${TOX_DIR:-/tox}"
RUN mkdir "${TOX_DIR}" /pip /src \
    && useradd -M --home-dir /pip "${NA_USER}" \
    && chown "${NA_USER}:${NA_USER}" "${TOX_DIR}" /pip
WORKDIR "${TOX_DIR}"

USER "${NA_USER}"
ENV PATH="${PATH}:/pip/.local/bin"

RUN pip install -qq --upgrade pip
COPY . /src

RUN pip install --user -r /src/requirements.txt \
    && pip install --user -r /src/requirements-dev.txt

ENTRYPOINT ["tox", "-c", "/src/tox.ini", "--root", "${TOX_DIR}", "--workdir", "${TOX_DIR}"]
