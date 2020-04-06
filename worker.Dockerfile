FROM spark-py:spark

ARG PYTHON_VERSION=3.7.5
ARG PYENV_HOME=/root/.pyenv
ARG POETRY_VERSION=1.0.2
ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1

USER root

# Install Python & PyEnv
RUN apt-get update
RUN apt-get -y install \
    make \
    git \
    build-essential \
    libssl-dev \
    zlib1g-dev  \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    curl \
    llvm \
    libncurses5-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libffi-dev \
    liblzma-dev

RUN git clone https://github.com/pyenv/pyenv.git ${PYENV_HOME}
ENV PATH ${PYENV_HOME}/shims:${PYENV_HOME}/bin:${PATH}
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}

RUN pip install --upgrade pip
RUN pyenv rehash

# Install Poetry
RUN pip install poetry==${POETRY_VERSION}

# Install Snappy
RUN apt-get -y install \
    libsnappy-dev

# Install Dependencies & Project
WORKDIR /app

COPY pyproject.toml poetry.lock /app/
RUN poetry config virtualenvs.create false
RUN poetry install

COPY . /app
RUN poetry install

# Install Spacy Model
RUN poetry run download_spacy_model
