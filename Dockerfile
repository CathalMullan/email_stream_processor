# Currently using Spark 3.0 (Debian based) - not Spark 2.4 (Alpine based)
FROM spark-py:spark

ARG PYTHON_VERSION=3.7.5
ARG PYENV_HOME=/root/.pyenv
ENV PYTHONUNBUFFERED=1

USER root

# Install Python & PyEnv
RUN apt-get update && apt-get -y install \
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
RUN pip install poetry
RUN pip install --upgrade pip setuptools

# Install Package
WORKDIR /
COPY . /

RUN poetry config virtualenvs.create false
RUN poetry install

# Install Spacy Model
RUN poetry run download_spacy_model
