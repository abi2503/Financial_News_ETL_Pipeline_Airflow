FROM quay.io/astronomer/astro-runtime:12.7.1
RUN python -m nltk.downloader punkt punkt_tab
