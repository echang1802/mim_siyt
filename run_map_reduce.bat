pip install nltk
python -m nltk.downloader punkt stopwords
python mapper.py test/MLA57250.parquet | python reducer.py
