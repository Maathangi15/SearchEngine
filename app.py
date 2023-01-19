from flask import Flask, render_template, request
from query import search
from elasticsearch_dsl import Index

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


@app.route('/search_all', methods=['GET','POST'])
def search_all():
    if request.method == 'POST':
        query = request.form['searchTerm']
        res = search([0,query])
        hits = res['hits']['hits']
        time = res['took']
        num_results = res['hits']['total']['value']

        return render_template('search_results.html', query=query, hits=hits, num_results=num_results, time=time)

    if request.method == 'GET':
        return render_template('index.html', init='True')


@app.route('/search_target', methods=['GET','POST'])
def search_target():
    if request.method == 'POST':
        target = request.form['target']
        res = search([1,target])
        hits = res['hits']['hits']
        time = res['took']
        num_results = res['hits']['total']['value']

        return render_template('search_results.html', query=target, hits=hits, num_results=num_results, time=time)

    if request.method == 'GET':
        return render_template('index.html', init='True')

@app.route('/search_term', methods=['GET','POST'])
def search_term():
    if request.method == 'POST':
        term = request.form['term']
        res = search([2,term])
        hits = res['hits']['hits']
        time = res['took']
        num_results = res['hits']['total']['value']

        return render_template('search_results.html', query=term, hits=hits, num_results=num_results, time=time)

    if request.method == 'GET':
        return render_template('index.html', init='True')

@app.route('/search_year_range', methods=['GET','POST'])
def search_year_range():
    if request.method == 'POST':
        year_range = request.form['year_range']
        res = search([3,year_range])
        hits = res['hits']['hits']
        time = res['took']
        num_results = res['hits']['total']['value']

        return render_template('search_results.html', query=year_range, hits=hits, num_results=num_results, time=time)

    if request.method == 'GET':
        return render_template('index.html', init='True')

@app.route('/search_movie_wildcard', methods=['GET','POST'])
def search_movie_wildcard():
    if request.method == 'POST':
        movie_wildcard = request.form['movie_wildcard']
        res = search([4,movie_wildcard])
        hits = res['hits']['hits']
        time = res['took']
        num_results = res['hits']['total']['value']

        return render_template('search_results.html', query=movie_wildcard, hits=hits, num_results=num_results, time=time)

    if request.method == 'GET':
        return render_template('index.html', init='True')

if __name__ == '__main__':
    app.run()
