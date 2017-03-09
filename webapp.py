from flask import Flask, render_template, request
from datastores.mongo.db import get_new

database = get_new()

db_name = "full_url"
app = Flask(__name__,
            static_folder='web/static',
            template_folder='web/templates')


@app.route('/', methods=['GET'])
def index():
    """
    Get the first 100 hits and show them.

    :return: Rendered template for index.html
    """
    parsed = database.mongodb[db_name].find({'parsed': True}).limit(100)

    return render_template('index.html', data=list(parsed))


@app.route('/get_rest', methods=['POST'])
def get_rest():
    """
    Get 'size' entries from offset 'start' as html table entries

    :return: 'size' entries from 'start' offset as html table rows
    """

    size = int(request.form.get('size'))
    start = int(request.form.get('start'))

    try:
        data = database.mongodb[db_name].find({'parsed': True}, {'_id': 0})\
            .limit(size)\
            .skip(start)
    except Exception:
        return "EOF"
    resp = ""

    for idx, entry in enumerate(data):
        try:
            resp += '<tr>'

            resp += '<td class="col-md-1">'+str(101+idx+start)+'</td>'

            resp += '<td class="col-md-2"><a href=' + entry.get('url') + '>' \
                    + entry.get('url') + '</td>'
            resp += '<td class="col-md-3">'
            resp += entry.get('title') \
                if entry.get('title') is not None else 'AA'
            resp += '</td>'

            resp += '<td class="col-md-3">'
            resp += entry.get('desc') \
                if entry.get('desc') is not None else 'AA'
            resp += '</td>'

            resp += '<td class="col-md-3">'
            resp += entry.get('keywords') \
                if entry.get('keywords') is not None else 'AA'
            resp += '</td>'

            resp += '</tr>'
        except Exception as e:
            print(e)
            print(entry)
            continue
    print("DONE")
    return resp


if __name__ == '__main__':
    app.run(debug=False)
