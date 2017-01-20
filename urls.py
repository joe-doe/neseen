import json
import os
from flask import Flask, render_template
from db import get_new

database = get_new()


# # open configuration file and read the values
# with open('config.json') as config_file:
#     config = json.load(config_file)
#
# # override values if custom config exists
# try:
#     with open('my_config.json') as ext_config_file:
#         ext_config = json.load(ext_config_file)
#         config.update(ext_config)
# except IOError:
#     print("No external configuration found. Using default")
#     pass
#
# # get sensible credentials from environment variables
# try:
#     config['uri'] = str(os.environ['mongodb_uri'])
#     config['DEBUG'] = str(os.environ['DEBUG'])
# except KeyError:
#     pass


app = Flask(__name__,
            static_folder='web/static',
            template_folder='web/templates')


@app.route("/")
def index():
    parsed = database.mongodb['url'].find({'parsed': True})

    return render_template("index.html", data=list(parsed))


if __name__ == '__main__':
    app.run(debug=False)
