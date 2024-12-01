from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Hi, My name is Anonimous, I'm from America 🌎"

if __name__ == '__main__':
    app.run(debug=True)
