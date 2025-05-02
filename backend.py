from flask import Flask, jsonify, request, send_from_directory, redirect
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import os
from auxiliares import open_json


def role_required(*roles):
    def decorator(f):
        @wraps(f)
        @login_required
        def wrapped(*args, **kwargs):
            if current_user.role not in roles:
                return jsonify({'msg':'Acesso negado'}), 403
            return f(*args, **kwargs)
        return wrapped
    return decorator

# Parametros principais
main_parameters = open_json()

# Configurações iniciais do Flask
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'react-build')
app = Flask(__name__, static_folder=template_dir, static_url_path='')
CORS(app, supports_credentials=True)  # para cookies via React
app.config['SECRET_KEY'] = main_parameters['backend']['secret_key']
app.config['SQLALCHEMY_DATABASE_URI'] = main_parameters['backend']['sqlite_path']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

login_manager = LoginManager(app)
login_manager.login_view = '/api/login'

# Models correspondendo às tabelas SQLite

# Model User
class User(db.Model, UserMixin):
    __tablename__ = 'users'
    user_id       = db.Column(db.Integer, primary_key=True)
    username      = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    role          = db.Column(db.String(20), nullable=False)  # 'root','moderator','normal'

    def set_password(self, pw):
        self.password_hash = generate_password_hash(pw)

    def check_password(self, pw):
        return check_password_hash(self.password_hash, pw)

    # UserMixin já implementa get_id()
    @property
    def id(self):
        return self.user_id

class Parameter(db.Model):
    __tablename__ = 'parameters'
    parameter_id    = db.Column(db.Integer, primary_key=True)
    parameter_name  = db.Column(db.Text, nullable=False)
    sql_script      = db.Column(db.Text)
    jobs            = db.relationship('JobHE', backref='parameter', lazy=True)

class JobHE(db.Model):
    __tablename__ = 'jobs_he'
    job_id           = db.Column(db.Integer, primary_key=True)
    job_name         = db.Column(db.Text, nullable=False)
    job_status       = db.Column(db.Integer, nullable=False)
    export_type      = db.Column(db.Text, nullable=False)
    export_path      = db.Column(db.Text, nullable=False)
    export_name      = db.Column(db.Text, nullable=False)
    days_offset       = db.Column(db.Integer)
    check_parameter = db.Column(db.Text)
    parameter_id     = db.Column(db.Integer, db.ForeignKey('parameters.parameter_id'))
    data_primary_key = db.Column(db.Text)
    sql_script       = db.Column(db.Text)
    schedule         = db.relationship('JobDE', uselist=False, backref='job')

class JobDE(db.Model):
    __tablename__ = 'jobs_de'
    schedule_id = db.Column(db.Integer, primary_key=True)
    job_id      = db.Column(db.Integer, db.ForeignKey('jobs_he.job_id'))
    job_minute  = db.Column(db.Text,  nullable=False)
    job_hour    = db.Column(db.Text,  nullable=False)
    job_day     = db.Column(db.Text,  nullable=False)


@app.route('/api/users', methods=['GET'])
@role_required('root')
def list_users():
    us = User.query.all()
    return jsonify([{'user_id':u.user_id,'username':u.username,'role':u.role} for u in us])

@app.route('/api/users', methods=['POST'])
@role_required('root')
def create_user():
    data = request.json
    if User.query.filter_by(username=data['username']).first():
        return jsonify({'msg':'Usuário já existe'}), 400
    u = User(username=data['username'], role=data['role'])
    u.set_password(data['password'])
    db.session.add(u)
    db.session.commit()
    return jsonify({'user_id':u.user_id}), 201

@app.route('/api/users/<int:user_id>', methods=['PUT'])
@role_required('root')
def update_user(user_id):
    data = request.json
    u = User.query.get_or_404(user_id)
    u.role = data.get('role', u.role)
    if data.get('password'):
        u.set_password(data['password'])
    db.session.commit()
    return jsonify({'msg':'updated'}), 200

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@role_required('root')
def delete_user(user_id):
    u = User.query.get_or_404(user_id)
    db.session.delete(u)
    db.session.commit()
    return jsonify({'msg':'deleted'}), 200

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.json or {}
    u = User.query.filter_by(username=data.get('username')).first()
    if not u or not u.check_password(data.get('password','')):
        return jsonify({'msg':'Usuário ou senha inválidos'}), 401
    login_user(u)
    return jsonify({'username': u.username, 'role': u.role}), 200

@app.route('/api/logout', methods=['POST'])
@login_required
def api_logout():
    logout_user()
    return jsonify({'msg':'deslogado'}), 200

@app.route('/api/me', methods=['GET'])
def api_me():
    if not current_user.is_authenticated:
        return jsonify({'authenticated': False}), 200
    return jsonify({
        'authenticated': True,
        'username': current_user.username,
        'role': current_user.role
    }), 200

# Endpoints da API
@app.route('/api/parameters', methods=['GET'])
def list_parameters():
    params = Parameter.query.all()
    return jsonify([
        {'parameter_id': p.parameter_id, 'parameter_name': p.parameter_name}
        for p in params
    ])

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    jobs = JobHE.query.all()
    result = []
    for j in jobs:
        # puxa todos os schedules
        scheds = JobDE.query.filter_by(job_id=j.job_id).all()
        minutes = [s.job_minute for s in scheds]
        hours   = [s.job_hour   for s in scheds]
        days    = set([s.job_day     for s in scheds])
        complete_hour = [f"{h}:{m}" for h in hours for m in minutes]
        result.append({
            'job_id': j.job_id,
            'job_name': j.job_name,
            'job_status': j.job_status,
            'export_type': j.export_type,
            'export_path': j.export_path,
            'export_name': j.export_name,
            'days_offset': j.days_offset,
            'check_parameter': j.check_parameter,
            'parameter_id': j.parameter_id,
            'data_primary_key': j.data_primary_key,
            'sql_script': j.sql_script,
            'schedule': {
                # dias podem repetir se o mesmo dia tiver múltiplos minutos/horas?
                'day': ','.join(days),
                # minutos e horas trazemos únicos (mantendo ordem de aparição)
                'minute': ','.join(dict.fromkeys(minutes)),
                'hour':   ','.join(dict.fromkeys(hours)),
                # campo com hora completa
                'complete_hour': ','.join(dict.fromkeys(complete_hour))
            }
        })
    return jsonify(result)

@app.route('/api/jobs', methods=['POST'])
def create_job():
    data = request.json
    # Cria JobHE
    new_job = JobHE(
        job_name=data['job_name'],
        job_status=data['job_status'],
        export_type=data['export_type'],
        export_path=data['export_path'],
        export_name=data['export_name'],
        days_offset=data['days_offset'],
        check_parameter=data.get('check_parameter'),
        parameter_id=data.get('parameter_id'),
        data_primary_key=data.get('data_primary_key'),
        sql_script=data.get('sql_script')
    )
    db.session.add(new_job)
    db.session.flush()  # obter job_id antes de commit para FK
    # Cria JobDE (produto cartesiano) se há schedule
    sched = data.get('schedule', {})
    minutes =  [x for x in sched.get('minute', '').split(',') if x]
    hours   =  [x for x in sched.get('hour',   '').split(',') if x]
    days    =  [x for x in set(sched.get('day',    '').split(',')) if x] #Adicionado set() para remover suplicados
    for d in days:
        for h in hours:
            for m in minutes:
                db.session.add(JobDE(
                    job_id=new_job.job_id,
                    job_minute=m,
                    job_hour=h,
                    job_day=d
                ))

    db.session.commit()
    return jsonify({'job_id': new_job.job_id}), 201

@app.route('/api/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):
    j = JobHE.query.get_or_404(job_id)
    scheds = JobDE.query.filter_by(job_id=job_id).all()
    return jsonify({
        'job_id': j.job_id,
        'job_name': j.job_name,
        'job_status': j.job_status,
        'export_type': j.export_type,
        'export_path': j.export_path,
        'export_name': j.export_name,
        'check_parameter': j.check_parameter,
        'days_offset': j.days_offset,
        'parameter_id': j.parameter_id,
        'data_primary_key': j.data_primary_key,
        'sql_script': j.sql_script,
        'schedule': {
            'minute': ','.join([s.job_minute for s in scheds]),
            'hour': ','.join([s.job_hour   for s in scheds]),
            'day': ','.join(set([s.job_day     for s in scheds])),
        }
    })

@app.route('/api/jobs/<int:job_id>', methods=['PUT'])
def update_job(job_id):
    data = request.json
    j = JobHE.query.get_or_404(job_id)
    # atualiza campos
    j.job_name = data['job_name']
    j.job_status=data['job_status']
    j.export_type = data['export_type']
    j.export_path = data['export_path']
    j.export_name = data['export_name']
    j.check_parameter = data['check_parameter']
    j.days_offset=data['days_offset']
    j.parameter_id = data.get('parameter_id')
    j.data_primary_key = data.get('data_primary_key')
    j.sql_script = data.get('sql_script')
    # remove agendas antigas
    JobDE.query.filter_by(job_id=job_id).delete()
    # recria todas as combinações de day × hour × minute
    sched = data.get('schedule', {})
    minutes =  [x for x in sched.get('minute', '').split(',') if x]
    hours   =  [x for x in sched.get('hour',   '').split(',') if x]
    days    =  [x for x in set(sched.get('day',    '').split(',')) if x] #Adicionado set() para remover suplicados
    for d in days:
        for h in hours:
            for m in minutes:
                db.session.add(JobDE(
                    job_id=job_id,
                    job_minute=m,
                    job_hour=h,
                    job_day=d
                ))
    db.session.commit()
    return jsonify({'msg':'updated'})

@app.route('/api/jobs/<int:job_id>', methods=['DELETE'])
#@role_required(['root', 'moderator'])
def delete_job(job_id):
    JobHE.query.filter_by(job_id=job_id).delete()
    JobDE.query.filter_by(job_id=job_id).delete()
    db.session.commit()
    return jsonify({'msg':'deleted'})

# Servindo o front-end React+Vite
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path):
    # se existir arquivo estático, serve ele
    file_path = os.path.join(app.static_folder, path)
    if path and os.path.exists(file_path):
        return send_from_directory(app.static_folder, path)
    # senão devolve o index.html para o SPA
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    # Cria as tabelas no banco, se não existirem
    #db.create_all()
    app.run(debug=True, port=5000)
