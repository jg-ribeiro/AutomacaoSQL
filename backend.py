from flask import Flask, jsonify, request, send_from_directory, redirect
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import os
from auxiliares import open_json

import time # For request duration logging

# --- Import Logging ---
from logging_config import get_logger, log_info, log_warning, log_error, log_exception, log_debug
logger = get_logger('backend')
# --- End Logging Import ---

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
app = Flask(__name__)
CORS(app, supports_credentials=True)  # para cookies via React
app.config['REACT_BUILD'] = template_dir
app.config['SECRET_KEY'] = main_parameters['backend']['secret_key']
app.config['SQLALCHEMY_DATABASE_URI'] = main_parameters['backend']['sqlite_path']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# --- Configurações Cruciais para Cookies de Sessão em HTTP ---
app.config['SESSION_COOKIE_SECURE'] = False  # <-- ESSENCIAL: Diz para NÃO exigir HTTPS
app.config['SESSION_COOKIE_HTTPONLY'] = True # <-- Boa prática: Previne acesso via JS

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


# --- Request Logging ---
@app.before_request
def log_request_info():
    # Store start time in request context
    request.start_time = time.time()
    user = current_user.username if current_user.is_authenticated else "anonymous"
    log_debug(logger, f"Request START: {request.method} {request.path} from {request.remote_addr}", user=user)

@app.after_request
def log_response_info(response):
    duration_ms = None
    if hasattr(request, 'start_time'):
        duration_ms = int((time.time() - request.start_time) * 1000)

    user = current_user.username if current_user.is_authenticated else "anonymous"
    log_level = log_info if response.status_code < 400 else log_warning if response.status_code < 500 else log_error

    log_level(
        logger,
        f"Request END: {request.method} {request.path} - Status {response.status_code}",
        user=user,
        duration_ms=duration_ms
    )
    return response

@app.errorhandler(Exception)
def handle_exception(e):
    """Log unhandled exceptions."""
    user = current_user.username if current_user.is_authenticated else "anonymous"
    log_exception(logger, f"Unhandled exception during request: {request.method} {request.path}", user=user)
    # Return a generic error response
    return jsonify(error="Internal Server Error", message=str(e)), 500


@app.route('/api/users', methods=['GET'])
@role_required('root')
def list_users():
    us = User.query.all()
    return jsonify([{'user_id':u.user_id,'username':u.username,'role':u.role} for u in us])

@app.route('/api/users', methods=['POST'])
@role_required('root')
def create_user():
    data = request.json
    username = data.get('username')
    role = data.get('role')
    actor = current_user.username # User performing the action

    # Verificação simples
    if not username or not role or not data.get('password'):
         log_warning(logger, f"User creation attempt failed: Missing data.", user=actor)
         return jsonify({'msg':'Missing username, password, or role'}), 400

    if User.query.filter_by(username=data['username']).first():
        log_warning(logger, f"User creation attempt failed: Username '{username}' already exists.", user=actor)
        return jsonify({'msg':'Usuário já existe'}), 400
    
    try:
        u = User(username=data['username'], role=data['role'])
        u.set_password(data['password'])
        db.session.add(u)
        db.session.commit()
        log_info(logger, f"User '{username}' (Role: {role}) created successfully by '{actor}'.", user=actor)
        return jsonify({'user_id':u.user_id}), 201
    except Exception as e:
        db.session.rollback()
        log_exception(logger, f"Error creating user '{username}' by '{actor}': {e}", user=actor)
        return jsonify({'msg': 'Error creating user'}), 500

@app.route('/api/users/<int:user_id>', methods=['PUT'])
@role_required('root')
def update_user(user_id):
    data = request.json
    actor = current_user.username
    u = User.query.get_or_404(user_id)
    u.role = data.get('role', u.role)
    target_username = u.username # Get username before potential changes

    try:
        changes = []
        new_role = data.get('role')
        new_password = data.get('password')

        if new_role and u.role != new_role:
            changes.append(f"role from '{u.role}' to '{new_role}'")
            u.role = new_role
        if new_password:
            changes.append("password updated")
            u.set_password(new_password)
        if not changes:
            log_info(logger, f"User update attempt for '{target_username}' (ID: {user_id}) by '{actor}': No changes provided.", user=actor)
            return jsonify({'msg':'no changes'}), 200 # Or 304 Not Modified

        db.session.commit()
        log_info(logger, f"User '{target_username}' (ID: {user_id}) updated by '{actor}': {'; '.join(changes)}.", user=actor)
        return jsonify({'msg':'updated'}), 200
    except Exception as e:
         db.session.rollback()
         log_exception(logger, f"Error updating user '{target_username}' (ID: {user_id}) by '{actor}': {e}", user=actor)
         return jsonify({'msg': 'Error updating user'}), 500

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@role_required('root')
def delete_user(user_id):
    actor = current_user.username
    u = User.query.get_or_404(user_id)
    target_username = u.username

    if u.username == actor: # Prevent self-deletion? Or handle differently.
        log_warning(logger, f"User '{actor}' attempted to delete themselves (ID: {user_id}). Action denied.", user=actor)
        return jsonify({'msg':'Cannot delete yourself'}), 403

    try:
        db.session.delete(u)
        db.session.commit()
        log_info(logger, f"User '{target_username}' (ID: {user_id}) deleted by '{actor}'.", user=actor)
        return jsonify({'msg':'deleted'}), 200
    except Exception as e:
         db.session.rollback()
         log_exception(logger, f"Error deleting user '{target_username}' (ID: {user_id}) by '{actor}': {e}", user=actor)
         return jsonify({'msg': 'Error deleting user'}), 500

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Autenticação
@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.json or {}
    username = data.get('username')
    password = data.get('password','')

    if not username or not password:
         # Log attempt with missing credentials, but be cautious about logging passwords
         log_warning(logger, f"Login attempt failed: Missing username or password. User: '{username}'", user=username)
         return jsonify({'msg':'Usuário ou senha inválidos'}), 401 # Generic message

    u = User.query.filter_by(username=data.get('username')).first()
    if not u or not u.check_password(password):
        log_warning(logger, f"Login attempt failed: Invalid credentials for user '{username}'.", user=username)
        return jsonify({'msg':'Usuário ou senha inválidos'}), 401
    

    login_user(u)
    log_info(logger, f"User '{username}' logged in successfully.", user=username)
    return jsonify({'username': u.username, 'role': u.role}), 200

@app.route('/api/logout', methods=['POST'])
@login_required
def api_logout():
    actor = current_user.username
    logout_user()
    log_info(logger, f"User '{actor}' logged out.", user=actor)
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
@login_required
def create_job():
    data = request.json
    actor = current_user.username
    job_name = data.get('job_name', 'Unnamed Job')
    log_info(logger, f"Attempting to create job '{job_name}' by user '{actor}'.", user=actor)

    try:
        # Basic validation
        required_fields = ['job_name', 'export_path', 'export_name', 'sql_script']
        if not all(field in data for field in required_fields):
             log_warning(logger, f"Job creation failed for '{job_name}' by '{actor}': Missing required fields.", user=actor)
             return jsonify({"msg": "Missing required fields"}), 400
        
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
        job_id = new_job.job_id # Get the ID

        # Cria JobDE (produto cartesiano) se há schedule
        sched = data.get('schedule', {})
        minutes =  [x for x in sched.get('minute', '').split(',') if x]
        hours   =  [x for x in sched.get('hour',   '').split(',') if x]
        days    =  [x for x in set(sched.get('day',    '').split(',')) if x] #Adicionado set() para remover suplicados

        schedule_details = f"Days: {','.join(days)}, Hours: {','.join(hours)}, Minutes: {','.join(minutes)}"
        log_debug(logger, f"Processing schedule for new job {job_id}: {schedule_details}", job_id=job_id, user=actor)

        schedule_count = 0
        for d in days:
            for h in hours:
                for m in minutes:
                    db.session.add(JobDE(
                        job_id=job_id,
                        job_minute=m,
                        job_hour=h,
                        job_day=d
                    ))
                    schedule_count += 1

        db.session.commit()
        log_info(logger, f"Job '{job_name}' (ID: {job_id}) created successfully by '{actor}'. {schedule_count} schedule entries added.", job_id=job_id, user=actor)
        return jsonify({'job_id': new_job.job_id}), 201
    except Exception as e:
        db.session.rollback()
        log_exception(logger, f"Error creating job '{job_name}' by '{actor}': {e}", user=actor)
        return jsonify({'msg': 'Error creating job'}), 500

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
@login_required
def update_job(job_id):
    data = request.json
    actor = current_user.username
    log_info(logger, f"Attempting to update job ID {job_id} by user '{actor}'.", job_id=job_id, user=actor)


    j = JobHE.query.get_or_404(job_id)
    original_name = j.job_name # For logging

    try:

        # Track changes for logging
        changes = []
        for field in ['job_name', 'job_status', 'export_type', 'export_path', 'export_name', 'days_offset', 'check_parameter', 'parameter_id', 'data_primary_key', 'sql_script']:
            if field in data and getattr(j, field) != data[field]:
                 changes.append(f"{field} changed") # Could log old/new values if needed
                 setattr(j, field, data[field])
        
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

        
        if not changes:
            log_info(logger, f"Job update attempt for '{original_name}' (ID: {job_id}) by '{actor}': No changes detected.", job_id=job_id, user=actor)
            return jsonify({'msg':'no changes'}), 200

        db.session.commit()
        log_info(logger, f"Job '{j.job_name}' (ID: {job_id}) updated successfully by '{actor}'. Changes: {'; '.join(changes)}.", job_id=job_id, user=actor)

        return jsonify({'msg':'updated'})
    except Exception as e:
        db.session.rollback()
        log_exception(logger, f"Error updating job '{original_name}' (ID: {job_id}) by '{actor}': {e}", job_id=job_id, user=actor)
        return jsonify({'msg': 'Error updating job'}), 500

@app.route('/api/jobs/<int:job_id>', methods=['DELETE'])
@login_required
def delete_job(job_id):
    actor = current_user.username
    log_info(logger, f"Attempting to delete job ID {job_id} by user '{actor}'.", job_id=job_id, user=actor)
    
    j = JobHE.query.get_or_404(job_id)
    job_name = j.job_name # For logging

    try:
        # Deleting JobHE should cascade delete JobDE due to relationship/FK config
        JobHE.query.filter_by(job_id=job_id).delete()
        JobDE.query.filter_by(job_id=job_id).delete()

        db.session.commit()
        log_info(logger, f"Job '{job_name}' (ID: {job_id}) deleted successfully by '{actor}'.", job_id=job_id, user=actor)
        return jsonify({'msg':'deleted'})
    except Exception as e:
        db.session.rollback()
        log_exception(logger, f"Error deleting job '{job_name}' (ID: {job_id}) by '{actor}': {e}", job_id=job_id, user=actor)
        return jsonify({'msg': 'Error deleting job'}), 500

# Servindo o front-end React+Vite (agora usando o REACT_BUILD do config)
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path):
    build_dir = app.config['REACT_BUILD']
    file_path = os.path.join(build_dir, path)

    # se existir arquivo estático na pasta de build, serve direto
    if path and os.path.isfile(file_path):
        return send_from_directory(build_dir, path)

    # senão, cai no index.html para o React Router
    return send_from_directory(build_dir, 'index.html')

if __name__ == '__main__':
    # Cria as tabelas no banco, se não existirem
    #db.create_all()
    app.run(host='0.0.0.0', debug=True, port=5000)
